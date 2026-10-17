/**
 * The adapter-owned Docker Helper worker operation.
 *
 * One explicit owner of the whole worker operation lifecycle over the
 * Docker Helper HTTP API on the helper Unix socket:
 *
 *   POST /run                    — exactly once, never retried
 *   GET  /operations/:id/logs    — the single drain owner
 *   GET  /operations/:id         — the single status poll owner
 *   POST /operations/:id/cancel  — the single cancel owner
 *
 * The operation id lives only inside one `runWorkerOperation` call; it is
 * never returned, never persisted, never written to the run tree and never
 * part of any diagnostic. The worker outcome is decided exclusively by the
 * authoritative operation status; log retrieval failures are
 * outcome-neutral and only degrade the internal capture result.
 *
 * Crash-window honesty (schema v9 does not exist yet): a lost create
 * response is never retried (the daemon may have accepted the operation —
 * it can remain orphaned), the operation id is known only to this call,
 * and the captured bytes exist only in memory. Durable operation identity
 * and log publication belong to a later increment.
 */

import {
  helperHttpExchange,
  parseHelperJsonObject,
  type HelperHttpExchangeOptions,
  type HelperHttpResponse,
} from "./helper_http.ts";
import type { WorkerOperationCause, WorkerOperationHandle, WorkerSignalHub } from "./worker_signals.ts";

/** Fixed per-execution capture bound in UTF-8 bytes (8 MiB). */
export const WORKER_LOG_CAPTURE_BOUND_BYTES = 8 * 1024 * 1024;
/**
 * Fixed bound for a single helper HTTP response body in bytes.
 *
 * The honest pair with the capture bound: the `/logs` response carries the
 * payload JSON-encoded, and the worst-case JSON escaping is six bytes per
 * payload byte (every control character becomes `\u00XX`; quotes and
 * backslashes double). The bound is therefore the full worst-case encoding
 * of a complete capture payload plus envelope slack, so a legitimate
 * worst-case log response is never rejected as oversized while the memory
 * bound stays finite.
 */
export const WORKER_LOG_RESPONSE_BOUND_BYTES = 6 * WORKER_LOG_CAPTURE_BOUND_BYTES + 65_536;
/** Bounded delay between operation cycles. */
export const WORKER_OPERATION_POLL_DELAY_MS = 100;
/**
 * Hard bound for the create and status exchanges inside the operation
 * loop. The create request is never retried and is never wired to the
 * abort signal, so this constant is its exact finite wall-clock bound
 * (further bounded by the remaining worker deadline).
 */
export const OPERATION_REQUEST_TIMEOUT_MS = 10_000;
/**
 * Hard bound for one log-drain exchange, deliberately shorter than the
 * create/status bound. Log observation must not own the worker outcome: a
 * hanging `/logs` costs at most this bound once (the drain then stops
 * permanently) and can never eat the worker deadline, so a terminal
 * status is still reached before the overall timeout.
 */
export const OPERATION_DRAIN_TIMEOUT_MS = 2_000;
/** Hard bound for the one best-effort cancel request. */
const OPERATION_CANCEL_TIMEOUT_MS = 2_000;

/** The internal capture status. `complete` is deliberately not a value. */
export type WorkerLogCaptureStatus = "captured" | "truncated" | "unavailable";

export interface WorkerLogCapture {
  /**
   * `captured` — the terminal status was reached, the final drain chain
   * ended with `next_offset: null` or the daemon's caught-up empty page,
   * no log error happened and neither daemon nor local truncation was
   * observed. It never means "complete":
   * daemon-side eviction outside the observed window cannot be proven
   * absent.
   * `truncated` — bytes were received but the stream is not proven
   * complete (daemon head eviction, the local bound, an aborted or failed
   * drain, or an operation that ended before the stream did).
   * `unavailable` — no bytes can honestly be obtained or verified.
   */
  readonly status: WorkerLogCaptureStatus;
  /** Total log bytes received from the daemon, in UTF-8 bytes. */
  readonly byteCount: number;
  /**
   * The retained bounded tail (at most `WORKER_LOG_CAPTURE_BOUND_BYTES`
   * bytes; head eviction marks `truncatedLocal`). Empty for
   * `unavailable`. The view is read-only by contract.
   */
  readonly tailBytes: Uint8Array;
  /** The daemon reported `truncated: true` (head eviction daemon-side). */
  readonly truncatedDaemon: boolean;
  /** The local capture bound evicted head bytes. */
  readonly truncatedLocal: boolean;
}

export type WorkerOperationOutcome = "completed" | "worker_failed" | "worker_timeout";

export interface WorkerOperationRequest {
  readonly socketPath: string;
  /** The Execution Session bearer; the only credential of the operation. */
  readonly bearer: string;
  /** The serialized `POST /run` request body (the request-parity contract). */
  readonly body: string;
  /** The agent state's `timeout_seconds`; the operation's own deadline. */
  readonly timeoutSeconds: number;
  /** The runner-owned first-wins hub for signals and the deadline. */
  readonly workerSignals: WorkerSignalHub;
  /**
   * Receives every forwarded worker output chunk exactly once, in offset
   * order. The production sink writes to the orchestrator's stderr.
   */
  readonly outputSink: (chunk: Uint8Array) => void;
  /**
   * The HTTP transport. The production value is `helperHttpExchange`;
   * tests inject their own transport or run the production one against a
   * fixture daemon.
   */
  readonly transport?: (options: HelperHttpExchangeOptions) => Promise<HelperHttpResponse>;
}

export interface WorkerOperationResult {
  readonly outcome: WorkerOperationOutcome;
  readonly capture: WorkerLogCapture;
}

interface OperationStatus {
  readonly state: "pending" | "running" | "succeeded" | "failed" | "cancelled";
  readonly exitCode: number | null;
  readonly resultCode: string | null;
}

// Real daemon operation ids are `op_` + lowercase hex (underscore is part
// of the id grammar); the shape stays URL-safe.
const OPERATION_ID_PATTERN = /^[A-Za-z0-9_-]{1,128}$/;
const STATUS_VALUES = new Set(["pending", "running", "succeeded", "failed", "cancelled"]);
// Real daemon result codes are lowercase identifier strings (verified live
// against Docker Helper 2.3.1: "succeeded", "container_exit_nonzero", ...),
// not digit strings.
const RESULT_CODE_PATTERN = /^[a-z][a-z0-9_]*$/;
const SUCCESS_RESULT_CODE = "succeeded";

/**
 * Runs one worker operation to its terminal classification. The outcome
 * is decided only by the authoritative status; every log failure is
 * outcome-neutral.
 */
export async function runWorkerOperation(request: WorkerOperationRequest): Promise<WorkerOperationResult> {
  const transport = request.transport ?? helperHttpExchange;
  const { socketPath, bearer, body, timeoutSeconds, workerSignals, outputSink } = request;
  if (typeof socketPath !== "string" || socketPath === "" || !socketPath.startsWith("/")) {
    throw new Error("worker operation socket path must be an absolute path");
  }
  if (typeof bearer !== "string" || bearer === "") {
    throw new Error("worker operation bearer must be a non-empty string");
  }
  if (typeof body !== "string" || body === "") {
    throw new Error("worker operation request body must be a non-empty string");
  }
  if (!Number.isSafeInteger(timeoutSeconds) || timeoutSeconds <= 0) {
    throw new Error("worker operation timeout_seconds must be a positive integer");
  }
  if (typeof outputSink !== "function") {
    throw new Error("worker operation output sink must be a function");
  }

  // The bounded capture buffer: a FIFO of chunks with a byte budget; head
  // chunks are evicted when the budget is exceeded, so the retained bytes
  // are always the tail of the stream and the memory stays bounded.
  const chunks: Uint8Array[] = [];
  let retainedBytes = 0;
  let totalBytes = 0;
  let truncatedDaemon = false;
  let truncatedLocal = false;
  let drainFailed = false;
  let drainComplete = false;
  let drainStalled = false;
  let cursor = 0;

  const appendChunk = (chunk: Uint8Array): void => {
    totalBytes += chunk.byteLength;
    if (chunk.byteLength > WORKER_LOG_CAPTURE_BOUND_BYTES) {
      chunks.length = 0;
      chunks.push(chunk.slice(chunk.byteLength - WORKER_LOG_CAPTURE_BOUND_BYTES));
      retainedBytes = WORKER_LOG_CAPTURE_BOUND_BYTES;
      truncatedLocal = true;
      return;
    }
    chunks.push(chunk);
    retainedBytes += chunk.byteLength;
    while (retainedBytes > WORKER_LOG_CAPTURE_BOUND_BYTES && chunks.length > 0) {
      const head = chunks[0];
      if (head === undefined) {
        break;
      }
      chunks.shift();
      retainedBytes -= head.byteLength;
      truncatedLocal = true;
    }
  };

  const captureSnapshot = (status: WorkerLogCaptureStatus): WorkerLogCapture => {
    const merged = new Uint8Array(retainedBytes);
    let offset = 0;
    for (const chunk of chunks) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return Object.freeze({
      status,
      byteCount: totalBytes,
      tailBytes: merged,
      truncatedDaemon,
      truncatedLocal,
    });
  };

  const controller = new AbortController();
  let abortCause: { cause: WorkerOperationCause; signal: "SIGINT" | "SIGTERM" } | null = null;
  let cancelRequested = false;
  let notifyAbort: (() => void) | null = null;
  const aborted = new Promise<void>((resolve) => {
    notifyAbort = resolve;
  });

  // The abort cause is read through a function: the closure assigns the
  // variable asynchronously, so no control-flow narrowing of the outer
  // binding may hide an abort recorded between awaits.
  const readAbortCause = (): { cause: WorkerOperationCause; signal: "SIGINT" | "SIGTERM" } | null => abortCause;

  const handle: WorkerOperationHandle = {
    abort: (cause, signal) => {
      if (abortCause !== null) {
        return;
      }
      abortCause = { cause, signal };
      notifyAbort?.();
      controller.abort();
    },
  };

  workerSignals.register(handle);
  let operationId: string | null = null;
  let outcome: WorkerOperationOutcome = "worker_failed";
  const deadlineAt = Date.now() + timeoutSeconds * 1000;
  const deadlineTimer = setTimeout(() => {
    workerSignals.claimTimeout(handle);
  }, timeoutSeconds * 1000);

  const remainingMs = (): number => Math.max(1, deadlineAt - Date.now());
  /** The create-phase outcome: a deadline that claimed first wins. */
  const createOutcome = (): WorkerOperationOutcome =>
    readAbortCause()?.cause === "timeout" ? "worker_timeout" : "worker_failed";
  const requestTimeout = (): number => Math.min(OPERATION_REQUEST_TIMEOUT_MS, remainingMs());

  const sendCancel = async (): Promise<void> => {
    if (operationId === null || cancelRequested) {
      return;
    }
    cancelRequested = true;
    try {
      await transport({
        socketPath,
        bearer,
        method: "POST",
        path: `/operations/${operationId}/cancel`,
        timeoutMs: Math.min(OPERATION_CANCEL_TIMEOUT_MS, remainingMs()),
        maxResponseBytes: WORKER_LOG_RESPONSE_BOUND_BYTES,
      });
    } catch {
      // Best-effort: a cancel failure never overrides the already fixed
      // outcome and is never surfaced.
    }
  };

  try {
    // The create request is deliberately NOT wired to the abort signal:
    // aborting it mid-flight would lose the only chance to learn the
    // operation id and cancel the operation. Its own timeout still bounds
    // it; an abort during the create is handled right after the response.
    let createResponse: HelperHttpResponse;
    try {
      createResponse = await transport({
        socketPath,
        bearer,
        method: "POST",
        path: "/run",
        body,
        timeoutMs: requestTimeout(),
        maxResponseBytes: WORKER_LOG_RESPONSE_BOUND_BYTES,
      });
    } catch {
      // Lost create response: the daemon may have accepted the operation.
      // Never retried; the operation can remain orphaned (the honest C1
      // window of this increment).
      const cause = readAbortCause();
      return {
        outcome: cause?.cause === "timeout" ? "worker_timeout" : "worker_failed",
        capture: captureSnapshot("unavailable"),
      };
    }
    // The daemon answers POST /run with 201 Created (async operation
    // acceptance); 200 is tolerated for API parity.
    if (createResponse.status !== 201 && createResponse.status !== 200) {
      return { outcome: createOutcome(), capture: captureSnapshot("unavailable") };
    }
    const created = parseHelperJsonObject(createResponse.bodyText, "docker-helper run response");
    if (created["ok"] !== true) {
      return { outcome: createOutcome(), capture: captureSnapshot("unavailable") };
    }
    const operationIdValue = created["operation_id"];
    if (typeof operationIdValue !== "string" || !OPERATION_ID_PATTERN.test(operationIdValue)) {
      return { outcome: createOutcome(), capture: captureSnapshot("unavailable") };
    }
    operationId = operationIdValue;
    const earlyCause = readAbortCause();
    if (earlyCause !== null) {
      // The abort arrived while the create was in flight; the id is known
      // now, so exactly one cancel is sent before the outcome is returned.
      await sendCancel();
      return {
        outcome: earlyCause.cause === "timeout" ? "worker_timeout" : "worker_failed",
        capture: captureSnapshot("unavailable"),
      };
    }

    /**
     * One drain exchange. During the running phase an empty caught-up page
     * is normal (no new bytes yet); in the terminal chain it is the
     * daemon's end-of-stream signal together with `next_offset: null`.
     */
    const drainOnce = async (terminal: boolean): Promise<void> => {
      let response: HelperHttpResponse;
      try {
        response = await transport({
          socketPath,
          bearer,
          method: "GET",
          path: `/operations/${operationId}/logs?offset=${cursor}`,
          timeoutMs: Math.min(OPERATION_DRAIN_TIMEOUT_MS, remainingMs()),
          maxResponseBytes: WORKER_LOG_RESPONSE_BOUND_BYTES,
          signal: controller.signal,
        });
      } catch {
        // An abort unwinds through the loop's abortCause check; every
        // other failure stops the drain without touching the outcome.
        if (readAbortCause() === null) {
          drainFailed = true;
        }
        return;
      }
      if (response.status !== 200) {
        drainFailed = true;
        return;
      }
      let logsResponse: Record<string, unknown>;
      try {
        logsResponse = parseHelperJsonObject(response.bodyText, "docker-helper operation log response");
      } catch {
        drainFailed = true;
        return;
      }
      const logsValue = logsResponse["logs"];
      const offsetValue = logsResponse["offset"];
      const nextValue = logsResponse["next_offset"];
      const truncatedValue = logsResponse["truncated"];
      if (
        typeof logsValue !== "string" ||
        typeof offsetValue !== "number" ||
        !Number.isSafeInteger(offsetValue) ||
        offsetValue !== cursor
      ) {
        drainFailed = true;
        return;
      }
      if (nextValue !== null && (typeof nextValue !== "number" || !Number.isSafeInteger(nextValue) || nextValue < cursor)) {
        drainFailed = true;
        return;
      }
      if (truncatedValue !== undefined && typeof truncatedValue !== "boolean") {
        drainFailed = true;
        return;
      }
      if (truncatedValue === true) {
        truncatedDaemon = true;
      }
      if (logsValue.length > 0) {
        const chunk = new TextEncoder().encode(logsValue);
        appendChunk(chunk);
        outputSink(chunk);
      }
      if (nextValue === null) {
        drainComplete = true;
        return;
      }
      if (logsValue.length === 0 && nextValue > cursor) {
        // An empty page that advances the cursor delivers nothing and can
        // never make progress: the bytes received stay real, but the
        // stream is not proven complete — stop instead of spinning
        // forever.
        drainStalled = true;
        return;
      }
      if (nextValue === cursor && logsValue.length === 0) {
        if (terminal) {
          // The daemon reports the stream caught up with an empty page
          // instead of null: the bytes received are the whole stream as
          // far as the daemon exposes it.
          drainComplete = true;
          return;
        }
        // Running phase: no new bytes yet, nothing to advance.
        return;
      }
      if (nextValue === cursor) {
        // A non-empty page that does not advance the cursor cannot make
        // progress; stop instead of spinning forever. The bytes received
        // stay real, but the stream is not proven complete.
        drainStalled = true;
        return;
      }
      cursor = nextValue;
    };

    const pollStatus = async (): Promise<OperationStatus | null> => {
      let response: HelperHttpResponse;
      try {
        response = await transport({
          socketPath,
          bearer,
          method: "GET",
          path: `/operations/${operationId}`,
          timeoutMs: requestTimeout(),
          maxResponseBytes: WORKER_LOG_RESPONSE_BOUND_BYTES,
          signal: controller.signal,
        });
      } catch {
        // An abort unwinds through the loop's abortCause check; a status
        // failure is a transport failure classified by the caller.
        return null;
      }
      if (response.status !== 200) {
        return null;
      }
      let statusResponse: Record<string, unknown>;
      try {
        statusResponse = parseHelperJsonObject(response.bodyText, "docker-helper operation status response");
      } catch {
        return null;
      }
      const statusValue = statusResponse["status"];
      if (typeof statusValue !== "string" || !STATUS_VALUES.has(statusValue)) {
        return null;
      }
      const exitValue = statusResponse["exit_code"];
      if (
        exitValue !== undefined &&
        exitValue !== null &&
        (typeof exitValue !== "number" || !Number.isSafeInteger(exitValue))
      ) {
        return null;
      }
      const resultValue = statusResponse["result_code"];
      if (resultValue !== undefined && (typeof resultValue !== "string" || !RESULT_CODE_PATTERN.test(resultValue))) {
        return null;
      }
      return {
        state: statusValue as OperationStatus["state"],
        exitCode: exitValue === undefined ? null : (exitValue as number),
        resultCode: resultValue === undefined ? null : (resultValue as string),
      };
    };

    const classify = (status: OperationStatus): WorkerOperationOutcome => {
      if (status.state === "succeeded") {
        // Contradictory success can never become a success: a present
        // non-zero exit code, or a result code other than the daemon's
        // success code, fails closed.
        if (
          (status.exitCode !== null && status.exitCode !== 0) ||
          (status.resultCode !== null && status.resultCode !== SUCCESS_RESULT_CODE)
        ) {
          return "worker_failed";
        }
        return "completed";
      }
      return "worker_failed";
    };

    // The operation loop: drain then poll, bounded delay between cycles.
    cycle: while (true) {
      if (abortCause !== null) {
        break cycle;
      }
      if (!drainComplete && !drainFailed) {
        await drainOnce(false);
      }
      if (abortCause !== null) {
        break cycle;
      }
      const status = await pollStatus();
      if (status === null) {
        // The authoritative status is unreachable or malformed: a
        // transport/worker failure, never a log-capture failure.
        outcome = "worker_failed";
        break cycle;
      }
      if (status.state === "running" || status.state === "pending") {
        await Promise.race([aborted, Bun.sleep(WORKER_OPERATION_POLL_DELAY_MS)]);
        continue;
      }
      // Terminal status reached: finish the drain chain, then classify.
      while (!drainComplete && !drainFailed && !drainStalled) {
        await drainOnce(true);
      }
      outcome = classify(status);
      break cycle;
    }

    const cause = readAbortCause();
    if (cause !== null) {
      await sendCancel();
      return {
        outcome: cause.cause === "timeout" ? "worker_timeout" : "worker_failed",
        capture: captureSnapshot(totalBytes > 0 ? "truncated" : "unavailable"),
      };
    }
    const captureStatus: WorkerLogCaptureStatus =
      totalBytes === 0
        ? "unavailable"
        : drainComplete && !drainFailed && !drainStalled && !truncatedDaemon && !truncatedLocal
          ? "captured"
          : "truncated";
    return { outcome, capture: captureSnapshot(captureStatus) };
  } finally {
    clearTimeout(deadlineTimer);
    workerSignals.unregister(handle);
    controller.abort();
  }
}
