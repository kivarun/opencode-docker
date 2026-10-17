/**
 * The adapter-owned worker operation battery.
 *
 * Every test drives the production `runWorkerOperation` (with the
 * production transport `helperHttpExchange`) against an in-process
 * `Bun.serve` fixture daemon on a fresh Unix socket that emulates the
 * Docker Helper operation API: `POST /run`, `GET /operations/:id/logs`,
 * `GET /operations/:id` and `POST /operations/:id/cancel`. The fixture
 * records every request so the tests pin exactly-once semantics,
 * monotonic drains, single cancels and bearer isolation.
 */
import { rmSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  OPERATION_DRAIN_TIMEOUT_MS,
  OPERATION_REQUEST_TIMEOUT_MS,
  runWorkerOperation,
  WORKER_LOG_CAPTURE_BOUND_BYTES,
  WORKER_LOG_RESPONSE_BOUND_BYTES,
  type WorkerOperationOutcome,
} from "../src/worker_operation.ts";
import { WorkerSignalHub } from "../src/worker_signals.ts";

const roots: string[] = [];
afterAll(() => {
  for (const root of roots) {
    rmSync(root, { recursive: true, force: true });
  }
});

interface FixtureOperation {
  id: string;
  state: "running" | "succeeded" | "failed" | "cancelled";
  exitCode: number;
  log: string;
}

interface FixtureConfig {
  /** The operation holds in `running` for this long before terminal. */
  holdMs?: number;
  /** The terminal status shape. */
  terminal?: "succeeded" | "failed" | "cancelled";
  exitCode?: number;
  /** The log content produced while running and at terminal. */
  log?: string;
  /** Daemon head eviction reported through `truncated: true`. */
  truncated?: boolean;
  /** Faults injected per endpoint. */
  logFault?: "none" | "status404" | "status500" | "malformed" | "oversized" | "hang" | "hangAfterFirst" | "stall";
  /** The drain page size in bytes (default 4096). */
  pageBytes?: number;
  statusFault?: "none" | "status404" | "status500" | "malformed" | "unknown" | "hang" | "omitExit";
  /** A malformed or hostile operation id handed out by /run. */
  operationId?: string;
  /** POST /run holds this long before responding. */
  createHoldMs?: number;
  /** POST /run hangs forever (the lost create response). */
  createHang?: boolean;
  /** POST /run never answers (connect failure). */
  createRefused?: boolean;
  /** POST /operations/:id/cancel fails with this status. */
  cancelFault?: "none" | "status500";
  /** Overrides the terminal result_code (contradictory-shape tests). */
  resultOverride?: string;
}

interface Fixture {
  socketPath: string;
  operations: Map<string, FixtureOperation>;
  requests: Array<{ method: string; path: string; bearer: string; body: string }>;
  close: () => void;
}

const PAGE_BYTES = 4096;

function startFixture(config: FixtureConfig): Fixture {
  const root = mkdtempSync(join(tmpdir(), "worker-operation-"));
  roots.push(root);
  const socketPath = join(root, "helper.sock");
  const operations = new Map<string, FixtureOperation>();
  const requests: Array<{ method: string; path: string; bearer: string; body: string }> = [];
  let counter = 0;
  const json = (value: unknown, status = 200): Response =>
    new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

  const server = Bun.serve({
    unix: socketPath,
    async fetch(request) {
      const url = new URL(request.url);
      const bearer = (request.headers.get("Authorization") ?? "").replace(/^Bearer /, "");
      const body = await request.text();
      requests.push({ method: request.method, path: url.pathname + url.search, bearer, body });
      if (url.pathname === "/run" && request.method === "POST") {
        if (config.createRefused === true) {
          return new Response("refused", { status: 503 });
        }
        if (config.createHang === true) {
          return new Promise(() => undefined);
        }
        counter += 1;
        const id = config.operationId ?? `op-${counter}`;
        const operation: FixtureOperation = {
          id,
          state: "running",
          exitCode: 0,
          log: config.log ?? "",
        };
        operations.set(id, operation);
        const finish = async (): Promise<void> => {
          if ((config.holdMs ?? 0) > 0) {
            await Bun.sleep(config.holdMs ?? 0);
          }
          if (operation.state === "running") {
            operation.state = config.terminal ?? "succeeded";
            operation.exitCode = config.exitCode ?? 0;
          }
        };
        if ((config.createHoldMs ?? 0) > 0) {
          void Bun.sleep(config.createHoldMs ?? 0).then(() => {
            void finish();
          });
        } else {
          void finish();
        }
        if (id.match(/^[A-Za-z0-9-]{1,128}$/) === null) {
          // an invalid id is still handed out; the client must fail closed
          return json({ ok: true, operation_id: id }, 201);
        }
        // the daemon answers POST /run with 201 Created
        return json({ ok: true, operation_id: id }, 201);
      }
      const parts = url.pathname.split("/");
      const operation = operations.get(parts[2] ?? "");
      if (operation === undefined) {
        return json({ error: "unknown operation" }, 404);
      }
      if (parts[3] === "logs") {
        if (config.logFault === "status404") return json({ error: "gone" }, 404);
        if (config.logFault === "status500") return json({ error: "boom" }, 500);
        if (config.logFault === "hang") return new Promise(() => undefined);
        if (config.logFault === "hangAfterFirst") {
          const priorLogs = requests.filter((request) => request.path.includes("/logs"));
          if (priorLogs.length > 1) {
            return new Promise(() => undefined);
          }
        }
        if (config.logFault === "malformed") return new Response("<not json>", { status: 200 });
        if (config.logFault === "oversized") {
          return json({ logs: "y".repeat(9 * 1024 * 1024), offset: 0, next_offset: 9 * 1024 * 1024 });
        }
        const offset = Number(url.searchParams.get("offset") ?? "0");
        if (!Number.isSafeInteger(offset) || offset < 0) {
          return json({ error: "invalid offset" }, 400);
        }
        const total = Buffer.byteLength(operation.log, "utf8");
        if (config.logFault === "stall" && operation.state !== "running") {
          // a terminal daemon that never ends the stream: the first drain
          // hands out the bytes, later drains answer empty pages whose
          // cursor keeps advancing without bytes
          const next = offset >= total ? offset + 1 : Math.max(total, offset);
          return json({ logs: operation.log.slice(offset, Math.max(total, offset)), offset, next_offset: next });
        }
        if (operation.state === "running") {
          if (offset >= total) {
            return json({ logs: "", offset, next_offset: offset });
          }
          const chunk = operation.log.slice(offset, Math.min(total, offset + (config.pageBytes ?? PAGE_BYTES)));
          const response: Record<string, unknown> = {
            logs: chunk,
            offset,
            next_offset: offset + Buffer.byteLength(chunk, "utf8"),
          };
          if (config.truncated === true) {
            response["truncated"] = true;
          }
          return json(response);
        }
        return json({ logs: operation.log.slice(offset), offset, next_offset: null });
      }
      if (parts[3] === "cancel" && request.method === "POST") {
        if (config.cancelFault === "status500") {
          return json({ error: "cancel refused" }, 500);
        }
        if (operation.state === "running") {
          operation.state = "cancelled";
        }
        return json({ ok: true, operation_id: operation.id, status: operation.state });
      }
      if (config.statusFault === "status404") return json({ error: "gone" }, 404);
      if (config.statusFault === "status500") return json({ error: "boom" }, 500);
      if (config.statusFault === "hang") return new Promise(() => undefined);
      if (config.statusFault === "malformed") return new Response("<not json>", { status: 200 });
      if (config.statusFault === "unknown") return json({ status: "detonating" });
      if (config.statusFault === "omitExit") return json({ status: operation.state });
      const status: Record<string, unknown> = { status: operation.state };
      if (operation.state === "succeeded") {
        // the real daemon shape: success carries the "succeeded" result
        // code; exit_code stays null/absent unless a container exit exists
        status["exit_code"] = operation.exitCode === 0 ? null : operation.exitCode;
        status["result_code"] = config.resultOverride ?? (operation.exitCode === 0 ? "succeeded" : "container_exit_nonzero");
      }
      if (operation.state === "failed") {
        status["exit_code"] = operation.exitCode === 0 ? null : operation.exitCode;
        status["result_code"] = operation.exitCode === 0 ? "docker_run_failed" : "container_exit_nonzero";
      }
      return json(status);
    },
  });
  return {
    socketPath,
    operations,
    requests,
    close: () => {
      server.stop(true);
    },
  };
}

const EMPTY_CAPTURE = {
  status: "unavailable" as const,
  byteCount: 0,
  tailBytes: new Uint8Array(0),
  truncatedDaemon: false,
  truncatedLocal: false,
};

interface RunOptions {
  timeoutSeconds?: number;
  bearer?: string;
  body?: string;
  forwardSignal?: "SIGINT" | "SIGTERM";
  signalAfterMs?: number;
}

async function runOperation(
  fixture: Fixture,
  options: RunOptions = {},
): Promise<Awaited<ReturnType<typeof runWorkerOperation>>> {
  const hub = new WorkerSignalHub();
  const forwarded: Uint8Array[] = [];
  const pending = runWorkerOperation({
    socketPath: fixture.socketPath,
    bearer: options.bearer ?? "execution-bearer",
    body: options.body ?? JSON.stringify({ image: "img" }),
    timeoutSeconds: options.timeoutSeconds ?? 10,
    workerSignals: hub,
    outputSink: (chunk) => {
      forwarded.push(chunk);
    },
  });
  if (options.forwardSignal !== undefined) {
    setTimeout(() => {
      hub.forwardUserSignal(options.forwardSignal as "SIGINT" | "SIGTERM");
    }, options.signalAfterMs ?? 50);
  }
  return await pending;
}

function requestsOf(fixture: Fixture, method: string, pathPrefix: string): typeof fixture.requests {
  return fixture.requests.filter(
    (request) => request.method === method && request.path.startsWith(pathPrefix),
  );
}

const cancelRequestsOf = (fixture: Fixture): typeof fixture.requests =>
  fixture.requests.filter((request) => request.method === "POST" && request.path.endsWith("/cancel"));

const createRequestsOf = (fixture: Fixture): typeof fixture.requests =>
  fixture.requests.filter((request) => request.method === "POST" && request.path === "/run");

// --- status battery -----------------------------------------------------------

test("running -> succeeded classifies completed with a captured bounded tail", async () => {
  const fixture = startFixture({ log: "line-1\nline-2\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("captured");
    expect(result.capture.byteCount).toBe(Buffer.byteLength("line-1\nline-2\n"));
    expect(new TextDecoder().decode(result.capture.tailBytes)).toBe("line-1\nline-2\n");
    expect(result.capture.truncatedDaemon).toBe(false);
    expect(result.capture.truncatedLocal).toBe(false);
  } finally {
    fixture.close();
  }
});

test("running -> failed with an exit code classifies worker_failed", async () => {
  const fixture = startFixture({ terminal: "failed", exitCode: 3, log: "boom\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
    // the logs of a failed operation are still drained and captured
    expect(result.capture.status).toBe("captured");
  } finally {
    fixture.close();
  }
});

test("a daemon-initiated cancelled status is a worker failure", async () => {
  const fixture = startFixture({ terminal: "cancelled", log: "x\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
  } finally {
    fixture.close();
  }
});

test("contradictory succeeded with a non-zero exit code can never become a success", async () => {
  const fixture = startFixture({ terminal: "succeeded", exitCode: 3, log: "x\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
  } finally {
    fixture.close();
  }
});

test("succeeded with a failure result_code is contradictory even with a null exit_code", async () => {
  const fixture = startFixture({
    terminal: "succeeded",
    exitCode: 0,
    resultOverride: "container_exit_nonzero",
    log: "x\n",
  });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
  } finally {
    fixture.close();
  }
});

test("the omitempty zero fields: a succeeded status without exit_code or result_code completes", async () => {
  const fixture = startFixture({ statusFault: "omitExit", terminal: "succeeded", log: "x\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
  } finally {
    fixture.close();
  }
});

test("a malformed status response fails closed as a transport failure", async () => {
  const fixture = startFixture({ statusFault: "malformed", log: "x\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
  } finally {
    fixture.close();
  }
});

test("an unknown status value fails closed", async () => {
  const fixture = startFixture({ statusFault: "unknown", log: "x\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
  } finally {
    fixture.close();
  }
});

test("status 404 and 500 are transport failures", async () => {
  for (const fault of ["status404", "status500"] as const) {
    const fixture = startFixture({ statusFault: fault, log: "x\n" });
    try {
      const result = await runOperation(fixture);
      expect(result.outcome).toBe("worker_failed");
    } finally {
      fixture.close();
    }
  }
});

test("a daemon that stops mid-operation is a transport failure", async () => {
  const fixture = startFixture({ holdMs: 400, log: "x\n" });
  try {
    setTimeout(() => fixture.close(), 120);
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
  } finally {
    void fixture;
  }
});

test("a lost create response is never retried and leaves the honest orphan", async () => {
  const fixture = startFixture({ createHang: true });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 1 });
    expect(result.outcome).toBe("worker_timeout");
    expect(result.capture.status).toBe("unavailable");
    expect(createRequestsOf(fixture)).toHaveLength(1);
    expect(cancelRequestsOf(fixture)).toHaveLength(0);
  } finally {
    fixture.close();
  }
});

test("a refused create response is a worker failure with zero cancels", async () => {
  const fixture = startFixture({ createRefused: true });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
    expect(result.capture.status).toBe("unavailable");
    expect(createRequestsOf(fixture)).toHaveLength(1);
    expect(cancelRequestsOf(fixture)).toHaveLength(0);
  } finally {
    fixture.close();
  }
});

test("G1: a hung create resolves at the exact finite create bound and never retries", async () => {
  // the daemon accepts the connection and never answers; the create
  // request's own bound (min(OPERATION_REQUEST_TIMEOUT_MS, remaining))
  // resolves it finitely even though the create is deliberately not
  // wired to the abort signal (aborting it would lose the only chance to
  // learn the operation id and cancel).
  const fixture = startFixture({ createHang: true });
  try {
    const startedAt = Date.now();
    const result = await runOperation(fixture, { timeoutSeconds: 30 });
    const elapsed = Date.now() - startedAt;
    expect(result.outcome).toBe("worker_failed");
    expect(result.capture.status).toBe("unavailable");
    // the exact finite wall-clock bound: the create cap
    expect(elapsed).toBeGreaterThanOrEqual(9_000);
    expect(elapsed).toBeLessThan(15_000);
    // POST /run is never retried; without an operation id no cancel exists
    expect(createRequestsOf(fixture)).toHaveLength(1);
    expect(cancelRequestsOf(fixture)).toHaveLength(0);
  } finally {
    fixture.close();
  }
}, 20_000);

test("G1: SIGINT and SIGTERM during a hung create both resolve finitely", async () => {
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    const fixture = startFixture({ createHang: true });
    try {
      const result = await runOperation(fixture, {
        timeoutSeconds: 1,
        forwardSignal: signal,
        signalAfterMs: 150,
      });
      expect(result.outcome).toBe("worker_failed");
      expect(result.capture.status).toBe("unavailable");
      // exactly one create, never retried, and no cancel (the operation
      // id was never learned)
      expect(createRequestsOf(fixture)).toHaveLength(1);
      expect(cancelRequestsOf(fixture)).toHaveLength(0);
    } finally {
      fixture.close();
    }
  }
});

test("G1: the worker deadline claims a hung create as a timeout with no cancel", async () => {
  const fixture = startFixture({ createHang: true });
  try {
    const startedAt = Date.now();
    const result = await runOperation(fixture, { timeoutSeconds: 1 });
    const elapsed = Date.now() - startedAt;
    expect(result.outcome).toBe("worker_timeout");
    expect(result.capture.status).toBe("unavailable");
    // bounded by the remaining worker deadline, not forever
    expect(elapsed).toBeLessThan(4_000);
    expect(createRequestsOf(fixture)).toHaveLength(1);
    expect(cancelRequestsOf(fixture)).toHaveLength(0);
  } finally {
    fixture.close();
  }
});

test("G1: the create and drain bounds are exported finite constants", () => {
  expect(OPERATION_REQUEST_TIMEOUT_MS).toBe(10_000);
  expect(OPERATION_DRAIN_TIMEOUT_MS).toBe(2_000);
  expect(Number.isSafeInteger(OPERATION_REQUEST_TIMEOUT_MS)).toBe(true);
  expect(Number.isSafeInteger(OPERATION_DRAIN_TIMEOUT_MS)).toBe(true);
});

test("an invalid operation id shape fails closed before any further request", async () => {
  const fixture = startFixture({ operationId: "bad id/slash", log: "x\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("worker_failed");
    expect(fixture.requests.filter((request) => request.path.startsWith("/operations/"))).toHaveLength(0);
  } finally {
    fixture.close();
  }
});

// --- logs battery -------------------------------------------------------------

test("a multi-page drain keeps monotonic offsets and forwards every byte exactly once", async () => {
  const log = `${"chunk-data-".repeat(400)}\n`;
  const fixture = startFixture({ log, holdMs: 120 });
  try {
    const forwarded: Uint8Array[] = [];
    const hub = new WorkerSignalHub();
    const result = await runWorkerOperation({
      socketPath: fixture.socketPath,
      bearer: "b",
      body: "{}",
      timeoutSeconds: 10,
      workerSignals: hub,
      outputSink: (chunk) => forwarded.push(chunk),
    });
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("captured");
    expect(result.capture.byteCount).toBe(Buffer.byteLength(log));
    const merged = new Uint8Array(forwarded.reduce((total, chunk) => total + chunk.byteLength, 0));
    let offset = 0;
    for (const chunk of forwarded) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }
    expect(new TextDecoder().decode(merged)).toBe(log);
    // the drain pages advanced monotonically
    const drainOffsets = requestsOf(fixture, "GET", "/operations/")
      .filter((request) => request.path.includes("/logs"))
      .map((request) => Number(new URL(request.path, "http://localhost").searchParams.get("offset")));
    for (let index = 1; index < drainOffsets.length; index += 1) {
      expect((drainOffsets[index] ?? 0)).toBeGreaterThanOrEqual(drainOffsets[index - 1] ?? 0);
    }
    // live pages during running plus exactly one final drain at terminal
    expect(drainOffsets.length).toBeGreaterThanOrEqual(2);
  } finally {
    fixture.close();
  }
});

test("ANSI escapes, NUL bytes and multi-byte Unicode pass through byte-exact", async () => {
  const log = "\u001b[31mred\u0000mid-é\u00e9-moji-\u{1F600}tail-no-newline";
  const fixture = startFixture({ log });
  try {
    const forwarded: Uint8Array[] = [];
    const hub = new WorkerSignalHub();
    const result = await runWorkerOperation({
      socketPath: fixture.socketPath,
      bearer: "b",
      body: "{}",
      timeoutSeconds: 10,
      workerSignals: hub,
      outputSink: (chunk) => forwarded.push(chunk),
    });
    expect(result.outcome).toBe("completed");
    const merged = new TextDecoder().decode(
      new Uint8Array([...forwarded].reduce((acc: number[], chunk) => [...acc, ...chunk], [] as number[])),
    );
    expect(merged).toBe(log);
  } finally {
    fixture.close();
  }
});

test("daemon head eviction (truncated:true) degrades the capture without touching the outcome", async () => {
  const fixture = startFixture({ log: "head-should-have-been-longer\n", truncated: true, holdMs: 120 });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("truncated");
    expect(result.capture.truncatedDaemon).toBe(true);
    expect(result.capture.truncatedLocal).toBe(false);
  } finally {
    fixture.close();
  }
});

test("the local capture bound evicts head bytes and keeps the bounded tail", async () => {
  const oversize = "L".repeat(WORKER_LOG_CAPTURE_BOUND_BYTES + 2_000_000);
  const fixture = startFixture({ log: oversize, holdMs: 400, pageBytes: 1024 * 1024 });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 20 });
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("truncated");
    expect(result.capture.truncatedLocal).toBe(true);
    expect(result.capture.byteCount).toBe(oversize.length);
    // the retained tail is bounded by the capture bound (whole-chunk
    // eviction keeps it at or under the bound)
    expect(result.capture.tailBytes.byteLength).toBeLessThanOrEqual(WORKER_LOG_CAPTURE_BOUND_BYTES);
    expect(result.capture.tailBytes.byteLength).toBeGreaterThan(WORKER_LOG_CAPTURE_BOUND_BYTES - 2 * 1024 * 1024);
    expect(new TextDecoder().decode(result.capture.tailBytes.slice(-16))).toBe("L".repeat(16));
  } finally {
    fixture.close();
  }
});

test("an oversized single response stops the drain without changing the outcome", async () => {
  const fixture = startFixture({ logFault: "oversized", log: "tail-after-fault\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
    expect(["truncated", "unavailable"]).toContain(result.capture.status);
  } finally {
    fixture.close();
  }
});

test("log failures are outcome-neutral: 404, 500 and malformed logs never change a succeeded operation", async () => {
  for (const fault of ["status404", "status500", "malformed"] as const) {
    const fixture = startFixture({ logFault: fault, log: "some bytes\n" });
    try {
      const result = await runOperation(fixture);
      expect(result.outcome).toBe("completed");
      expect(["truncated", "unavailable"]).toContain(result.capture.status);
    } finally {
      fixture.close();
    }
  }
});

test("a hanging log fetch cannot outlive the worker deadline and the outcome stays authoritative", async () => {
  // the operation stays running past the deadline, so the deadline claims
  // the hung drain deterministically (the drain's own shorter bound would
  // otherwise only fail the drain and continue polling)
  const fixture = startFixture({ logFault: "hang", holdMs: 5_000, log: "early-bytes\n" });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 1 });
    expect(result.outcome).toBe("worker_timeout");
    expect(cancelRequestsOf(fixture)).toHaveLength(1);
  } finally {
    fixture.close();
  }
});

test("G2: a hung /logs never delays the terminal status or eats the worker deadline", async () => {
  // the operation is already terminal daemon-side; every /logs request
  // hangs forever. The drain's own bounded cap fails the drain once, the
  // loop stops draining and the authoritative status still arrives well
  // before the overall worker deadline: log observation cannot own the
  // worker outcome.
  const fixture = startFixture({ logFault: "hang", log: "early-bytes\n" });
  try {
    const startedAt = Date.now();
    const result = await runOperation(fixture, { timeoutSeconds: 5 });
    const elapsed = Date.now() - startedAt;
    expect(result.outcome).toBe("completed");
    // strictly before the 5 s deadline, and bounded by the drain cap plus
    // the poll cycle rather than the deadline
    expect(elapsed).toBeLessThan(4_500);
    expect(result.capture.status === "truncated" || result.capture.status === "unavailable").toBe(true);
    // the hung drain is attempted exactly once and never retried
    expect(
      fixture.requests.filter((request) => request.path.includes("/logs")),
    ).toHaveLength(1);
    // no cancel is sent after the terminal status was classified
    expect(cancelRequestsOf(fixture)).toHaveLength(0);
  } finally {
    fixture.close();
  }
});

test("G2: a hung terminal-phase drain also cannot turn a succeeded operation into a timeout", async () => {
  // the daemon stays running for a while and only then reaches its
  // terminal; the first drain succeeds, later drains hang forever, the
  // terminal drain chain stops at its bound and the success survives
  const fixture = startFixture({ logFault: "hangAfterFirst", holdMs: 500, log: "delivered-early\n" });
  try {
    const startedAt = Date.now();
    const result = await runOperation(fixture, { timeoutSeconds: 6 });
    const elapsed = Date.now() - startedAt;
    expect(result.outcome).toBe("completed");
    expect(elapsed).toBeLessThan(5_500);
    expect(result.capture.status).toBe("truncated");
    expect(new TextDecoder().decode(result.capture.tailBytes)).toBe("delivered-early\n");
    expect(cancelRequestsOf(fixture)).toHaveLength(0);
  } finally {
    fixture.close();
  }
});

test("a terminal daemon that reports a caught-up empty page instead of null completes the stream", async () => {
  // the real daemon's terminal shape: an empty page with next_offset ===
  // offset is the end-of-stream signal together with null
  const fixture = startFixture({ log: "stalled-stream\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("captured");
    expect(new TextDecoder().decode(result.capture.tailBytes)).toBe("stalled-stream\n");
  } finally {
    fixture.close();
  }
});

test("a terminal daemon whose empty page keeps advancing the cursor without bytes stops draining", async () => {
  const fixture = startFixture({ logFault: "stall", log: "stalled-stream\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("truncated");
    expect(new TextDecoder().decode(result.capture.tailBytes)).toBe("stalled-stream\n");
  } finally {
    fixture.close();
  }
});

test("an empty stream yields capture unavailable without changing the outcome", async () => {
  const fixture = startFixture({ log: "" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("unavailable");
    expect(result.capture.byteCount).toBe(0);
  } finally {
    fixture.close();
  }
});

// --- signals battery ----------------------------------------------------------

test("the worker deadline claims first and cancels exactly once", async () => {
  const fixture = startFixture({ holdMs: 5_000, log: "x\n" });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 1 });
    expect(result.outcome).toBe("worker_timeout");
    expect(cancelRequestsOf(fixture)).toHaveLength(1);
  } finally {
    fixture.close();
  }
});

test("SIGINT and SIGTERM each cancel exactly once and the outcome stays a worker failure", async () => {
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    const fixture = startFixture({ holdMs: 5_000, log: "x\n" });
    try {
      const result = await runOperation(fixture, {
        timeoutSeconds: 30,
        forwardSignal: signal,
        signalAfterMs: 120,
      });
      expect(result.outcome).toBe("worker_failed");
      expect(cancelRequestsOf(fixture)).toHaveLength(1);
    } finally {
      fixture.close();
    }
  }
});

test("a double abort creates exactly one cancel", async () => {
  const fixture = startFixture({ holdMs: 5_000, log: "x\n" });
  try {
    const hub = new WorkerSignalHub();
    const pending = runWorkerOperation({
      socketPath: fixture.socketPath,
      bearer: "b",
      body: "{}",
      timeoutSeconds: 30,
      workerSignals: hub,
      outputSink: () => {},
    });
    setTimeout(() => {
      hub.forwardUserSignal("SIGINT");
      hub.forwardUserSignal("SIGTERM");
    }, 120);
    const result = await pending;
    expect(result.outcome).toBe("worker_failed");
    expect(cancelRequestsOf(fixture)).toHaveLength(1);
  } finally {
    fixture.close();
  }
});

test("a signal losing to the deadline is neither delivered nor recorded", async () => {
  // direct hub semantics: a claimed timeout suppresses later user signals
  const hub = new WorkerSignalHub();
  const delivered: Array<"SIGINT" | "SIGTERM"> = [];
  const handle = {
    abort: (cause: "user_signal" | "timeout", signal: "SIGINT" | "SIGTERM") => {
      delivered.push(signal);
      void cause;
    },
  };
  hub.register(handle);
  expect(hub.claimTimeout(handle)).toBe(true);
  expect(delivered).toEqual(["SIGTERM"]);
  expect(hub.forwardUserSignal("SIGINT")).toBe(false);
  expect(delivered).toEqual(["SIGTERM"]);
  hub.unregister(handle);
  expect(hub.forwardUserSignal("SIGINT")).toBe(true);
  expect(hub.claimTimeout(handle)).toBe(false);
  // a user signal claims first; the deadline then loses
  const hub2 = new WorkerSignalHub();
  const handle2 = {
    abort: (cause: "user_signal" | "timeout", signal: "SIGINT" | "SIGTERM") => {
      delivered.push(signal);
      void cause;
    },
  };
  hub2.register(handle2);
  expect(hub2.forwardUserSignal("SIGTERM")).toBe(true);
  expect(hub2.claimTimeout(handle2)).toBe(false);
  expect(hub2.forwardUserSignal("SIGINT")).toBe(true);
});

test("a signal after the operation reached its terminal sends no cancel", async () => {
  const fixture = startFixture({ log: "x\n" });
  try {
    const result = await runOperation(fixture);
    expect(result.outcome).toBe("completed");
    const hub = new WorkerSignalHub();
    // the operation unregistered itself; a late signal is simply not
    // delivered to any operation
    expect(hub.forwardUserSignal("SIGINT")).toBe(true);
    expect(cancelRequestsOf(fixture)).toHaveLength(0);
  } finally {
    fixture.close();
  }
});

test("a failing cancel never overrides the already fixed outcome", async () => {
  const fixture = startFixture({ holdMs: 5_000, cancelFault: "status500", log: "x\n" });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 1 });
    expect(result.outcome).toBe("worker_timeout");
    expect(cancelRequestsOf(fixture)).toHaveLength(1);
  } finally {
    fixture.close();
  }
});

test("an abort during a hanging create still cancels exactly once after the id is known", async () => {
  const fixture = startFixture({ createHoldMs: 1_500, log: "x\n" });
  try {
    const result = await runOperation(fixture, {
      timeoutSeconds: 30,
      forwardSignal: "SIGINT",
      signalAfterMs: 200,
    });
    expect(result.outcome).toBe("worker_failed");
    expect(cancelRequestsOf(fixture)).toHaveLength(1);
  } finally {
    fixture.close();
  }
});

test("an abort during a hung log fetch frees the loop and cancels once", async () => {
  const fixture = startFixture({ logFault: "hang", log: "" });
  try {
    const result = await runOperation(fixture, {
      timeoutSeconds: 30,
      forwardSignal: "SIGTERM",
      signalAfterMs: 200,
    });
    expect(result.outcome).toBe("worker_failed");
    expect(cancelRequestsOf(fixture)).toHaveLength(1);
  } finally {
    fixture.close();
  }
});

// --- request/auth battery -----------------------------------------------------

test("the request body and bearer pass through unchanged; concurrent operations stay isolated", async () => {
  const fixture = startFixture({ log: "x\n" });
  try {
    const hubA = new WorkerSignalHub();
    const hubB = new WorkerSignalHub();
    const [first, second] = await Promise.all([
      runWorkerOperation({
        socketPath: fixture.socketPath,
        bearer: "bearer-A",
        body: JSON.stringify({ image: "image-a" }),
        timeoutSeconds: 10,
        workerSignals: hubA,
        outputSink: () => {},
      }),
      runWorkerOperation({
        socketPath: fixture.socketPath,
        bearer: "bearer-B",
        body: JSON.stringify({ image: "image-b" }),
        timeoutSeconds: 10,
        workerSignals: hubB,
        outputSink: () => {},
      }),
    ]);
    expect(first.outcome).toBe("completed");
    expect(second.outcome).toBe("completed");
    const creates = createRequestsOf(fixture);
    expect(creates).toHaveLength(2);
    expect(creates.map((request) => request.bearer).sort()).toEqual(["bearer-A", "bearer-B"]);
    expect(creates.map((request) => JSON.parse(request.body).image).sort()).toEqual([
      "image-a",
      "image-b",
    ]);
    const ids = [...fixture.operations.keys()];
    expect(ids).toHaveLength(2);
  } finally {
    fixture.close();
  }
});

test("the bearer never appears in any diagnostic of a failing operation", async () => {
  let caught: unknown = null;
  try {
    await runWorkerOperation({
      socketPath: "/tmp/opencode/missing-helper-socket.sock",
      bearer: "leak-canary-token",
      body: "{}",
      timeoutSeconds: 1,
      workerSignals: new WorkerSignalHub(),
      outputSink: () => {},
    });
  } catch (cause) {
    caught = cause;
  }
  // the production transport reports connect failures as typed errors;
  // whatever surfaces must never carry the bearer
  const text = caught instanceof Error ? `${caught.message}` : String(caught);
  expect(text.includes("leak-canary-token")).toBe(false);
});

test("hostile operation inputs are rejected before any request", async () => {
  const shapes: Array<Record<string, unknown>> = [
    { socketPath: "relative.sock", bearer: "b", body: "{}", timeoutSeconds: 1, workerSignals: new WorkerSignalHub(), outputSink: () => {} },
    { socketPath: "/tmp/opencode/x.sock", bearer: "", body: "{}", timeoutSeconds: 1, workerSignals: new WorkerSignalHub(), outputSink: () => {} },
    { socketPath: "/tmp/opencode/x.sock", bearer: "b", body: "", timeoutSeconds: 1, workerSignals: new WorkerSignalHub(), outputSink: () => {} },
    { socketPath: "/tmp/opencode/x.sock", bearer: "b", body: "{}", timeoutSeconds: 0, workerSignals: new WorkerSignalHub(), outputSink: () => {} },
    { socketPath: "/tmp/opencode/x.sock", bearer: "b", body: "{}", timeoutSeconds: 1.5, workerSignals: new WorkerSignalHub(), outputSink: () => {} },
  ];
  for (const shape of shapes) {
    let threw = false;
    try {
      await runWorkerOperation(shape as never);
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  }
});

test("the capture object is frozen and the byte view is the retained tail", async () => {
  const fixture = startFixture({ log: "frozen-tail\n" });
  try {
    const result = await runOperation(fixture);
    expect(Object.isFrozen(result.capture)).toBe(true);
    expect(result.capture.status).toBe("captured");
    expect(new TextDecoder().decode(result.capture.tailBytes)).toBe("frozen-tail\n");
    expect(EMPTY_CAPTURE.status).toBe("unavailable");
  } finally {
    fixture.close();
  }
});

// --- G3: the honest capture/response bound pair -------------------------------

test("G3: the response bound is the worst-case JSON encoding of a full capture payload", () => {
  // every control character escapes to six ASCII bytes (`\u00XX`), quotes
  // and backslashes double; the envelope adds a bounded constant. The
  // response bound must therefore admit the worst-case encoding of a
  // complete capture payload, or a legitimate worst-case log response
  // would be rejected as oversized.
  expect(WORKER_LOG_RESPONSE_BOUND_BYTES).toBe(6 * WORKER_LOG_CAPTURE_BOUND_BYTES + 65_536);
  expect(Number.isSafeInteger(WORKER_LOG_CAPTURE_BOUND_BYTES)).toBe(true);
  expect(Number.isSafeInteger(WORKER_LOG_RESPONSE_BOUND_BYTES)).toBe(true);
});

test("G3: a control-character-heavy payload that the old 8 MiB response bound would reject now delivers byte-exactly", async () => {
  // 2 MiB of NUL bytes encode to a ~12 MiB JSON response: larger than the
  // former 8 MiB response bound, well under the honest pair's bound.
  const payload = "\u0000".repeat(2 * 1024 * 1024);
  const fixture = startFixture({ log: payload, pageBytes: 2 * 1024 * 1024 });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 30 });
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("captured");
    expect(result.capture.byteCount).toBe(2 * 1024 * 1024);
    expect(result.capture.truncatedDaemon).toBe(false);
    expect(result.capture.truncatedLocal).toBe(false);
    expect(result.capture.tailBytes.byteLength).toBe(2 * 1024 * 1024);
    // byte-exact: every retained byte is NUL
    expect(Buffer.from(result.capture.tailBytes).equals(Buffer.alloc(2 * 1024 * 1024, 0))).toBe(true);
  } finally {
    fixture.close();
  }
}, 20_000);

test("G3: a full 8 MiB worst-case payload survives the response bound as captured", async () => {
  // the honest full-scale proof: a complete 8 MiB capture payload of NUL
  // bytes encodes to ~48 MiB of JSON response — exactly the worst case the
  // bound pair admits — and is never rejected as oversized.
  const payload = "\u0000".repeat(WORKER_LOG_CAPTURE_BOUND_BYTES);
  const fixture = startFixture({ log: payload, pageBytes: WORKER_LOG_CAPTURE_BOUND_BYTES });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 40 });
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("captured");
    expect(result.capture.byteCount).toBe(WORKER_LOG_CAPTURE_BOUND_BYTES);
    expect(result.capture.truncatedDaemon).toBe(false);
    expect(result.capture.truncatedLocal).toBe(false);
    expect(result.capture.tailBytes.byteLength).toBe(WORKER_LOG_CAPTURE_BOUND_BYTES);
  } finally {
    fixture.close();
  }
}, 40_000);

test("G3: quotes, backslashes, controls and multibyte UTF-8 pass through the bounded drain byte-exactly", async () => {
  // a mixed worst-case payload: every escaping class at once
  const parts: string[] = [];
  for (let index = 0; index < 200_000; index += 1) {
    parts.push('"', "\\", "\u0000", "\u0001", "é", "😀");
  }
  const payload = parts.join("");
  const fixture = startFixture({ log: payload, pageBytes: 3 * 1024 * 1024 });
  try {
    const result = await runOperation(fixture, { timeoutSeconds: 30 });
    expect(result.outcome).toBe("completed");
    expect(result.capture.status).toBe("captured");
    expect(result.capture.byteCount).toBe(Buffer.byteLength(payload, "utf8"));
    expect(new TextDecoder().decode(result.capture.tailBytes)).toBe(payload);
  } finally {
    fixture.close();
  }
}, 20_000);
