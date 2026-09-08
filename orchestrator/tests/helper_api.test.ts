import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { HttpHelperTransport, TransportError, type RunOutcome } from "../src/helper_api.ts";
import type { WorkerSpec } from "../src/worker.ts";

const SPEC: WorkerSpec = {
  image: "alpine:3.22",
  entrypoint: "/bin/sh",
  command: ["-c", "echo hi"],
  workdir: "/workspace",
  mounts: [{ source: ".", target: "/workspace" }],
  containerEnv: {
    DOCKER_HELPER_SESSION_TOKEN: "dht_child",
    SMOKE_RUN_ID: "run-1",
  },
};

interface LogChunk {
  text: string;
  truncated?: boolean;
}

interface FakeDaemonOptions {
  runStatus?: number;
  runBody?: unknown;
  runDelayMs?: number;
  statusForPoll?: (pollIndex: number, cancelledSeen: boolean) => Record<string, unknown>;
  logsChunks?: LogChunk[];
  cancelStatus?: number;
  cancelBody?: unknown;
  onEvent?: (event: string) => void;
}

interface FakeDaemon {
  socketPath: string;
  events: string[];
  cancelledSeen: () => boolean;
  stop: () => Promise<void>;
}

function chunkRanges(chunks: LogChunk[]): Array<{ start: number; end: number; text: string; truncated: boolean }> {
  const ranges: Array<{ start: number; end: number; text: string; truncated: boolean }> = [];
  let start = 0;
  for (const chunk of chunks) {
    ranges.push({ start, end: start + chunk.text.length, text: chunk.text, truncated: chunk.truncated === true });
    start += chunk.text.length;
  }
  return ranges;
}

function fakeDaemon(socketPath: string, options: FakeDaemonOptions): FakeDaemon {
  const events: string[] = [];
  let cancelledSeenFlag = false;
  const ranges = chunkRanges(options.logsChunks ?? []);
  const totalLength = ranges.reduce((acc, r) => acc + r.text.length, 0);
  const server = Bun.serve({
    unix: socketPath,
    async fetch(request) {
      const url = new URL(request.url);
      const method = request.method;
      let body: unknown;
      try {
        body = request.body === null ? undefined : await request.json();
      } catch {
        body = undefined;
      }
      void body;
      const json = (payload: unknown, status = 200) =>
        new Response(JSON.stringify(payload), { status, headers: { "Content-Type": "application/json" } });
      if (method === "POST" && url.pathname === "/run") {
        events.push("daemon:run");
        options.onEvent?.("daemon:run");
        if ((options.runDelayMs ?? 0) > 0) {
          await Bun.sleep(options.runDelayMs ?? 0);
        }
        if (options.runBody !== undefined) {
          return json(options.runBody, options.runStatus ?? 201);
        }
        if (options.runStatus !== undefined && options.runStatus !== 201) {
          return json({ ok: false, code: "invalid_image", message: "x" }, options.runStatus);
        }
        return json({ ok: true, operation_id: "op_test", status: "running" }, options.runStatus ?? 201);
      }
      if (method === "GET" && url.pathname === "/operations/op_test") {
        events.push("daemon:status");
        options.onEvent?.("daemon:status");
        if (options.statusForPoll !== undefined) {
          return json({ ok: true, operation_id: "op_test", ...options.statusForPoll(events.filter((e) => e === "daemon:status").length, cancelledSeenFlag) });
        }
        if (cancelledSeenFlag) {
          return json({ ok: true, operation_id: "op_test", status: "failed", exit_code: -1, result_code: "cancelled" });
        }
        return json({ ok: true, operation_id: "op_test", status: "running" });
      }
      if (method === "GET" && url.pathname === "/operations/op_test/logs") {
        events.push("daemon:logs");
        options.onEvent?.("daemon:logs");
        const offset = Number(url.searchParams.get("offset") ?? "0");
        const range = ranges.find((r) => offset >= r.start && offset < r.end);
        if (range === undefined) {
          return json({
            ok: true,
            operation_id: "op_test",
            offset,
            next_offset: Math.max(offset, totalLength),
            truncated: false,
            logs: "",
          });
        }
        return json({
          ok: true,
          operation_id: "op_test",
          offset,
          next_offset: range.end,
          truncated: range.truncated,
          logs: range.text.slice(offset - range.start),
        });
      }
      if (method === "POST" && url.pathname === "/operations/op_test/cancel") {
        events.push("daemon:cancel");
        options.onEvent?.("daemon:cancel");
        if ((options.cancelStatus ?? 200) !== 200) {
          return json(options.cancelBody ?? { ok: false, code: "error", message: "cancel failed" }, options.cancelStatus ?? 500);
        }
        cancelledSeenFlag = true;
        return json(options.cancelBody ?? { ok: true, operation_id: "op_test", status: "failed", exit_code: -1, result_code: "cancelled" });
      }
      if (method === "POST" && url.pathname === "/pull") {
        events.push("daemon:pull");
        return json({ ok: true, message: "image pulled successfully", output: "pulled\n", duration: "1s" });
      }
      return json({ ok: false, code: "not_found", message: "unexpected" }, 404);
    },
  });
  return {
    socketPath,
    events,
    cancelledSeen: () => cancelledSeenFlag,
    stop: async () => {
      server.stop(true);
    },
  };
}

async function withDaemon(options: FakeDaemonOptions, fn: (daemon: FakeDaemon) => Promise<void>): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const daemon = fakeDaemon(join(dir, "test.sock"), options);
  try {
    await fn(daemon);
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
}

function captureStdout(): { written: string[]; restore: () => void } {
  const written: string[] = [];
  const original = process.stdout.write;
  process.stdout.write = ((chunk: unknown) => {
    written.push(String(chunk));
    return true;
  }) as typeof process.stdout.write;
  return { written, restore: () => { process.stdout.write = original; } };
}

function captureStderr(): { lines: string[]; restore: () => void } {
  const lines: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => {
    lines.push(args.join(" "));
  };
  return { lines, restore: () => { console.error = original; } };
}

test("transport: run succeeds (HTTP 201 + running) and streams logs once", async () => {
  await withDaemon(
    {
      statusForPoll: (poll) => (poll >= 2 ? { status: "succeeded", result_code: "succeeded" } : { status: "running" }),
      logsChunks: [{ text: "line1\n" }, { text: "line2\n" }],
    },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      const captured = captureStdout();
      try {
        const outcome: RunOutcome = await transport.run(SPEC, "dht_bearer");
        expect(outcome.code).toBe(0);
        expect(outcome.operationId).toBe("op_test");
        expect(captured.written).toEqual(["line1\n", "line2\n"]);
      } finally {
        captured.restore();
      }
      const runReq = daemon.events.indexOf("daemon:run");
      expect(runReq).toBeGreaterThanOrEqual(0);
      expect(daemon.events.filter((e) => e === "daemon:cancel").length).toBe(0);
      expect(daemon.events.filter((e) => e === "daemon:logs").length).toBeGreaterThanOrEqual(2);
    },
  );
});

test("transport: cancelled operation maps to exit 143", async () => {
  await withDaemon(
    { statusForPoll: (poll, cancelledSeen) => (cancelledSeen ? { status: "failed", exit_code: -1, result_code: "cancelled" } : { status: "running" }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      const runPromise = transport.run(SPEC, "dht_bearer");
      await Bun.sleep(50);
      await transport.cancelActive();
      const outcome = await runPromise;
      expect(outcome.code).toBe(143);
      expect(daemon.events).toContain("daemon:cancel");
    },
  );
});

test("transport: cancel is applied after operation identity when the signal arrives during POST /run", async () => {
  const events: string[] = [];
  await withDaemon(
    {
      runDelayMs: 300,
      statusForPoll: (poll, cancelledSeen) => (cancelledSeen ? { status: "failed", exit_code: -1, result_code: "cancelled" } : { status: "running" }),
      onEvent: (event) => events.push(event),
    },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      const runPromise = transport.run(SPEC, "dht_bearer");
      await Bun.sleep(50);
      expect(events).toContain("daemon:run");
      expect(events).not.toContain("daemon:status");
      expect(events).not.toContain("daemon:cancel");
      events.push("test:cancelActive-called");

      const cancelPromise = transport.cancelActive();
      const deletePromise = cancelPromise.then(() => {
        events.push("test:session-delete");
      });

      const outcome = await runPromise;
      events.push(`test:run-resolved:${outcome.code}`);
      await deletePromise;

      expect(outcome.code).toBe(143);
      const indexOfAll = (event: string) =>
        events.map((e, i) => (e === event ? i : -1)).filter((i) => i >= 0);
      const first = (event: string) => Math.min(...indexOfAll(event));
      const last = (event: string) => Math.max(...indexOfAll(event));

      // no cancel request existed when the signal arrived (no operation identity yet)
      expect(first("test:cancelActive-called")).toBeLessThan(first("daemon:status"));
      expect(first("test:cancelActive-called")).toBeLessThan(first("daemon:cancel"));
      // the pending cancellation is applied only after the identity exists
      expect(first("daemon:cancel")).toBeGreaterThan(first("daemon:run"));
      expect(first("daemon:cancel")).toBeGreaterThan(first("daemon:status"));
      // the operation reached its terminal state (cancelled) before cleanup
      expect(last("daemon:status")).toBeGreaterThan(first("daemon:cancel"));
      // the Session delete happens only after cancel confirmation (terminal state)
      expect(first("test:session-delete")).toBeGreaterThan(last("daemon:status"));
      expect(first("test:session-delete")).toBeGreaterThan(first("daemon:cancel"));
      expect(daemon.cancelledSeen()).toBe(true);
    },
  );
});

test("transport: cancel endpoint failure rejects cancelActive and the run fails", async () => {
  await withDaemon(
    { cancelStatus: 500, statusForPoll: (poll, cancelledSeen) => (cancelledSeen ? { status: "failed", exit_code: -1, result_code: "cancelled" } : { status: "running" }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      const runPromise = transport.run(SPEC, "dht_bearer");
      await Bun.sleep(50);
      const cancelOutcome = await transport.cancelActive().then(
        () => "resolved",
        (cause) => `rejected:${cause instanceof Error ? cause.message : String(cause)}`,
      );
      expect(cancelOutcome).toContain("rejected:");
      await expect(runPromise).rejects.toThrow(TransportError);
      expect(daemon.events).toContain("daemon:cancel");
      expect(daemon.cancelledSeen()).toBe(false);
    },
  );
});

test("transport: run refuses to start after cancellation was requested with no run in flight", async () => {
  await withDaemon({}, async (daemon) => {
    const transport = new HttpHelperTransport(daemon.socketPath);
    await transport.cancelActive();
    await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(
      /cancellation was requested before this run started/,
    );
    expect(daemon.events).not.toContain("daemon:run");
  });
});

test("transport: failed run maps exit_code; failed+cancelled maps to 143", async () => {
  await withDaemon(
    { statusForPoll: (poll) => (poll >= 2 ? { status: "failed", exit_code: 125, result_code: "docker_run_failed" } : { status: "running" }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      const outcome = await transport.run(SPEC, "dht_bearer");
      expect(outcome.code).toBe(125);
    },
  );
});

test("transport: POST /run must answer HTTP 201", async () => {
  await withDaemon(
    { runStatus: 200, runBody: { ok: true, operation_id: "op_test", status: "running" } },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/expected 201/);
      expect(daemon.events).not.toContain("daemon:status");
    },
  );
});

test("transport: POST /run must report status running", async () => {
  await withDaemon({ runBody: { ok: true, operation_id: "op_test", status: "queued" } }, async (daemon) => {
    const transport = new HttpHelperTransport(daemon.socketPath);
    await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/unexpected status "queued"/);
  });
});

test("transport: POST /run must include an operation_id", async () => {
  await withDaemon({ runBody: { ok: true, status: "running" } }, async (daemon) => {
    const transport = new HttpHelperTransport(daemon.socketPath);
    await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/no valid operation_id/);
  });
});

test("transport: status operation_id mismatch fails closed", async () => {
  await withDaemon(
    { statusForPoll: () => ({ status: "running", operation_id: "op_other" }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/operation_id mismatch/);
      expect(daemon.events).not.toContain("daemon:cancel");
    },
  );
});

test("transport: unknown operation status fails closed", async () => {
  await withDaemon(
    { statusForPoll: () => ({ status: "weird_status" }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/unexpected status "weird_status"/);
    },
  );
});

test("transport: succeeded with inconsistent result_code fails closed", async () => {
  await withDaemon(
    { statusForPoll: () => ({ status: "succeeded", result_code: "docker_run_failed", exit_code: 0 }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(
        /status succeeded with result_code "docker_run_failed"/,
      );
    },
  );
});

test("transport: succeeded without result_code fails closed", async () => {
  await withDaemon(
    { statusForPoll: () => ({ status: "succeeded", exit_code: 0 }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/no valid result_code/);
    },
  );
});

test("transport: failed with unknown result_code fails closed", async () => {
  await withDaemon(
    { statusForPoll: () => ({ status: "failed", exit_code: 3, result_code: "mystery" }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(
        /unexpected result_code "mystery"/,
      );
    },
  );
});

test("transport: failed without exit_code fails closed", async () => {
  await withDaemon(
    { statusForPoll: () => ({ status: "failed", result_code: "container_exit_nonzero" }) },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/no integer exit_code/);
    },
  );
});

test("transport: truncation advances offset, warns once, and drains the final fragment", async () => {
  await withDaemon(
    {
      statusForPoll: (poll) => (poll >= 3 ? { status: "succeeded", result_code: "succeeded", exit_code: 0 } : { status: "running" }),
      logsChunks: [
        { text: "AAAAAA" },
        { text: "BBBBBB", truncated: true },
        { text: "CCCCCC" },
      ],
    },
    async (daemon) => {
      const transport = new HttpHelperTransport(daemon.socketPath);
      const capturedOut = captureStdout();
      const capturedErr = captureStderr();
      try {
        const outcome = await transport.run(SPEC, "dht_bearer");
        expect(outcome.code).toBe(0);
      } finally {
        capturedOut.restore();
        capturedErr.restore();
      }
      expect(capturedOut.written).toEqual(["AAAAAA", "BBBBBB", "CCCCCC"]);
      const warnings = capturedErr.lines.filter((l) => l.includes("log buffer"));
      expect(warnings.length).toBe(1);
      const logRequests = daemon.events.filter((e) => e === "daemon:logs").length;
      expect(logRequests).toBeGreaterThanOrEqual(4);
    },
  );
});

test("transport: logs response missing next_offset fails closed", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-logs-"));
  const socketPath = join(dir, "test.sock");
  const server = Bun.serve({
    unix: socketPath,
    async fetch(request) {
      const url = new URL(request.url);
      if (request.method === "POST" && url.pathname === "/run") {
        return new Response(JSON.stringify({ ok: true, operation_id: "op_test", status: "running" }), { status: 201 });
      }
      if (url.pathname === "/operations/op_test") {
        return new Response(JSON.stringify({ ok: true, operation_id: "op_test", status: "running" }));
      }
      if (url.pathname === "/operations/op_test/logs") {
        return new Response(JSON.stringify({ ok: true, operation_id: "op_test", offset: 0, truncated: false, logs: "x" }));
      }
      return new Response(JSON.stringify({ ok: false }), { status: 404 });
    },
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/no valid next_offset/);
  } finally {
    server.stop(true);
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: logs operation_id mismatch fails closed", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-logs-"));
  const socketPath = join(dir, "test.sock");
  const server = Bun.serve({
    unix: socketPath,
    async fetch(request) {
      const url = new URL(request.url);
      if (request.method === "POST" && url.pathname === "/run") {
        return new Response(JSON.stringify({ ok: true, operation_id: "op_test", status: "running" }), { status: 201 });
      }
      if (url.pathname === "/operations/op_test") {
        return new Response(JSON.stringify({ ok: true, operation_id: "op_test", status: "running" }));
      }
      if (url.pathname === "/operations/op_test/logs") {
        return new Response(
          JSON.stringify({ ok: true, operation_id: "op_other", offset: 0, next_offset: 1, truncated: false, logs: "x" }),
        );
      }
      return new Response(JSON.stringify({ ok: false }), { status: 404 });
    },
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/operation_id mismatch/);
  } finally {
    server.stop(true);
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: pull success and failure", async () => {
  await withDaemon({}, async (daemon) => {
    const transport = new HttpHelperTransport(daemon.socketPath);
    await transport.pull("alpine:3.22", "dht_bearer");
    expect(daemon.events).toContain("daemon:pull");
  });
  const dir = await mkdtemp(join(tmpdir(), "transport-pull-"));
  const socketPath = join(dir, "test.sock");
  const server = Bun.serve({
    unix: socketPath,
    fetch() {
      return new Response(JSON.stringify({ ok: false, code: "cli_failure", message: "cannot start docker pull" }));
    },
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    await expect(transport.pull("alpine:3.22", "dht_bearer")).rejects.toThrow(TransportError);
  } finally {
    server.stop(true);
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: endpoint unreachable surfaces a transport error", async () => {
  const transport = new HttpHelperTransport(join(tmpdir(), "transport-missing.sock"));
  await expect(transport.pull("alpine:3.22", "dht_bearer")).rejects.toThrow(TransportError);
});
