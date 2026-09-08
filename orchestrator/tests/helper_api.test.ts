import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  HttpHelperTransport,
  TransportError,
  type RunOutcome,
} from "../src/helper_api.ts";
import type { WorkerSpec } from "../src/worker.ts";

interface FakeDaemonOptions {
  runResponse?: unknown;
  operationTimeline?: string[];
  exitCode?: number;
  logsChunks?: string[];
  cancelRuns?: boolean;
  pullResponse?: unknown;
}

function fakeDaemon(
  socketPath: string,
  options: FakeDaemonOptions,
): { requests: { method: string; path: string; body?: unknown }[]; stop: () => Promise<void> } {
  const requests: { method: string; path: string; body?: unknown }[] = [];
  let pollCount = 0;
  const timeline = options.operationTimeline ?? ["running", "succeeded"];
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
      requests.push({ method, path: url.pathname + url.search, body });
      const json = (payload: unknown, status = 200) =>
        new Response(JSON.stringify(payload), { status, headers: { "Content-Type": "application/json" } });
      if (method === "POST" && url.pathname === "/run") {
        return json(options.runResponse ?? { ok: true, operation_id: "op_test", status: "running" });
      }
      if (method === "GET" && url.pathname === "/operations/op_test") {
        const status = timeline[Math.min(pollCount, timeline.length - 1)] ?? "succeeded";
        pollCount++;
        return json({
          ok: true,
          operation_id: "op_test",
          status,
          ...(status === "failed" ? { exit_code: options.exitCode ?? 125, result_code: "docker_run_failed" } : {}),
        });
      }
      if (method === "GET" && url.pathname === "/operations/op_test/logs") {
        const offset = Number(url.searchParams.get("offset") ?? "0");
        const chunks = options.logsChunks ?? ["line1\n", "line2\n"];
        let consumed = 0;
        let emitted = "";
        let next = offset;
        for (const chunk of chunks) {
          if (consumed + chunk.length > offset && emitted === "") {
            emitted = chunk.slice(Math.max(0, offset - consumed));
            next = consumed + chunk.length;
          }
          consumed += chunk.length;
        }
        return json({ ok: true, operation_id: "op_test", offset, next_offset: next, truncated: false, logs: emitted });
      }
      if (method === "POST" && url.pathname === "/operations/op_test/cancel") {
        return json({ ok: true, operation_id: "op_test", status: "cancelled" });
      }
      if (method === "POST" && url.pathname === "/pull") {
        if (options.pullResponse !== undefined && (options.pullResponse as Record<string, unknown>).ok !== true) {
          return json(options.pullResponse);
        }
        return json(options.pullResponse ?? { ok: true, message: "image pulled successfully", output: "pulled\n", duration: "1s" });
      }
      return json({ ok: false, code: "not_found", message: "unexpected" }, 404);
    },
  });
  return {
    requests,
    stop: async () => {
      server.stop(true);
    },
  };
}

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

test("transport: run succeeds and streams logs", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const daemon = fakeDaemon(socketPath, {
    operationTimeline: ["running", "running", "succeeded"],
    logsChunks: ["line1\n", "line2\n"],
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    const outcome: RunOutcome = await transport.run(SPEC, "dht_bearer");
    expect(outcome.code).toBe(0);
    expect(outcome.operationId).toBe("op_test");

    const runReq = daemon.requests.find((r) => r.method === "POST" && r.path === "/run");
    expect((runReq?.body as Record<string, unknown>).image).toBe("alpine:3.22");
    expect((runReq?.body as Record<string, unknown>).environment).toEqual(SPEC.containerEnv);
    expect((runReq?.body as Record<string, unknown>).mounts).toEqual(SPEC.mounts);

    const logReqs = daemon.requests.filter((r) => r.path.startsWith("/operations/op_test/logs"));
    expect(logReqs.length).toBeGreaterThanOrEqual(2);
    expect(logReqs[0]?.path).toBe("/operations/op_test/logs?offset=0");
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: failed run maps exit_code", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const daemon = fakeDaemon(socketPath, {
    operationTimeline: ["running", "failed"],
    exitCode: 125,
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    const outcome = await transport.run(SPEC, "dht_bearer");
    expect(outcome.code).toBe(125);
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: unknown terminal status maps to exit 1", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const daemon = fakeDaemon(socketPath, {
    operationTimeline: ["running", "weird_status"],
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    const outcome = await transport.run(SPEC, "dht_bearer");
    expect(outcome.code).toBe(1);
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: cancelled operation (failed + result_code cancelled) maps to 143", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const server = Bun.serve({
    unix: socketPath,
    fetch() {
      return new Response(
        JSON.stringify({
          ok: true,
          operation_id: "op_test",
          status: "failed",
          exit_code: -1,
          result_code: "cancelled",
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    },
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    const outcome = await transport.run(SPEC, "dht_bearer");
    expect(outcome.code).toBe(143);
  } finally {
    server.stop(true);
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: invalid run response is an error", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const daemon = fakeDaemon(socketPath, { runResponse: { ok: true } });
  try {
    const transport = new HttpHelperTransport(socketPath);
    await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(TransportError);
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: daemon error body surfaces code and message", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const daemon = fakeDaemon(socketPath, {
    runResponse: { ok: false, code: "invalid_image", message: "image is required" },
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    await expect(transport.run(SPEC, "dht_bearer")).rejects.toThrow(/invalid_image: image is required/);
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: pull success", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const daemon = fakeDaemon(socketPath, {});
  try {
    const transport = new HttpHelperTransport(socketPath);
    await transport.pull("alpine:3.22", "dht_bearer");
    const pullReq = daemon.requests.find((r) => r.method === "POST" && r.path === "/pull");
    expect((pullReq?.body as Record<string, unknown>).image).toBe("alpine:3.22");
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: pull failure is an error", async () => {
  const dir = await mkdtemp(join(tmpdir(), "transport-test-"));
  const socketPath = join(dir, "test.sock");
  const daemon = fakeDaemon(socketPath, {
    pullResponse: { ok: false, code: "cli_failure", message: "cannot start docker pull" },
  });
  try {
    const transport = new HttpHelperTransport(socketPath);
    await expect(transport.pull("alpine:3.22", "dht_bearer")).rejects.toThrow(TransportError);
  } finally {
    await daemon.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("transport: endpoint unreachable surfaces a transport error", async () => {
  const missing = join(tmpdir(), "transport-missing.sock");
  const transport = new HttpHelperTransport(missing);
  await expect(transport.pull("alpine:3.22", "dht_bearer")).rejects.toThrow(TransportError);
});
