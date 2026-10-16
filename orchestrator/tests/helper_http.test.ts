/**
 * Transport-owner battery for `helper_http.ts`: bounded exchanges over
 * the helper Unix socket, the explicit bearer, the strict HTTP status
 * handling, the one JSON parser owner and content-free diagnostics.
 *
 * Every exchange in this file runs against an in-process `Bun.serve`
 * fixture bound to a fresh Unix socket; the production transport
 * (`helperHttpExchange`) is exercised unmodified.
 */
import { rmSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  HelperHttpError,
  helperHttpExchange,
  parseHelperJsonObject,
} from "../src/helper_http.ts";

const roots: string[] = [];
afterAll(() => {
  for (const root of roots) {
    rmSync(root, { recursive: true, force: true });
  }
});

interface Fixture {
  socketPath: string;
  server: ReturnType<typeof Bun.serve>;
  requests: Array<{ method: string; path: string; bearer: string; body: string }>;
}

function startFixture(
  respond: (context: {
    method: string;
    path: string;
    bearer: string;
    body: string;
  }) => Response | Promise<Response>,
): Fixture {
  const root = mkdtempSync(join(tmpdir(), "helper-http-"));
  roots.push(root);
  const socketPath = join(root, "helper.sock");
  const requests: Array<{ method: string; path: string; bearer: string; body: string }> = [];
  const server = Bun.serve({
    unix: socketPath,
    async fetch(request) {
      const url = new URL(request.url);
      const bearer = (request.headers.get("Authorization") ?? "").replace(/^Bearer /, "");
      const body = await request.text();
      requests.push({ method: request.method, path: url.pathname + url.search, bearer, body });
      return await respond({ method: request.method, path: url.pathname + url.search, bearer, body });
    },
  });
  return { socketPath, server, requests };
}

const OK_RESPONSE = { ok: true, operation_id: "op-1" };

test("the exchange carries the bearer header and the exact body to the daemon", async () => {
  const fixture = startFixture(() => Response.json(OK_RESPONSE));
  try {
    const response = await helperHttpExchange({
      socketPath: fixture.socketPath,
      bearer: "secret-bearer",
      method: "POST",
      path: "/run",
      body: JSON.stringify({ image: "i" }),
      timeoutMs: 5_000,
      maxResponseBytes: 64 * 1024,
    });
    expect(response.status).toBe(200);
    expect(fixture.requests).toHaveLength(1);
    expect(fixture.requests[0]?.bearer).toBe("secret-bearer");
    expect(fixture.requests[0]?.body).toBe(JSON.stringify({ image: "i" }));
    expect(fixture.requests[0]?.path).toBe("/run");
    expect(JSON.parse(response.bodyText)).toEqual(OK_RESPONSE);
  } finally {
    fixture.server.stop(true);
  }
});

test("a non-200 status is surfaced to the caller, never classified by the transport", async () => {
  const fixture = startFixture(() => new Response("nope", { status: 503 }));
  try {
    const response = await helperHttpExchange({
      socketPath: fixture.socketPath,
      bearer: "b",
      method: "GET",
      path: "/operations/op-1",
      timeoutMs: 5_000,
      maxResponseBytes: 64 * 1024,
    });
    expect(response.status).toBe(503);
  } finally {
    fixture.server.stop(true);
  }
});

test("a response exceeding the size bound fails with response_too_large", async () => {
  const fixture = startFixture(() => new Response("x".repeat(4096), { status: 200 }));
  try {
    let caught: unknown = null;
    try {
      await helperHttpExchange({
        socketPath: fixture.socketPath,
        bearer: "b",
        method: "GET",
        path: "/operations/op-1",
        timeoutMs: 5_000,
        maxResponseBytes: 1024,
      });
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBeInstanceOf(HelperHttpError);
    expect((caught as HelperHttpError).reason).toBe("response_too_large");
  } finally {
    fixture.server.stop(true);
  }
});

test("a hanging response fails with the exchange timeout", async () => {
  const fixture = startFixture(() => new Promise(() => undefined));
  try {
    let caught: unknown = null;
    try {
      await helperHttpExchange({
        socketPath: fixture.socketPath,
        bearer: "b",
        method: "GET",
        path: "/operations/op-1",
        timeoutMs: 300,
        maxResponseBytes: 64 * 1024,
      });
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBeInstanceOf(HelperHttpError);
    expect((caught as HelperHttpError).reason).toBe("timeout");
  } finally {
    fixture.server.stop(true);
  }
});

test("an external abort signal rejects the exchange with aborted", async () => {
  const fixture = startFixture(() => new Promise(() => undefined));
  try {
    const controller = new AbortController();
    const pending = helperHttpExchange({
      socketPath: fixture.socketPath,
      bearer: "b",
      method: "GET",
      path: "/operations/op-1",
      timeoutMs: 30_000,
      maxResponseBytes: 64 * 1024,
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(), 150);
    let caught: unknown = null;
    try {
      await pending;
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBeInstanceOf(HelperHttpError);
    expect((caught as HelperHttpError).reason).toBe("aborted");
  } finally {
    fixture.server.stop(true);
  }
});

test("an unreachable socket fails with connect_failed and never leaks the bearer", async () => {
  let caught: unknown = null;
  try {
    await helperHttpExchange({
      socketPath: "/tmp/opencode/definitely-missing-helper.sock",
      bearer: "super-secret-token",
      method: "GET",
      path: "/operations/op-1",
      timeoutMs: 2_000,
      maxResponseBytes: 64 * 1024,
    });
  } catch (cause) {
    caught = cause;
  }
  expect(caught).toBeInstanceOf(HelperHttpError);
  const error = caught as HelperHttpError;
  expect(error.reason).toBe("connect_failed");
  expect(error.message).not.toContain("super-secret-token");
});

test("hostile request shapes are rejected before any socket work", async () => {
  const shapes: Array<Parameters<typeof helperHttpExchange>[0]> = [
    {
      socketPath: "relative.sock",
      bearer: "b",
      method: "GET",
      path: "/x",
      timeoutMs: 1000,
      maxResponseBytes: 64,
    },
    {
      socketPath: "/abs.sock",
      bearer: "",
      method: "GET",
      path: "/x",
      timeoutMs: 1000,
      maxResponseBytes: 64,
    },
    {
      socketPath: "/abs.sock",
      bearer: "b",
      method: "GET",
      path: "no-leading-slash",
      timeoutMs: 1000,
      maxResponseBytes: 64,
    },
    {
      socketPath: "/abs.sock",
      bearer: "b",
      method: "GET",
      path: "/x?inject=1 extra",
      timeoutMs: 1000,
      maxResponseBytes: 64,
    },
    {
      socketPath: "/abs.sock",
      bearer: "b",
      method: "GET",
      path: "/x",
      timeoutMs: 0,
      maxResponseBytes: 64,
    },
    {
      socketPath: "/abs.sock",
      bearer: "b",
      method: "GET",
      path: "/x",
      timeoutMs: 1000,
      maxResponseBytes: 0,
    },
  ];
  for (const shape of shapes) {
    let caught: unknown = null;
    try {
      await helperHttpExchange(shape);
    } catch (cause) {
      caught = cause;
    }
    expect(caught).toBeInstanceOf(HelperHttpError);
    expect((caught as HelperHttpError).reason).toBe("request_invalid");
  }
});

test("the JSON parser owner rejects malformed and non-object bodies with content-free diagnostics", () => {
  expect(() => parseHelperJsonObject("{not json", "docker-helper response")).toThrow(
    "docker-helper response is not valid JSON",
  );
  expect(() => parseHelperJsonObject("[1,2]", "docker-helper response")).toThrow(
    "docker-helper response is not a JSON object",
  );
  expect(() => parseHelperJsonObject("null", "docker-helper response")).toThrow(
    "docker-helper response is not a JSON object",
  );
  const parsed = parseHelperJsonObject('{"ok":true}', "docker-helper response");
  expect(parsed).toEqual({ ok: true });
  // a canary fragment of the body never appears in any diagnostic
  try {
    parseHelperJsonObject('{"token-canary":"x"', "docker-helper response");
  } catch (cause) {
    expect((cause as Error).message).not.toContain("token-canary");
  }
});
