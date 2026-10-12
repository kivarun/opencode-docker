/**
 * Fixture fake Docker Helper daemon for the control-plane smoke test.
 *
 * The production orchestrator CLI child talks to the REAL docker-helper
 * binary, which talks to this daemon over a unix socket. The daemon
 * emulates the HTTP API surface the CLI uses: GET /auth, POST /sessions,
 * DELETE /sessions/:id, POST /pull, POST /run (async operation) and
 * GET /operations/:id[/logs]. Worker behavior is keyed by the state id
 * from the orchestrator-owned execution document.
 *
 * usage: bun control_plane_daemon.ts <socket-path>
 */
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";

const socketPath = process.argv[2] ?? "";
if (socketPath === "") {
  console.error("usage: control_plane_daemon.ts SOCKET_PATH");
  process.exit(2);
}

const PLAN_1 = JSON.stringify({
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "Body A" }],
});
const PLAN_2 = JSON.stringify({
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [
    { id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] },
    { id: "stage-2", template: "review", tasks: [{ id: "task-b", depends_on: [] }] },
  ],
  new_tasks: [{ id: "task-b", body: "Body B" }],
});

interface SessionRecord {
  id: string;
  token: string;
  workspace: string;
}
const sessions = new Map<string, SessionRecord>();
const sessionsByToken = new Map<string, SessionRecord>();
const operations = new Map<string, { status: Record<string, unknown> }>();
let counter = 0;

function json(response: unknown, status = 200): Response {
  return new Response(JSON.stringify(response), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function executeWorker(workspace: string, mounts: Array<{ source: string; target: string }>): number {
  const inputs = mounts.find((m) => m.target === "/pipeline/inputs");
  const outputs = mounts.find((m) => m.target === "/pipeline/outputs");
  if (inputs === undefined || outputs === undefined) return 1;
  let stateId = "";
  try {
    const doc = readFileSync(join(workspace, inputs.source, ".orchestrator", "execution.md"), "utf8");
    const match = doc.match(/^state_id: "(.+)"$/m);
    if (match) stateId = match[1] as string;
  } catch {
    return 1;
  }
  if (stateId === "dev_entry") {
    Bun.sleepSync(2500);
  }
  const outputsRoot = join(workspace, outputs.source);
  mkdirSync(outputsRoot, { recursive: true });
  if (stateId === "architect") {
    writeFileSync(join(outputsRoot, "plan"), PLAN_1, { mode: 0o600 });
  } else if (stateId === "planner2") {
    writeFileSync(join(outputsRoot, "plan2"), PLAN_2, { mode: 0o600 });
  } else if (stateId === "review_entry") {
    writeFileSync(join(outputsRoot, "report"), "SMOKE-REPORT\n", { mode: 0o600 });
  }
  return 0;
}

Bun.serve({
  unix: socketPath,
  async fetch(request) {
    const url = new URL(request.url);
    const bodyText = await request.text();
    const auth = request.headers.get("Authorization") ?? "";
    const bearer = auth.startsWith("Bearer ") ? auth.slice(7) : "";
    if (url.pathname === "/auth") {
      return json({ authority: "launcher", principal: "smoke", launcher_id: "dhl_smoke" });
    }
    if (url.pathname === "/sessions" && request.method === "POST") {
      const body = JSON.parse(bodyText) as { workspace?: string };
      counter += 1;
      const record: SessionRecord = { id: `sess-${counter}`, token: `tok-${counter}`, workspace: body.workspace ?? "" };
      sessions.set(record.id, record);
      sessionsByToken.set(record.token, record);
      appendFileSync(`${socketPath}.events`, `${JSON.stringify({ event: "create", id: record.id })}\n`);
      return json({ ok: true, token: record.token, session: { id: record.id, launcher_id: "dhl_smoke" } });
    }
    const deleteMatch = url.pathname.match(/^\/sessions\/(.+)$/);
    if (deleteMatch !== null && request.method === "DELETE") {
      const id = deleteMatch[1] ?? "";
      appendFileSync(`${socketPath}.events`, `${JSON.stringify({ event: "delete", id })}\n`);
      if (!sessions.has(id)) return json({ error: "unknown session" }, 404);
      return json({ ok: true, deleted: true, id });
    }
    if (url.pathname.startsWith("/operations/")) {
      const parts = url.pathname.split("/");
      const operation = operations.get(parts[2] ?? "");
      if (operation === undefined) return json({ error: "unknown operation" }, 404);
      return json(operation.status);
    }
    if (url.pathname === "/pull" && request.method === "POST") {
      return json({ ok: true });
    }
    if (url.pathname === "/run" && request.method === "POST") {
      const body = JSON.parse(bodyText) as { mounts?: Array<{ source: string; target: string }> };
      const record = sessionsByToken.get(bearer);
      if (record === undefined) return json({ error: "unknown session token" }, 403);
      counter += 1;
      const operationId = `op-${counter}`;
      const exitCode = executeWorker(record.workspace, body.mounts ?? []);
      operations.set(operationId, {
        status: { status: "succeeded", exit_code: exitCode, result_code: exitCode === 0 ? "0" : "1" },
      });
      return json({ ok: true, operation_id: operationId });
    }
    return json({ error: `no route ${request.method} ${url.pathname}` }, 404);
  },
});

console.log(`DAEMON-READY ${socketPath}`);
void dirname;
