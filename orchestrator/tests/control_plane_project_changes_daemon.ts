/**
 * Fixture fake Docker Helper daemon for the browser UI v6 project-changes
 * suite. It behaves like `control_plane_daemon.ts` except that the stage
 * workers also write into the shared run project workspace, so a
 * terminal run carries real run-workspace changes relative to the
 * durable project baseline:
 *
 * - `dev_entry` always adds `dev-notes.txt` and rewrites `README.md`;
 * - with the task body `CHANGES-MANY` it additionally writes 150 small
 *   page files (a >100-change run for the pagination battery);
 * - with the task body `CHANGES-FAIL` it writes the two files and then
 *   exits nonzero (a failed terminal run with partial changes);
 * - `review_entry` adds `report-copy.txt`.
 *
 * usage: bun control_plane_project_changes_daemon.ts <socket-path> [dev-entry-delay-ms]
 */
import { appendFileSync, chmodSync, existsSync, mkdirSync, openSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const socketPath = process.argv[2] ?? "";
if (socketPath === "") {
  console.error("usage: control_plane_project_changes_daemon.ts SOCKET_PATH [DEV_ENTRY_DELAY_MS]");
  process.exit(2);
}
const devEntryDelayMs = Number(process.argv[3] ?? "") || 300;

const PLAN_1 = JSON.stringify({
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [{ id: "task-a", body: "Body A" }],
});
const PLAN_1_POINTER = JSON.stringify({
  schema_version: 1,
  kind: "run_plan_proposal",
  stages: [{ id: "stage-1", template: "development", tasks: [{ id: "task-a", depends_on: [] }] }],
  new_tasks: [],
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
interface OperationRecord {
  id: string;
  state: "running" | "succeeded" | "failed" | "cancelled";
  exitCode: number;
  log: string;
}
const operations = new Map<string, OperationRecord>();
const architectRunsByWorkspace = new Map<string, number>();
let counter = 0;

function json(response: unknown, status = 200): Response {
  return new Response(JSON.stringify(response), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function runMode(projectRoot: string): string {
  try {
    return readFileSync(join(projectRoot, "mode.txt"), "utf8").trim();
  } catch {
    return "";
  }
}

async function executeWorker(operation: OperationRecord, workspace: string, mounts: Array<{ source: string; target: string }>): Promise<void> {
  operation.log = `WORKER-START ${operation.id}\n`;
  const inputs = mounts.find((m) => m.target === "/pipeline/inputs");
  const outputs = mounts.find((m) => m.target === "/pipeline/outputs");
  const project = mounts.find((m) => m.target === "/workspace");
  if (inputs === undefined || outputs === undefined || project === undefined) {
    operation.exitCode = 1;
    operation.state = "failed";
    operation.log += "WORKER-FAILED missing mounts\n";
    return;
  }
  let stateId = "";
  try {
    const doc = readFileSync(join(workspace, inputs.source, ".orchestrator", "execution.md"), "utf8");
    const match = doc.match(/^state_id: "(.+)"$/m);
    if (match) stateId = match[1] as string;
  } catch {
    operation.exitCode = 1;
    operation.state = "failed";
    operation.log += "WORKER-FAILED unreadable execution document\n";
    return;
  }
  operation.log += `WORKER-RUNNING state=${stateId}\n`;
  const projectRoot = join(workspace, project.source);
  if (stateId === "dev_entry") {
    await Bun.sleep(devEntryDelayMs);
    const mode = runMode(projectRoot);
    mkdirSync(projectRoot, { recursive: true });
    writeFileSync(join(projectRoot, "dev-notes.txt"), "DEV-NOTES\n", { mode: 0o644 });
    writeFileSync(join(projectRoot, "README.md"), "README-CHANGED\n", { mode: 0o644 });
    if (mode === "MANY") {
      for (let index = 1; index <= 150; index++) {
        writeFileSync(join(projectRoot, `page-${String(index).padStart(3, "0")}.txt`), `PAGE ${index}\n`, { mode: 0o644 });
      }
    }
    if (mode === "FAIL") {
      operation.exitCode = 1;
      operation.state = "failed";
      operation.log += "WORKER-FAILED injected failure\n";
      return;
    }
    if (mode === "CONTENT") {
      // the browser UI v7 content-review scenario: one modified text
      // file, one deleted file, one binary file, one retargeted
      // symlink, one directory mode change and one large file
      writeFileSync(join(projectRoot, "text.txt"), "TEXT-CHANGED\n", { mode: 0o644 });
      rmSync(join(projectRoot, "old.txt"), { force: true });
      writeFileSync(join(projectRoot, "bin.dat"), Buffer.from([66, 73, 78, 0, 255, 0, 10]), { mode: 0o644 });
      rmSync(join(projectRoot, "link"), { force: true });
      symlinkSync("target2", join(projectRoot, "link"));
      chmodSync(join(projectRoot, "moded.txt"), 0o700);
      mkdirSync(join(projectRoot, "newdir"), { recursive: true, mode: 0o700 });
      writeFileSync(join(projectRoot, "big.txt"), `${"M".repeat(99)}\n`.repeat(1024), { mode: 0o644 });
      writeFileSync(join(projectRoot, "blob.bin"), Buffer.from([1, 2, 0, 255, 3, 0, 10]), { mode: 0o644 });
    }
  } else if (stateId === "review_entry") {
    mkdirSync(projectRoot, { recursive: true });
    writeFileSync(join(projectRoot, "report-copy.txt"), "REPORT-COPY\n", { mode: 0o644 });
  }
  const outputsRoot = join(workspace, outputs.source);
  mkdirSync(outputsRoot, { recursive: true });
  if (stateId === "architect") {
    const architectRuns = (architectRunsByWorkspace.get(workspace) ?? 0) + 1;
    architectRunsByWorkspace.set(workspace, architectRuns);
    writeFileSync(join(outputsRoot, "plan"), architectRuns === 1 ? PLAN_1 : PLAN_1_POINTER, { mode: 0o600 });
  } else if (stateId === "planner2") {
    writeFileSync(join(outputsRoot, "plan2"), PLAN_2, { mode: 0o600 });
  } else if (stateId === "review_entry") {
    writeFileSync(join(outputsRoot, "report"), "SMOKE-REPORT\n", { mode: 0o600 });
  }
  operation.log += `WORKER-DONE state=${stateId}\n`;
  operation.exitCode = 0;
  operation.state = "succeeded";
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
      appendEvent("create", record.id);
      return json({ ok: true, token: record.token, session: { id: record.id, launcher_id: "dhl_smoke" } });
    }
    const deleteMatch = url.pathname.match(/^\/sessions\/(.+)$/);
    if (deleteMatch !== null && request.method === "DELETE") {
      const id = deleteMatch[1] ?? "";
      appendEvent("delete", id);
      if (!sessions.has(id)) return json({ error: "unknown session" }, 404);
      return json({ ok: true, deleted: true, id });
    }
    if (url.pathname.startsWith("/operations/")) {
      const parts = url.pathname.split("/");
      const operation = operations.get(parts[2] ?? "");
      if (operation === undefined) return json({ error: "unknown operation" }, 404);
      if (parts[3] === "logs") {
        const offset = Number(url.searchParams.get("offset") ?? "0");
        if (!Number.isSafeInteger(offset) || offset < 0) return json({ error: "invalid offset" }, 400);
        const total = Buffer.byteLength(operation.log, "utf8");
        if (operation.state === "running") {
          if (offset >= total) {
            return json({ logs: "", offset, next_offset: offset });
          }
          const chunk = operation.log.slice(offset, Math.min(total, offset + 4096));
          return json({ logs: chunk, offset, next_offset: offset + Buffer.byteLength(chunk, "utf8") });
        }
        return json({ logs: operation.log.slice(offset), offset, next_offset: null });
      }
      if (parts[3] === "cancel" && request.method === "POST") {
        if (operation.state === "running") {
          operation.state = "cancelled";
        }
        return json({ ok: true, operation_id: operation.id, status: operation.state });
      }
      const status: Record<string, unknown> = { status: operation.state };
      if (operation.state === "succeeded") {
        status["exit_code"] = null;
        status["result_code"] = "succeeded";
      }
      if (operation.state === "failed") {
        status["exit_code"] = operation.exitCode === 0 ? null : operation.exitCode;
        status["result_code"] = operation.exitCode === 0 ? "docker_run_failed" : "container_exit_nonzero";
      }
      return json(status);
    }
    if (url.pathname === "/pull" && request.method === "POST") {
      return json({ ok: true });
    }
    if (url.pathname === "/run" && request.method === "POST") {
      const body = JSON.parse(bodyText) as { mounts?: Array<{ source: string; target: string }> };
      const record = sessionsByToken.get(bearer);
      if (record === undefined) return json({ error: "unknown session token" }, 403);
      counter += 1;
      const operation: OperationRecord = {
        id: `op-${counter}`,
        state: "running",
        exitCode: 0,
        log: "",
      };
      operations.set(operation.id, operation);
      void executeWorker(operation, record.workspace, body.mounts ?? []);
      return json({ ok: true, operation_id: operation.id });
    }
    return json({ error: `no route ${request.method} ${url.pathname}` }, 404);
  },
});

function appendEvent(event: string, id: string): void {
  appendFileSync(`${socketPath}.events`, `${JSON.stringify({ event, id })}\n`);
}

console.log(`DAEMON-READY ${socketPath}`);
void existsSync;
void openSync;
