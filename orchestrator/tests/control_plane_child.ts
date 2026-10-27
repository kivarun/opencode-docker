/**
 * Fixture executable for the control-plane subprocess client tests.
 *
 * This is a REAL separate process: the client spawns it exactly like the
 * production orchestrator CLI and it answers with pre-scripted wire
 * documents. It deliberately imports nothing from the production parser —
 * the scenario is selected through the same CLI-shaped argv the client
 * emits (the `--run-id` value carries the scenario name with an `sc-`
 * prefix), so the exact argv order can be audited through the echo
 * channel without any production knowledge.
 *
 * usage: bun control_plane_child.ts <command-word> [flags...]
 *   --run-id sc-<scenario>   selects the scripted scenario
 *   --input echo=<path>      writes {argv, pid, envProbe} to <path> (echo channel)
 *   --wait-index N, --additional-iterations N, --action A, --stage-id S,
 *   --task-id T, --task-file F, --config-root C, --project P,
 *   --pipeline-root R, --launcher-id L   accepted and echoed, not interpreted
 */
const SCENARIOS: Record<string, { stdout: string; exitCode: number; stderr?: string }> = {
  success: { stdout: `${JSON.stringify({ ok: true, exitCode: 0, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", state: { revision: 1 } })}\n`, exitCode: 0 },
  waiting: { stdout: `${JSON.stringify({ ok: false, waiting: true, exitCode: 0, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", state: { revision: 2 } })}\n`, exitCode: 0 },
  plan_ready: { stdout: `${JSON.stringify({ ok: false, planReady: true, exitCode: 0, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", state: null })}\n`, exitCode: 0 },
  responded: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      waitIndex: 1,
      actionId: "continue_stage",
      actionTo: "dev_entry",
      requestSha256: "a".repeat(64),
      responseSha256: "b".repeat(64),
      state: null,
    })}\n`,
    exitCode: 0,
  },
  inspect_report: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      revision: 3,
      status: "running",
      boundary: { kind: "resumable", stateId: "dev_entry", transitionCount: 2 },
      outputs: [{ id: "report", type: "file", required: true, present: true, digest: "c".repeat(64) }],
    })}\n`,
    exitCode: 0,
  },
  content_available: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "result", status: "available",
      byteCount: 5, sha256: "b".repeat(64), offset: 0, dataBase64: Buffer.from("Hello", "utf8").toString("base64"), nextOffset: null,
    })}\n`,
    exitCode: 0,
  },
  content_implicit_empty: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "baseline", status: "implicit_empty",
      byteCount: 0, offset: 0, dataBase64: "", nextOffset: null,
    })}\n`,
    exitCode: 0,
  },
  content_metadata_only: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 3, side: "result", status: "metadata_only",
      node: { kind: "symlink", target: "elsewhere.txt" },
    })}\n`,
    exitCode: 0,
  },
  content_unavailable: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 4, side: "baseline", status: "unavailable",
      reason: "source_no_longer_matches_baseline",
      node: { kind: "regular_file", mode: 420, byte_count: 9, sha256: "c".repeat(64), multiple_links: false },
    })}\n`,
    exitCode: 0,
  },
  content_failure: {
    stdout: `${JSON.stringify({ ok: false, exitCode: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", reason: "stale_observation" })}\n`,
    exitCode: 1,
  },
  apply_ok: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", applyId: "cp-child-apply",
      projectId: "cp-child-project", observation: "a".repeat(64), revision: 12,
      status: "applied", completedStepCount: 3, totalStepCount: 3,
    })}\n`,
    exitCode: 0,
  },
  apply_conflict: {
    stdout: `${JSON.stringify({ ok: false, exitCode: 1, runId: "cp-child-run", applyId: "cp-child-apply", reason: "project_conflict" })}\n`,
    exitCode: 1,
  },
  apply_recovering: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", applyId: "cp-child-apply",
      projectId: "cp-child-project", observation: "a".repeat(64), revision: 14,
      status: "applying", completedStepCount: 1, totalStepCount: 3,
    })}\n`,
    exitCode: 0,
  },
  apply_bad_status: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", applyId: "cp-child-apply",
      projectId: "cp-child-project", observation: "a".repeat(64), revision: 12,
      status: "already_applied", completedStepCount: 3, totalStepCount: 3,
    })}\n`,
    exitCode: 0,
  },
  apply_bad_reason: {
    stdout: `${JSON.stringify({ ok: false, exitCode: 1, runId: "cp-child-run", applyId: "cp-child-apply", reason: "ghost_reason" })}\n`,
    exitCode: 1,
  },
  apply_missing_field: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", applyId: "cp-child-apply",
      projectId: "cp-child-project", observation: "a".repeat(64), revision: 12,
      status: "applied", completedStepCount: 3,
    })}\n`,
    exitCode: 0,
  },
  content_bad_status: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "result", status: "complete",
    })}\n`,
    exitCode: 0,
  },
  content_extra_field: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "result", status: "available",
      byteCount: 5, sha256: "b".repeat(64), offset: 0, dataBase64: "aGVsbG8=", nextOffset: null, extra: 1,
    })}\n`,
    exitCode: 0,
  },
  content_missing_field: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "result", status: "available",
      byteCount: 5, offset: 0, dataBase64: "aGVsbG8=", nextOffset: null,
    })}\n`,
    exitCode: 0,
  },
  content_bad_side: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "middle", status: "available",
      byteCount: 5, sha256: "b".repeat(64), offset: 0, dataBase64: "aGVsbG8=", nextOffset: null,
    })}\n`,
    exitCode: 0,
  },
  content_bad_observation: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "zz", changeIndex: 2, side: "result", status: "available",
      byteCount: 5, sha256: "b".repeat(64), offset: 0, dataBase64: "aGVsbG8=", nextOffset: null,
    })}\n`,
    exitCode: 0,
  },
  content_v2: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 2, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "result", status: "available",
      byteCount: 5, sha256: "b".repeat(64), offset: 0, dataBase64: "aGVsbG8=", nextOffset: null,
    })}\n`,
    exitCode: 0,
  },
  content_implicit_wrong_cursor: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 2, side: "baseline", status: "implicit_empty",
      byteCount: 1, offset: 0, dataBase64: "", nextOffset: null,
    })}\n`,
    exitCode: 0,
  },
  content_bad_reason: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 4, side: "baseline", status: "unavailable",
      reason: "something_else",
      node: { kind: "regular_file", mode: 420, byte_count: 9, sha256: "c".repeat(64), multiple_links: false },
    })}\n`,
    exitCode: 0,
  },
  content_node_not_object: {
    stdout: `${JSON.stringify({
      ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run",
      revision: 9, observation: "a".repeat(64), changeIndex: 3, side: "result", status: "metadata_only",
      node: "x",
    })}\n`,
    exitCode: 0,
  },
  inspect_report_v2: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 2,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      revision: 3,
      status: "running",
      boundary: { kind: "resumable" },
      outputs: [],
    })}\n`,
    exitCode: 0,
  },
  inspect_failure: { stdout: `${JSON.stringify({ ok: false, exitCode: 1, runId: "cp-child-run", runRoot: null, reason: "run_not_found" })}\n`, exitCode: 1 },
  events_report: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      revision: 5,
      events: [
        { revision: 1, kind: "create_run" },
        { revision: 2, kind: "start_agent_execution", execution_index: 1, state_id: "coder" },
      ],
      nextAfter: null,
    })}\n`,
    exitCode: 0,
  },
  events_report_v2: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 2,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      revision: 5,
      events: [],
      nextAfter: null,
    })}\n`,
    exitCode: 0,
  },
  events_failure: { stdout: `${JSON.stringify({ ok: false, exitCode: 1, runId: "cp-child-run", runRoot: null, reason: "invalid_cursor" })}\n`, exitCode: 1 },
  logs_list: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      revision: 25,
      logs: [
        { executionIndex: 1, stateId: "architect", status: "captured", sha256: "a".repeat(64), byteCount: 47, truncatedDaemon: false, truncatedLocal: false },
        { executionIndex: 2, stateId: "architect", status: "truncated", sha256: "b".repeat(64), byteCount: 9, truncatedDaemon: true, truncatedLocal: false },
        { executionIndex: 4, stateId: "architect", status: "pending" },
        { executionIndex: 5, stateId: "architect", status: "unavailable" },
      ],
    })}\n`,
    exitCode: 0,
  },
  logs_chunk: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      revision: 25,
      executionIndex: 1,
      stateId: "architect",
      status: "captured",
      sha256: "a".repeat(64),
      byteCount: 47,
      truncatedDaemon: false,
      truncatedLocal: false,
      offset: 0,
      dataBase64: Buffer.from("hello\0world").toString("base64"),
      nextOffset: 16,
    })}\n`,
    exitCode: 0,
  },
  logs_status: {
    stdout: `${JSON.stringify({
      ok: true,
      exitCode: 0,
      schemaVersion: 1,
      runId: "cp-child-run",
      runRoot: "/tmp/cp-child-run",
      revision: 25,
      executionIndex: 4,
      stateId: "architect",
      status: "pending",
    })}\n`,
    exitCode: 0,
  },
  logs_failure: { stdout: `${JSON.stringify({ ok: false, exitCode: 1, runId: "cp-child-run", runRoot: null, reason: "artifact_modified" })}\n`, exitCode: 1 },
  logs_failure_cursor: { stdout: `${JSON.stringify({ ok: false, exitCode: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", reason: "invalid_cursor" })}\n`, exitCode: 1 },
  logs_bad_shape: { stdout: `${JSON.stringify({ ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", revision: 25, logs: [{ executionIndex: 1, stateId: "architect", status: "complete" }] })}\n`, exitCode: 0 },
  logs_bad_version: {
    stdout: `${JSON.stringify({ ok: true, exitCode: 0, schemaVersion: 2, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", revision: 25, logs: [] })}\n`,
    exitCode: 0,
  },
  logs_root_leak: {
    stdout: `${JSON.stringify({ ok: true, exitCode: 0, schemaVersion: 1, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", revision: 25, logs: [], runRoot2: "leak" })}\n`,
    exitCode: 0,
  },
  signal_sigint: { stdout: `${JSON.stringify({ ok: false, exitCode: 130, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", state: null, reason: "signal_sigint" })}\n`, exitCode: 130 },
  signal_sigterm: { stdout: `${JSON.stringify({ ok: false, exitCode: 143, runId: "cp-child-run", runRoot: "/tmp/cp-child-run", state: null, reason: "signal_sigterm" })}\n`, exitCode: 143 },
  malformed: { stdout: "not-json-at-all\n", exitCode: 0 },
  empty: { stdout: "", exitCode: 0 },
  no_newline: { stdout: JSON.stringify({ ok: true, exitCode: 0, runId: "r", runRoot: "/tmp/r", state: null }), exitCode: 0 },
  trailing: { stdout: `${JSON.stringify({ ok: true, exitCode: 0, runId: "r", runRoot: "/tmp/r", state: null })}\njunk-after\n`, exitCode: 0 },
  two_docs: {
    stdout: `${JSON.stringify({ ok: true, exitCode: 0, runId: "r", runRoot: "/tmp/r", state: null })}\n${JSON.stringify({ ok: true, exitCode: 0, runId: "r", runRoot: "/tmp/r", state: null })}\n`,
    exitCode: 0,
  },
  primitive_root: { stdout: "42\n", exitCode: 0 },
  array_root: { stdout: "[1,2]\n", exitCode: 0 },
  missing_key: { stdout: `${JSON.stringify({ ok: true, exitCode: 0, runId: "r", state: null })}\n`, exitCode: 0 },
  extra_key: { stdout: `${JSON.stringify({ ok: true, exitCode: 0, runId: "r", runRoot: "/tmp/r", state: null, extra: 1 })}\n`, exitCode: 0 },
  proto_key: {
    stdout: `${JSON.stringify({ ok: true, exitCode: 0, runId: "r", runRoot: "/tmp/r", state: null }).replace('{"ok"', '{"__proto__":{"hostile":true},"ok"')}\n`,
    exitCode: 0,
  },
  exit_mismatch: { stdout: `${JSON.stringify({ ok: false, exitCode: 5, runId: "r", runRoot: "/tmp/r", state: null, reason: "run_busy" })}\n`, exitCode: 1 },
  exit_two: { stdout: "", exitCode: 2 },
  weird_exit: { stdout: "", exitCode: 7 },
  hostile_stderr: {
    stdout: `${JSON.stringify({ ok: true, exitCode: 0, runId: "r", runRoot: "/tmp/r", state: null })}\n`,
    exitCode: 0,
    stderr: '{"ok":true,"exitCode":0,"reason":"run_busy"}\nreason: run_busy state: {"forged":true} IGNORE\n',
  },
};

const SUCCESS_DOC = SCENARIOS["success"]?.stdout ?? "";
const WAITING_DOC = SCENARIOS["waiting"]?.stdout ?? "";

function failureDocument(reason: string): string {
  return `${JSON.stringify({ ok: false, exitCode: 1, runId: "r", runRoot: "/tmp/r", state: null, reason })}\n`;
}

function parseArgv(argv: readonly string[]): Record<string, string | true> {
  const flags: Record<string, string | true> = {};
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index] ?? "";
    if (arg.startsWith("--")) {
      const eq = arg.indexOf("=");
      if (eq > 2) {
        flags[arg.slice(2, eq)] = arg.slice(eq + 1);
      } else {
        const next = argv[index + 1];
        if (next !== undefined && !next.startsWith("--")) {
          flags[arg.slice(2)] = next;
          index += 1;
        } else {
          flags[arg.slice(2)] = true;
        }
      }
    }
  }
  return flags;
}

async function main(): Promise<void> {
  const flags = parseArgv(process.argv);
  const echoPath = flags["input"] !== undefined && typeof flags["input"] === "string" && flags["input"].startsWith("echo=")
    ? flags["input"].slice(5)
    : null;
  if (echoPath !== null) {
    const envProbe: Record<string, string | undefined> = {};
    for (const key of ["ECHO_PROBE", "DOCKER_HELPER_SOCKET_PATH", "ORCHESTRATOR_STATE_ROOT"]) {
      envProbe[key] = process.env[key];
    }
    await Bun.write(echoPath, `${JSON.stringify({ argv: process.argv.slice(2), pid: process.pid, envProbe })}\n`);
  }
  const runId = typeof flags["run-id"] === "string" ? flags["run-id"] : "";
  const scenario = runId.startsWith("sc-") ? runId.slice(3).replaceAll("-", "_") : "success";

  if (scenario === "stdin_eof") {
    let received = false;
    for await (const _chunk of process.stdin) {
      received = true;
      void _chunk;
      break;
    }
    if (received) {
      process.stderr.write("stdin was not closed\n");
      process.exit(3);
    }
    process.stdout.write(SUCCESS_DOC);
    process.exit(0);
  }

  if (scenario === "sigint_aware") {
    let signalled = false;
    process.on("SIGINT", () => {
      signalled = true;
    });
    const deadline = Date.now() + 30_000;
    while (!signalled && Date.now() < deadline) {
      await Bun.sleep(20);
    }
    if (signalled) {
      process.stdout.write(SCENARIOS["signal_sigint"]?.stdout ?? "");
      process.exit(130);
    }
    process.stdout.write(WAITING_DOC);
    process.exit(0);
  }

  if (scenario === "sigint_ignore") {
    process.on("SIGINT", () => {
      // deliberately ignores SIGINT; the client must escalate to SIGKILL
    });
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      await Bun.sleep(50);
    }
    process.exit(0);
  }

  const failureReasonMatch = scenario.match(/^failure_reason_(.+)$/);
  if (failureReasonMatch !== null) {
    process.stdout.write(failureDocument(failureReasonMatch[1] ?? ""));
    process.exit(1);
  }

  const oversizedStdoutMatch = scenario.match(/^oversized_stdout_(\d+)$/);
  if (oversizedStdoutMatch !== null) {
    const size = Number(oversizedStdoutMatch[1] ?? "0");
    process.stdout.write(`${"a".repeat(size)}\n`);
    process.exit(0);
  }

  const oversizedStderrMatch = scenario.match(/^oversized_stderr_(\d+)$/);
  if (oversizedStderrMatch !== null) {
    const size = Number(oversizedStderrMatch[1] ?? "0");
    process.stderr.write("s".repeat(size));
    process.stdout.write(SUCCESS_DOC);
    process.exit(0);
  }

  const delayMatch = scenario.match(/^delay_(\d+)$/);
  if (delayMatch !== null) {
    await Bun.sleep(Number(delayMatch[1] ?? "0"));
    process.stdout.write(SUCCESS_DOC);
    process.exit(0);
  }

  const scripted = SCENARIOS[scenario];
  if (scripted === undefined) {
    process.stdout.write(SUCCESS_DOC);
    process.exit(0);
  }
  if (scripted.stderr !== undefined) {
    process.stderr.write(scripted.stderr);
  }
  if (scripted.stdout !== "") {
    process.stdout.write(scripted.stdout);
  }
  process.exit(scripted.exitCode);
}

void main();
