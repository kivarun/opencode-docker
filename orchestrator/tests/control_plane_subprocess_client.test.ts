import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  OrchestratorSubprocessClient,
  type OrchestratorCallResult,
  type OrchestratorRunProjection,
  type OrchestratorSubprocessClientConfig,
} from "../src/control_plane/subprocess_client.ts";
import { OrchestratorSubprocessClientError, type OrchestratorTransportReason } from "../src/control_plane/routing.ts";

const CHILD = join(import.meta.dir, "control_plane_child.ts");
const WORK_DIR = tmpdir();

function clientConfig(overrides: Partial<OrchestratorSubprocessClientConfig> = {}): OrchestratorSubprocessClientConfig {
  return {
    bunExecutable: process.execPath,
    orchestratorEntrypoint: CHILD,
    workingDirectory: WORK_DIR,
    configRoot: "/opt/orchestrator/config",
    baseEnv: { PATH: "/usr/bin:/bin", ECHO_PROBE: "before" },
    stdoutLimitBytes: 1_000_000,
    stderrLimitBytes: 1_000_000,
    sigintGraceMs: 2000,
    ...overrides,
  };
}

async function waitEchoReady(echoPath: string): Promise<void> {
  for (let i = 0; i < 100 && !existsSync(echoPath); i += 1) {
    await Bun.sleep(50);
  }
  if (!existsSync(echoPath)) throw new Error(`echo channel never appeared: ${echoPath}`);
}

function makeClient(overrides: Partial<OrchestratorSubprocessClientConfig> = {}): OrchestratorSubprocessClient {
  return new OrchestratorSubprocessClient(clientConfig(overrides));
}

function inspectScenario(scenario: string): { kind: "inspect"; runId: string } {
  return { kind: "inspect", runId: `sc-${scenario}` };
}

function expectFailure(result: OrchestratorCallResult, reason: OrchestratorTransportReason, expectedRunId?: string): void {
  expect(result.ok).toBe(false);
  if (result.ok) throw new Error("unreachable");
  expect(result.failure.reason).toBe(reason);
  if (expectedRunId !== undefined) {
    expect(result.failure.expectedRunId).toBe(expectedRunId);
  }
}

function expectRunProjection(result: OrchestratorCallResult, kind: OrchestratorRunProjection["kind"]): OrchestratorRunProjection {
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("unreachable");
  const projection = result.projection as OrchestratorRunProjection;
  expect(projection.kind).toBe(kind);
  return projection;
}

describe("control plane subprocess client: config", () => {
  test("rejects a non-absolute bun executable before any spawn", () => {
    expect(() => makeClient({ bunExecutable: "bun" })).toThrow(OrchestratorSubprocessClientError);
    try {
      makeClient({ bunExecutable: "bun" });
    } catch (error) {
      expect((error as OrchestratorSubprocessClientError).reason).toBe("invalid_client_config");
    }
  });

  test("rejects relative or empty paths in required path fields", () => {
    expect(() => makeClient({ orchestratorEntrypoint: "src/main.ts" })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ workingDirectory: "." })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ configRoot: "relative" })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ pipelineRoot: "relative" })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ configRoot: "" })).toThrow(OrchestratorSubprocessClientError);
  });

  test("rejects unsafe limits and grace values", () => {
    expect(() => makeClient({ stdoutLimitBytes: 0 })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ stderrLimitBytes: -1 })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ stdoutLimitBytes: 1.5 })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ stdoutLimitBytes: Number.MAX_SAFE_INTEGER + 1 })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ sigintGraceMs: -1 })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ sigintGraceMs: 0 })).not.toThrow();
  });

  test("rejects an unsafe base environment", () => {
    expect(() => makeClient({ baseEnv: ["PATH"] as unknown as Record<string, string> })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ baseEnv: { "": "x" } })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ baseEnv: { A: "a\0b" } })).toThrow(OrchestratorSubprocessClientError);
    expect(() => makeClient({ baseEnv: { A: 1 as unknown as string } })).toThrow(OrchestratorSubprocessClientError);
  });

  test("rejects unknown config fields", () => {
    expect(() => makeClient({ shell: true } as unknown as Partial<OrchestratorSubprocessClientConfig>)).toThrow(
      OrchestratorSubprocessClientError,
    );
  });

  test("the config is captured once: later caller mutations cannot reach a child", async () => {
    const echoPath = join(WORK_DIR, `cp-echo-${Math.random().toString(36).slice(2, 8)}.json`);
    const callerBaseEnv = { PATH: "/usr/bin:/bin", ECHO_PROBE: "before" };
    const client = new OrchestratorSubprocessClient(
      clientConfig({ baseEnv: callerBaseEnv as Record<string, string> }),
    );
    callerBaseEnv.ECHO_PROBE = "after";
    const result = await client.run({
      kind: "run",
      runId: "sc-echo-run",
      projectPath: "/srv/projects/demo",
      inputs: [{ id: "echo", path: echoPath }],
    });
    expectRunProjection(result, "success");
    const echo = JSON.parse(readFileSync(echoPath, "utf8")) as { envProbe: Record<string, string | undefined> };
    expect(echo.envProbe.ECHO_PROBE).toBe("before");
    rmSync(echoPath, { force: true });
  });
});

describe("control plane subprocess client: wire parser", () => {
  test("projects a success outcome without the raw state", async () => {
    const client = makeClient();
    const result = await client.run({ kind: "resume", runId: "sc-success" });
    const projection = expectRunProjection(result, "success");
    expect(projection.exitCode).toBe(0);
    expect(projection.runId).toBe("cp-child-run");
    expect(projection.runRoot).toBe("/tmp/cp-child-run");
    expect(JSON.stringify(projection)).not.toContain('"state"');
    expect(JSON.stringify(projection)).not.toContain('"revision"');
  });

  test("projects a waiting outcome", async () => {
    const client = makeClient();
    const projection = expectRunProjection(await client.run({ kind: "resume", runId: "sc-waiting" }), "waiting");
    expect(projection.exitCode).toBe(0);
    expect(JSON.stringify(projection)).not.toContain('"state"');
  });

  test("projects a planReady outcome", async () => {
    const client = makeClient();
    const projection = expectRunProjection(await client.run({ kind: "run", runId: "sc-plan-ready", projectPath: "/p", inputs: [] }), "planReady");
    expect(projection.exitCode).toBe(0);
  });

  test("projects a responded outcome with the action binding", async () => {
    const client = makeClient();
    const projection = expectRunProjection(
      await client.run({ kind: "respond", runId: "sc-responded", waitIndex: 1, actionId: "continue_stage" }),
      "responded",
    );
    if (projection.kind !== "responded") throw new Error("unreachable");
    expect(projection.waitIndex).toBe(1);
    expect(projection.actionId).toBe("continue_stage");
    expect(projection.actionTo).toBe("dev_entry");
    expect(projection.requestSha256).toBe("a".repeat(64));
    expect(projection.responseSha256).toBe("b".repeat(64));
    expect(JSON.stringify(projection)).not.toContain('"state"');
  });

  test("a waiting document on a respond command is an unexpected shape", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "respond", runId: "sc-waiting", waitIndex: 1, actionId: "continue_stage" }), "unexpected_output_shape");
  });

  test("projects the inspect report with an opaque frozen boundary", async () => {
    const client = makeClient();
    const result = await client.run(inspectScenario("inspect-report"));
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    const projection = result.projection;
    if (projection.kind !== "inspect") throw new Error("unreachable");
    expect(projection.schemaVersion).toBe(1);
    expect(projection.status).toBe("running");
    expect(projection.revision).toBe(3);
    expect(projection.boundary.kind).toBe("resumable");
    expect(Object.isFrozen(projection)).toBe(true);
    expect(Object.isFrozen(projection.boundary)).toBe(true);
    expect(Object.isFrozen(projection.outputs)).toBe(true);
  });

  test("an inspect schemaVersion other than 1 is unsupported_contract", async () => {
    const client = makeClient();
    expectFailure(await client.run(inspectScenario("inspect-report-v2")), "unsupported_contract");
  });

  test("projects an inspect failure outcome", async () => {
    const client = makeClient();
    const result = await client.run(inspectScenario("inspect-failure"));
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error("unreachable");
    expect(result.projection.kind).toBe("inspectFailure");
    if (result.projection.kind !== "inspectFailure") throw new Error("unreachable");
    expect(result.projection.exitCode).toBe(1);
    expect(result.projection.reason).toBe("run_not_found");
    expect(result.projection.runRoot).toBeNull();
  });

  test("exit-1 failure outcomes project the public reason vocabulary", async () => {
    const client = makeClient();
    const projection = expectRunProjection(
      await client.run({ kind: "resume", runId: "sc-failure-reason-run_busy" }),
      "failure",
    );
    if (projection.kind !== "failure") throw new Error("unreachable");
    expect(projection.exitCode).toBe(1);
    expect(projection.reason).toBe("run_busy");
  });

  test("an unknown failure reason is an unexpected shape, never accepted", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "resume", runId: "sc-failure-reason-totally_unknown" }), "unexpected_output_shape");
  });

  test("signal outcomes project 130/signal_sigint and 143/signal_sigterm", async () => {
    const client = makeClient();
    const sigint = expectRunProjection(await client.run({ kind: "run", runId: "sc-signal-sigint", projectPath: "/p", inputs: [] }), "failure");
    if (sigint.kind !== "failure") throw new Error("unreachable");
    expect(sigint.exitCode).toBe(130);
    expect(sigint.reason).toBe("signal_sigint");
    const sigterm = expectRunProjection(await client.run({ kind: "run", runId: "sc-signal-sigterm", projectPath: "/p", inputs: [] }), "failure");
    if (sigterm.kind !== "failure") throw new Error("unreachable");
    expect(sigterm.exitCode).toBe(143);
    expect(sigterm.reason).toBe("signal_sigterm");
  });

  test("malformed stdout variants are transport failures", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "resume", runId: "sc-malformed" }), "malformed_output");
    expectFailure(await client.run({ kind: "resume", runId: "sc-empty" }), "malformed_output");
    expectFailure(await client.run({ kind: "resume", runId: "sc-no-newline" }), "malformed_output");
    expectFailure(await client.run({ kind: "resume", runId: "sc-trailing" }), "malformed_output");
    expectFailure(await client.run({ kind: "resume", runId: "sc-two-docs" }), "malformed_output");
  });

  test("non-object JSON roots are unexpected shapes", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "resume", runId: "sc-primitive-root" }), "unexpected_output_shape");
    expectFailure(await client.run({ kind: "resume", runId: "sc-array-root" }), "unexpected_output_shape");
  });

  test("missing, extra and hostile-prototype keys are rejected", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "resume", runId: "sc-missing-key" }), "unexpected_output_shape");
    expectFailure(await client.run({ kind: "resume", runId: "sc-extra-key" }), "unexpected_output_shape");
    expectFailure(await client.run({ kind: "resume", runId: "sc-proto-key" }), "unexpected_output_shape");
  });

  test("a document exit code that disagrees with the process exit code fails", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "resume", runId: "sc-exit-mismatch" }), "exit_code_mismatch");
  });

  test("exit 2 is a command rejection and any other exit code is a terminated child", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "resume", runId: "sc-exit-two" }), "command_rejected");
    expectFailure(await client.run({ kind: "resume", runId: "sc-weird-exit" }), "child_terminated");
  });

  test("stdout overflow is a bounded transport failure and the child is not left hanging", async () => {
    const client = makeClient({ stdoutLimitBytes: 64 });
    const started = performance.now();
    expectFailure(await client.run({ kind: "resume", runId: "sc-oversized-stdout-200" }), "stdout_too_large");
    expect(performance.now() - started).toBeLessThan(5000);
  });

  test("stderr overflow is a bounded transport failure even with valid stdout", async () => {
    const client = makeClient({ stderrLimitBytes: 64 });
    expectFailure(await client.run({ kind: "resume", runId: "sc-oversized-stderr-200" }), "stderr_too_large");
  });

  test("hostile stderr is never classified: a valid stdout still wins", async () => {
    const client = makeClient();
    const projection = expectRunProjection(await client.run({ kind: "resume", runId: "sc-hostile-stderr" }), "success");
    expect(projection.kind).toBe("success");
  });

  test("a delayed child resolves after its work, not at a fixed request timeout", async () => {
    const client = makeClient();
    const started = performance.now();
    expectRunProjection(await client.run({ kind: "resume", runId: "sc-delay-600" }), "success");
    expect(performance.now() - started).toBeGreaterThanOrEqual(550);
  });

  test("the child's stdin is closed: reading it observes immediate EOF", async () => {
    const client = makeClient();
    expectRunProjection(await client.run({ kind: "resume", runId: "sc-stdin-eof" }), "success");
  });

  test("transport failures carry the expected run id", async () => {
    const client = makeClient();
    expectFailure(await client.run({ kind: "resume", runId: "sc-empty" }), "malformed_output", "sc-empty");
    const unspawnable = makeClient({ bunExecutable: "/nonexistent/bun-binary" });
    expectFailure(
      await unspawnable.run({ kind: "run", projectPath: "/p", inputs: [] }),
      "spawn_failed",
      "",
    );
  });

  test("spawn failure is a typed transport failure", async () => {
    const client = makeClient({ bunExecutable: "/nonexistent/bun-binary" });
    expectFailure(await client.run({ kind: "resume", runId: "sc-success" }), "spawn_failed", "sc-success");
  });
});

describe("control plane subprocess client: process lifecycle", () => {
  test("two children of different run ids run in parallel", async () => {
    const client = makeClient();
    const [first, second] = await Promise.all([
      client.run({ kind: "resume", runId: "sc-delay-400" }),
      client.run({ kind: "resume", runId: "sc-delay-400" }),
    ]);
    expectRunProjection(first, "success");
    expectRunProjection(second, "success");
  });

  test(
    "aborting one call signals only its own child",
    async () => {
    const client = makeClient({ sigintGraceMs: 5000 });
    const abortA = new AbortController();
    const echoA = join(WORK_DIR, `cp-ready-a-${Math.random().toString(36).slice(2, 8)}.json`);
    const slowAReal = client.run(
      { kind: "run", runId: "sc-sigint-aware", projectPath: "/p", inputs: [{ id: "echo", path: echoA }] },
      { signal: abortA.signal },
    );
    const slowB = client.run({ kind: "resume", runId: "sc-delay-1000" });
    await waitEchoReady(echoA);
    abortA.abort();
    rmSync(echoA, { force: true });
    const projectionA = expectRunProjection(await slowAReal, "failure");
    if (projectionA.kind !== "failure") throw new Error("unreachable");
    expect(projectionA.exitCode).toBe(130);
    expect(projectionA.reason).toBe("signal_sigint");
    const projectionB = expectRunProjection(await slowB, "success");
    expect(projectionB.kind).toBe("success");
    },
    20000,
  );

  test("a SIGINT-aware child returns its signal outcome within the grace window", async () => {
    const client = makeClient({ sigintGraceMs: 5000 });
    const abort = new AbortController();
    const echoPath = join(WORK_DIR, `cp-ready-b-${Math.random().toString(36).slice(2, 8)}.json`);
    const pending = client.run(
      { kind: "run", runId: "sc-sigint-aware", projectPath: "/p", inputs: [{ id: "echo", path: echoPath }] },
      { signal: abort.signal },
    );
    await waitEchoReady(echoPath);
    abort.abort();
    rmSync(echoPath, { force: true });
    const projection = expectRunProjection(await pending, "failure");
    if (projection.kind !== "failure") throw new Error("unreachable");
    expect(projection.exitCode).toBe(130);
    expect(projection.reason).toBe("signal_sigint");
  });

  test("a SIGINT-ignoring child is force-killed after the grace interval", async () => {
    const client = makeClient({ sigintGraceMs: 300 });
    const abort = new AbortController();
    const echoPath = join(WORK_DIR, `cp-ready-c-${Math.random().toString(36).slice(2, 8)}.json`);
    const pending = client.run(
      { kind: "run", runId: "sc-sigint-ignore", projectPath: "/p", inputs: [{ id: "echo", path: echoPath }] },
      { signal: abort.signal },
    );
    await waitEchoReady(echoPath);
    abort.abort();
    rmSync(echoPath, { force: true });
    expectFailure(await pending, "cancellation_forced", "sc-sigint-ignore");
    expectRunProjection(await client.run({ kind: "resume", runId: "sc-success" }), "success");
  });

  test("an aborted signal before the spawn creates no child", async () => {
    const client = makeClient();
    const echoPath = join(WORK_DIR, `cp-nochild-${Math.random().toString(36).slice(2, 8)}.json`);
    const abort = new AbortController();
    abort.abort();
    expectFailure(
      await client.run(
        { kind: "run", runId: "sc-success", projectPath: "/p", inputs: [{ id: "echo", path: echoPath }] },
        { signal: abort.signal },
      ),
      "cancellation_forced",
      "sc-success",
    );
    expect(existsSync(echoPath)).toBe(false);
  });

  test("a double abort still produces exactly one outcome", async () => {
    const client = makeClient({ sigintGraceMs: 5000 });
    const abort = new AbortController();
    const echoPath = join(WORK_DIR, `cp-ready-d-${Math.random().toString(36).slice(2, 8)}.json`);
    const pending = client.run(
      { kind: "run", runId: "sc-sigint-aware", projectPath: "/p", inputs: [{ id: "echo", path: echoPath }] },
      { signal: abort.signal },
    );
    await waitEchoReady(echoPath);
    abort.abort();
    abort.abort();
    rmSync(echoPath, { force: true });
    const projection = expectRunProjection(await pending, "failure");
    if (projection.kind !== "failure") throw new Error("unreachable");
    expect(projection.exitCode).toBe(130);
  });

  test("an externally killed child is a terminated child and the next invocation works", async () => {
    const client = makeClient();
    const echoPath = join(WORK_DIR, `cp-kill-${Math.random().toString(36).slice(2, 8)}.json`);
    const pending = client.run({
      kind: "run",
      runId: "sc-delay-8000",
      projectPath: "/p",
      inputs: [{ id: "echo", path: echoPath }],
    });
    for (let i = 0; i < 50 && !existsSync(echoPath); i += 1) {
      await Bun.sleep(100);
    }
    const echo = JSON.parse(readFileSync(echoPath, "utf8")) as { pid: number };
    process.kill(echo.pid, "SIGKILL");
    expectFailure(await pending, "child_terminated", "sc-delay-8000");
    expectRunProjection(await client.run({ kind: "resume", runId: "sc-success" }), "success");
    rmSync(echoPath, { force: true });
  });

  test("the child receives the exact routed argv through the real spawn", async () => {
    const client = makeClient();
    const echoPath = join(WORK_DIR, `cp-argv-${Math.random().toString(36).slice(2, 8)}.json`);
    await client.run({
      kind: "run",
      runId: "sc-echo-run",
      pipelineRoot: "/srv/pipelines/custom",
      projectPath: "/srv/projects/demo",
      inputs: [{ id: "task", path: "/srv/inputs/task.md" }, { id: "echo", path: echoPath }],
      launcherId: "dhl_runner",
    });
    const echo = JSON.parse(readFileSync(echoPath, "utf8")) as { argv: string[] };
    expect(echo.argv).toEqual([
      "run",
      "--run-id",
      "sc-echo-run",
      "--pipeline-root",
      "/srv/pipelines/custom",
      "--config-root",
      "/opt/orchestrator/config",
      "--project",
      "/srv/projects/demo",
      "--input",
      "task=/srv/inputs/task.md",
      "--input",
      `echo=${echoPath}`,
      "--launcher-id",
      "dhl_runner",
      "--json",
    ]);
    rmSync(echoPath, { force: true });
  });
});

describe("control plane subprocess client: source restrictions", () => {
  const clientSource = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "subprocess_client.ts"), "utf8");
  const routingSource = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "routing.ts"), "utf8");
  const combined = `${clientSource}\n${routingSource}`;

  test("exactly one Bun.spawn owner and no shell execution anywhere", () => {
    expect(combined.split("Bun.spawn(").length - 1).toBe(1);
    expect(combined).not.toContain("shell:true");
    expect(combined).not.toContain("shell: true");
    expect(combined).not.toContain("sh -c");
    expect(combined).not.toContain("bash -c");
    expect(combined).not.toContain("exec(");
    expect(routingSource).not.toContain("Bun.spawn");
  });

  test("no state reads, no orchestration-internal imports, no helper API client", () => {
    expect(combined).not.toContain("state.json");
    for (const forbidden of [
      'from "../pipeline_v2_state',
      'from "../pipeline_state_store',
      'from "../pipeline_v2_coordinator',
      'from "../pipeline_v2_recovery',
      'from "../pipeline_v2_runner',
      'from "../pipeline_runner',
      'from "../run_snapshot_store',
      'from "../pipeline_v2_docker_runtime',
      'from "../pipeline_v2_run_plan',
    ]) {
      expect(combined).not.toContain(forbidden);
    }
    expect(combined).not.toContain("Bun.serve");
    expect(combined).not.toContain("fetch(");
  });

  test("no stderr classification and no process-local run registry", () => {
    expect(combined).not.toMatch(/stderr\.(match|includes|indexOf|test|search)/);
    expect(combined).not.toContain("new Map(");
    expect(clientSource.split("JSON.parse(").length - 1).toBe(1);
  });

  test("the public reason vocabulary is imported from its single owner, never copied", () => {
    expect(clientSource).toContain('from "../pipeline_v2_public_reason.ts"');
    expect(combined).not.toContain('"run_already_exists"');
    expect(combined).not.toContain('"worker_failed"');
    expect(combined).not.toMatch(/const\s+\w*REASONS\w*\s*=\s*\[/);
  });
});

describe("control plane subprocess client: real CLI smoke", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), "cp-smoke-"));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const taskFile = join(smokeRoot, "userdata", "task.md");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let client: OrchestratorSubprocessClient;
  let daemonPid = 0;
  const runId = `cps-${Math.random().toString(36).slice(2, 8)}`;

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "smoke project\n", { mode: 0o600 });
    mkdirSync(join(smokeRoot, "userdata"), { recursive: true });
    writeFileSync(taskFile, "SMOKE-TASK\n", { mode: 0o600 });
    const log = join(smokeRoot, "daemon.log");
    const daemonLogFd = openSync(log, "a");
    const daemon = spawn(process.execPath, [join(import.meta.dir, "control_plane_daemon.ts"), socketPath], {
      stdio: ["ignore", daemonLogFd, daemonLogFd],
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("smoke daemon socket never appeared");
    client = new OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
      workingDirectory: smokeRoot,
      configRoot: join(fixtureRoot, "config"),
      pipelineRoot: join(fixtureRoot, "bundle"),
      baseEnv: {
        PATH: "/usr/bin:/bin",
        HOME: smokeRoot,
        XDG_CONFIG_HOME: credDir,
        ORCHESTRATOR_STATE_ROOT: stateRoot,
        DOCKER_HELPER_SOCKET_PATH: socketPath,
        ARCHITECT_SOURCE_VAR_1: "smoke-a",
        CODER_SOURCE_VAR_1: "smoke-c",
      },
      stdoutLimitBytes: 4_000_000,
      stderrLimitBytes: 4_000_000,
      sigintGraceMs: 3000,
    });
  }, 20000);

  afterAll(() => {
    if (daemonPid !== 0) {
      try {
        process.kill(daemonPid);
      } catch {
        // already gone
      }
    }
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  test(
    "fresh run reaches plan_ready with a state-free projection and inspect shows schemaVersion 1",
    async () => {
      const result = await client.run({
        kind: "run",
        runId,
        projectPath: projectDir,
        inputs: [{ id: "task", path: taskFile }],
      });
      const projection = expectRunProjection(result, "planReady");
      expect(projection.runId).toBe(runId);
      expect(typeof projection.runRoot).toBe("string");
      expect(JSON.stringify(projection)).not.toContain('"state"');

      const inspectResult = await client.run({ kind: "inspect", runId });
      expect(inspectResult.ok).toBe(true);
      if (!inspectResult.ok) throw new Error("unreachable");
      expect(inspectResult.projection.kind).toBe("inspect");
      if (inspectResult.projection.kind !== "inspect") throw new Error("unreachable");
      expect(inspectResult.projection.schemaVersion).toBe(1);
      expect(inspectResult.projection.status).toBe("running");
      expect(inspectResult.projection.boundary.kind).toBe("plan_ready");
    },
    60000,
  );

  test(
    "the busy race projects run_busy for the loser and waiting for the winner",
    async () => {
      const winner = client.run({ kind: "resume_plan", runId, stageId: "stage-1", initialBudget: 1 });
      await Bun.sleep(900);
      const loser = await client.run({ kind: "resume", runId });
      const loserProjection = expectRunProjection(loser, "failure");
      if (loserProjection.kind !== "failure") throw new Error("unreachable");
      expect(loserProjection.reason).toBe("run_busy");
      const winnerProjection = expectRunProjection(await winner, "waiting");
      expect(winnerProjection.runId).toBe(runId);
    },
    60000,
  );

  test(
    "a lost mutating projection is recovered through inspect by run id",
    async () => {
      void client.run({ kind: "continue_stage", runId, waitIndex: 1, additionalIterations: 1 });
      await Bun.sleep(9000);
      const inspectResult = await client.run({ kind: "inspect", runId });
      expect(inspectResult.ok).toBe(true);
      if (!inspectResult.ok) throw new Error("unreachable");
      expect(inspectResult.projection.kind).toBe("inspect");
      if (inspectResult.projection.kind !== "inspect") throw new Error("unreachable");
      expect(inspectResult.projection.status).toBe("waiting");
      expect(inspectResult.projection.boundary.kind).toBe("waiting");
      expect(inspectResult.projection.boundary.waitIndex).toBe(2);
    },
    60000,
  );
});

// --- events projection -------------------------------------------------------

function eventsScenario(scenario: string): { kind: "events"; runId: string } {
  return { kind: "events", runId: `sc-${scenario}` };
}

test("projects the events report with the run root dropped before the HTTP layer", async () => {
  const client = makeClient();
  const result = await client.run(eventsScenario("events-report"));
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("unreachable");
  const projection = result.projection;
  if (projection.kind !== "events") throw new Error("unreachable");
  expect(projection.schemaVersion).toBe(1);
  expect(projection.runId).toBe("cp-child-run");
  expect(projection.revision).toBe(5);
  expect(projection.nextAfter).toBeNull();
  expect(projection.events).toEqual([
    { revision: 1, kind: "create_run" },
    { revision: 2, kind: "start_agent_execution", execution_index: 1, state_id: "coder" },
  ]);
  expect(Object.isFrozen(projection)).toBe(true);
  expect(Object.isFrozen(projection.events)).toBe(true);
  expect(JSON.stringify(projection)).not.toContain("runRoot");
  expect(JSON.stringify(projection)).not.toContain("/tmp/cp-child-run");
}, 15000);

test("an events schemaVersion other than 1 is unsupported_contract", async () => {
  const client = makeClient();
  expectFailure(await client.run(eventsScenario("events-report-v2")), "unsupported_contract");
}, 15000);

test("projects an events failure outcome without classifying the reason", async () => {
  const client = makeClient();
  const result = await client.run(eventsScenario("events-failure"));
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("unreachable");
  expect(result.projection.kind).toBe("eventsFailure");
  if (result.projection.kind !== "eventsFailure") throw new Error("unreachable");
  expect(result.projection.exitCode).toBe(1);
  expect(result.projection.reason).toBe("invalid_cursor");
  expect(result.projection.runRoot).toBeNull();
}, 15000);

// --- logs projection -----------------------------------------------------------

function logsScenario(scenario: string): { kind: "logs"; runId: string } {
  return { kind: "logs", runId: `sc-${scenario}` };
}

test("projects the logs list with the run root dropped and the metadata frozen", async () => {
  const client = makeClient();
  const result = await client.run(logsScenario("logs-list"));
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("unreachable");
  const projection = result.projection;
  if (projection.kind !== "logs") throw new Error("unreachable");
  expect(projection.schemaVersion).toBe(1);
  expect(projection.runId).toBe("cp-child-run");
  expect(projection.revision).toBe(25);
  expect(projection.logs).toHaveLength(4);
  expect(Object.isFrozen(projection)).toBe(true);
  expect(JSON.stringify(projection)).not.toContain("runRoot");
  expect(JSON.stringify(projection)).not.toContain("/tmp/cp-child-run");
}, 15000);

test("projects the logs chunk with the base64 content and the cursor", async () => {
  const client = makeClient();
  const result = await client.run({ kind: "logs", runId: "sc-logs-chunk", executionIndex: 1, offset: 0, limit: 16 });
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("unreachable");
  const projection = result.projection;
  if (projection.kind !== "logsChunk") throw new Error("unreachable");
  expect(projection.status).toBe("captured");
  expect(projection.sha256).toBe("a".repeat(64));
  expect(projection.byteCount).toBe(47);
  expect(projection.offset).toBe(0);
  expect(Buffer.from(projection.dataBase64, "base64").toString()).toBe("hello\0world");
  expect(projection.nextOffset).toBe(16);
  expect(JSON.stringify(projection)).not.toContain("runRoot");
}, 15000);

test("projects the logs status document for a pending execution", async () => {
  const client = makeClient();
  const result = await client.run({ kind: "logs", runId: "sc-logs-status", executionIndex: 4 });
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("unreachable");
  const projection = result.projection;
  if (projection.kind !== "logsExecution") throw new Error("unreachable");
  expect(projection.status).toBe("pending");
  expect(projection.executionIndex).toBe(4);
  expect(Object.keys(projection).sort()).toEqual([
    "executionIndex", "kind", "revision", "runId", "schemaVersion", "stateId", "status",
  ]);
}, 15000);

test("projects the logs failure without classifying the reason", async () => {
  const client = makeClient();
  const result = await client.run(logsScenario("logs-failure"));
  expect(result.ok).toBe(true);
  if (!result.ok) throw new Error("unreachable");
  expect(result.projection.kind).toBe("logsFailure");
  if (result.projection.kind !== "logsFailure") throw new Error("unreachable");
  expect(result.projection.reason).toBe("artifact_modified");
  expect(result.projection.runRoot).toBeNull();
}, 15000);

test("a logs metadata entry with an unknown status is unexpected_output_shape", async () => {
  const client = makeClient();
  expectFailure(await client.run(logsScenario("logs-bad-shape")), "unexpected_output_shape");
}, 15000);

test("a logs projection with schemaVersion 2 is unsupported_contract", async () => {
  const client = makeClient();
  expectFailure(await client.run(logsScenario("logs-bad-version")), "unsupported_contract");
}, 15000);

test("a logs document carrying an extra unknown field is unexpected_output_shape", async () => {
  const client = makeClient();
  expectFailure(await client.run(logsScenario("logs-root-leak")), "unexpected_output_shape");
}, 15000);
