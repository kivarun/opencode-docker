import { expect, test } from "bun:test";
import { runCli, type CliIo } from "../src/main.ts";
import type { CliResult, CliRunOptions, CliStdio } from "../src/docker_helper.ts";
import type { PipelineV2RunOutcome } from "../src/pipeline_v2_runner.ts";

const CANARY_SECRET = "CANARY-SECRET-dhsec9f1";

interface Recorder {
  io: CliIo;
  out: string[];
  err: string[];
  runnerCalls: Array<{ args: string[]; env: Record<string, string>; stdio: CliStdio; opts?: CliRunOptions }>;
}

function makeIo(): Recorder {
  const out: string[] = [];
  const err: string[] = [];
  const runnerCalls: Recorder["runnerCalls"] = [];
  const io: CliIo = {
    baseEnv: { HOME: "/home/u", CANARY_SECRET_ENV: CANARY_SECRET },
    runner: {
      run: (
        args: string[],
        env: Record<string, string>,
        stdio: CliStdio,
        opts?: CliRunOptions,
      ): Promise<CliResult> => {
        runnerCalls.push({ args, env, stdio, opts });
        return Promise.resolve({ code: 0 });
      },
      killActive: (signal: "SIGINT" | "SIGTERM"): boolean => {
        err.push(`killActive:${signal}`);
        return false;
      },
    },
    fetchAuth: () => Promise.resolve({ status: 200, body: {} }),
    resolveHelperConfig: () => ({ socketPath: "/run/dh.sock", credentialFile: "/creds/token" }),
    resolveStateRootProjection: () => ({ localRoot: "/state/root", daemonRoot: "/daemon/root" }),
    runPipelineV2: (async () => {
      throw new Error("fake runPipelineV2 not configured");
    }) as unknown as CliIo["runPipelineV2"],
    runSmoke: (async () => {
      throw new Error("fake runSmoke not configured");
    }) as unknown as CliIo["runSmoke"],
    runAgentSmoke: (async () => {
      throw new Error("fake runAgentSmoke not configured");
    }) as unknown as CliIo["runAgentSmoke"],
    writeStdout: (text) => out.push(text),
    writeError: (text) => err.push(text),
  };
  return { io, out, err, runnerCalls };
}

const RUN_ARGS = [
  "run",
  "--pipeline-root", "/abs/pipeline",
  "--config-root", "/abs/config",
  "--project", "/abs/project",
  "--input", "spec=/abs/spec.md",
  "--input", "docs=/abs/docs dir",
  "--launcher-id", "dhl_l1",
];

function runOutcome(overrides: Partial<PipelineV2RunOutcome>): PipelineV2RunOutcome {
  return {
    ok: true,
    exitCode: 0,
    runId: "rid-1",
    runRoot: "/state/root/pipeline-runs/rid-1",
    state: null,
    ...overrides,
  } as PipelineV2RunOutcome;
}

test("run dispatches exactly once to runPipelineV2 with the exact options mapping", async () => {
  const { io, err } = makeIo();
  const calls: Array<{ options: unknown; deps: unknown }> = [];
  io.runPipelineV2 = (async (options: unknown, deps: unknown) => {
    calls.push({ options, deps });
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  io.runSmoke = (async () => {
    throw new Error("smoke must not run");
  }) as unknown as CliIo["runSmoke"];
  io.runAgentSmoke = (async () => {
    throw new Error("agent-smoke must not run");
  }) as unknown as CliIo["runAgentSmoke"];

  const exit = await runCli(RUN_ARGS, io);
  expect(exit).toBe(0);
  expect(calls.length).toBe(1);
  const { options, deps } = calls[0]!;
  expect(options).toEqual({
    pipelineRoot: "/abs/pipeline",
    configRoot: "/abs/config",
    projectSourcePath: "/abs/project",
    inputBindings: [
      { id: "spec", path: "/abs/spec.md" },
      { id: "docs", path: "/abs/docs dir" },
    ],
    launcherId: "dhl_l1",
  });
  const d = deps as Record<string, unknown>;
  expect(d.helperConfig).toEqual({ socketPath: "/run/dh.sock", credentialFile: "/creds/token" });
  expect(d.baseEnv).toEqual(io.baseEnv);
  expect(d.stateRootProjection).toEqual({ localRoot: "/state/root", daemonRoot: "/daemon/root" });
  expect(d.fetchAuth).toBe(io.fetchAuth);
  expect(typeof d.cli).toBe("function");
  expect(typeof d.onSignal).toBe("function");
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
});

test("run deps.cli is wired to the single runner instance", async () => {
  const { io, runnerCalls } = makeIo();
  let capturedCli: unknown;
  io.runPipelineV2 = (async (_options: unknown, deps: unknown) => {
    capturedCli = (deps as Record<string, unknown>).cli;
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  await runCli(["run", "--pipeline-root=/p", "--config-root=/c", "--project=/pr"], io);
  const cli = capturedCli as (args: string[], env: Record<string, string>, stdio: CliStdio) => Promise<CliResult>;
  await cli(["session", "list"], {}, "capture");
  expect(runnerCalls).toEqual([{ args: ["session", "list"], env: {}, stdio: "capture", opts: undefined }]);
});

test("exit codes 0/1/130/143 pass through unchanged", async () => {
  for (const code of [0, 1, 130, 143]) {
    const { io } = makeIo();
    io.runPipelineV2 = (async () => runOutcome({ ok: code === 0, exitCode: code })) as unknown as CliIo["runPipelineV2"];
    const exit = await runCli(["run", "--pipeline-root=/p", "--config-root=/c", "--project=/pr"], io);
    expect(exit).toBe(code);
  }
});

test("parse errors return 2 before any runner or runner-function call", async () => {
  const { io } = makeIo();
  let called = 0;
  io.runPipelineV2 = (async () => {
    called += 1;
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  const exit = await runCli(["run", "--pipeline-root=/p"], io);
  expect(exit).toBe(2);
  expect(called).toBe(0);
});

test("unknown command returns 2 before any runner call", async () => {
  const { io } = makeIo();
  let called = 0;
  io.runPipelineV2 = (async () => {
    called += 1;
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  expect(await runCli(["deploy"], io)).toBe(2);
  expect(called).toBe(0);
});

test("state-root resolver failure is a CLI configuration error (exit 2, no runner call)", async () => {
  const { io, err } = makeIo();
  let called = 0;
  io.runPipelineV2 = (async () => {
    called += 1;
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  io.resolveStateRootProjection = () => {
    throw new Error("cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)");
  };
  const exit = await runCli(["run", "--pipeline-root=/p", "--config-root=/c", "--project=/pr"], io);
  expect(exit).toBe(2);
  expect(called).toBe(0);
  expect(err.join("\n")).toContain("cannot build the orchestrator state root");
});

test("human mode: success prints one stderr summary line with run/state/outputs", async () => {
  const { io, err, out } = makeIo();
  io.runPipelineV2 = (async () => runOutcome({})) as unknown as CliIo["runPipelineV2"];
  const exit = await runCli(RUN_ARGS, io);
  expect(exit).toBe(0);
  expect(out).toEqual([]);
  expect(err).toEqual([
    "orchestrator: run ok (run rid-1, state /state/root/pipeline-runs/rid-1/state.json, outputs /state/root/pipeline-runs/rid-1/outputs)",
  ]);
});

test("human mode: post-run-root failure prints one summary line with reason", async () => {
  const { io, err } = makeIo();
  io.runPipelineV2 = (async () =>
    runOutcome({ ok: false, exitCode: 1, reason: "worker_failed" })) as unknown as CliIo["runPipelineV2"];
  const exit = await runCli(RUN_ARGS, io);
  expect(exit).toBe(1);
  expect(err).toEqual([
    "orchestrator: run failed (run rid-1, reason worker_failed, state /state/root/pipeline-runs/rid-1/state.json)",
  ]);
});

test("human mode: pre-run-root failure prints no summary line", async () => {
  const { io, err } = makeIo();
  io.runPipelineV2 = (async () =>
    runOutcome({ ok: false, exitCode: 1, runId: "", runRoot: null })) as unknown as CliIo["runPipelineV2"];
  const exit = await runCli(RUN_ARGS, io);
  expect(exit).toBe(1);
  expect(err).toEqual([]);
});

test("JSON mode: stdout holds exactly one JSON document, stderr may carry runner diagnostics", async () => {
  const { io, out, err } = makeIo();
  const result = runOutcome({ ok: false, exitCode: 143, reason: "signal_sigterm" });
  io.runPipelineV2 = (async () => {
    io.writeError("orchestrator: launcher credential ok (launcher dhl_l1)");
    return result;
  }) as unknown as CliIo["runPipelineV2"];
  const exit = await runCli([...RUN_ARGS, "--json"], io);
  expect(exit).toBe(143);
  expect(out).toEqual([`${JSON.stringify(result)}\n`]);
  expect(err).toContain("orchestrator: launcher credential ok (launcher dhl_l1)");
  expect(out.join("")).not.toContain(CANARY_SECRET);
});

test("JSON mode: document round-trips the outcome fields without extra wrappers", async () => {
  const { io, out } = makeIo();
  const result = runOutcome({});
  io.runPipelineV2 = (async () => result) as unknown as CliIo["runPipelineV2"];
  await runCli([...RUN_ARGS, "--json"], io);
  expect(out.length).toBe(1);
  const document = JSON.parse(out[0]!) as Record<string, unknown>;
  expect(Object.keys(document).sort()).toEqual(
    ["exitCode", "ok", "runId", "runRoot", "state"].sort(),
  );
  expect(document.runId).toBe("rid-1");
  expect(document.ok).toBe(true);
});

test("JSON mode: inherit-asked CLI calls are captured and forwarded to stderr", async () => {
  const { io, out, err } = makeIo();
  io.runPipelineV2 = (async (_options: unknown, deps: unknown) => {
    const cli = (deps as Record<string, unknown>).cli as (
      args: string[],
      env: Record<string, string>,
      stdio: CliStdio,
      opts?: CliRunOptions,
    ) => Promise<CliResult>;
    await cli(["pull", "--endpoint", "/sock", "img:1"], {}, "inherit");
    await cli(["run", "--format", "json"], {}, "inherit", { signalOnAbort: true, timeoutSeconds: 60 });
    await cli(["session", "create"], {}, "capture");
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  const stdios: CliStdio[] = [];
  io.runner.run = (args, env, stdio, opts) => {
    stdios.push(stdio);
    if (stdio === "capture") {
      return Promise.resolve({ code: 0, stdout: "WORKER-EVENTS\n", stderr: "helper-warning\n" });
    }
    return Promise.resolve({ code: 0 });
  };
  const exit = await runCli([...RUN_ARGS, "--json"], io);
  expect(exit).toBe(0);
  expect(stdios).toEqual(["capture", "capture", "capture"]);
  expect(out).toHaveLength(1);
  expect(out[0]!.startsWith("{")).toBe(true);
  expect(out.join("")).not.toContain("WORKER-EVENTS");
  expect(err.join("\n")).toContain("WORKER-EVENTS");
  expect(err.join("\n")).toContain("helper-warning");
});

test("human mode keeps the adapter's inherit plumbing untouched", async () => {
  const { io, err } = makeIo();
  const stdios: CliStdio[] = [];
  io.runner.run = (args, env, stdio, opts) => {
    stdios.push(stdio);
    return Promise.resolve({ code: 0, stdout: "WORKER-EVENTS\n" });
  };
  io.runPipelineV2 = (async (_options: unknown, deps: unknown) => {
    const cli = (deps as Record<string, unknown>).cli as (
      args: string[],
      env: Record<string, string>,
      stdio: CliStdio,
    ) => Promise<CliResult>;
    await cli(["pull", "--endpoint", "/sock", "img:1"], {}, "inherit");
    await cli(["run"], {}, "inherit");
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  await runCli(RUN_ARGS, io);
  expect(stdios).toEqual(["inherit", "inherit"]);
  expect(err).toEqual([
    "orchestrator: run ok (run rid-1, state /state/root/pipeline-runs/rid-1/state.json, outputs /state/root/pipeline-runs/rid-1/outputs)",
  ]);
});

test("smoke and agent-smoke keep routing to their own runner functions", async () => {
  const smokeCalls: Array<{ options: unknown; hasDeps: boolean }> = [];
  const agentCalls: Array<{ options: unknown; hasDeps: boolean }> = [];
  const { io, err } = makeIo();
  io.runSmoke = (async (options: unknown, deps: unknown) => {
    smokeCalls.push({ options, hasDeps: typeof (deps as Record<string, unknown>).cli === "function" });
    return { ok: true, exitCode: 0, runId: "s1", sessionId: "sess", status: "completed" };
  }) as unknown as CliIo["runSmoke"];
  io.runAgentSmoke = (async (options: unknown, deps: unknown) => {
    agentCalls.push({ options, hasDeps: typeof (deps as Record<string, unknown>).cli === "function" });
    return { ok: true, exitCode: 0, runId: "a1", sessionId: "sess", status: "completed" };
  }) as unknown as CliIo["runAgentSmoke"];

  expect(await runCli(["smoke", "--workspace", "/w", "--image", "img:1", "--launcher-id", "dhl_x"], io)).toBe(0);
  expect(await runCli(["agent-smoke", "--workspace", "/w", "--config-root", "/cfg"], io)).toBe(0);
  expect(smokeCalls).toEqual([
    { options: { workspace: "/w", workerImage: "img:1", launcherId: "dhl_x" }, hasDeps: true },
  ]);
  expect(agentCalls).toEqual([
    {
      options: { workspace: "/w", configRoot: "/cfg", pipelineRoot: "/opt/orchestrator/pipelines/default", launcherId: undefined },
      hasDeps: true,
    },
  ]);
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
});
