import { expect, test } from "bun:test";
import { runCli, type CliIo } from "../src/main.ts";
import type { CliResult, CliRunOptions, CliStdio } from "../src/docker_helper.ts";
import { resolveHelperConfig } from "../src/launcher.ts";
import type { PipelineV2RunOutcome } from "../src/pipeline_v2_runner.ts";
import type { PipelineV2WaitResponseOutcome } from "../src/pipeline_v2_wait_respond.ts";

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
    resumePipelineV2: (async () => {
      throw new Error("fake resumePipelineV2 not configured");
    }) as unknown as CliIo["resumePipelineV2"],
    respondPipelineV2Wait: (async () => {
      throw new Error("fake respondPipelineV2Wait not configured");
    }) as unknown as CliIo["respondPipelineV2Wait"],
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

test("a real resolveHelperConfig without HOME and XDG_CONFIG_HOME is a CLI configuration error", async () => {
  const { io, err } = makeIo();
  let runnerCalls = 0;
  io.runner.run = () => {
    runnerCalls += 1;
    return Promise.resolve({ code: 0 });
  };
  let pipelineV2Calls = 0;
  io.runPipelineV2 = (async () => {
    pipelineV2Calls += 1;
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  // The production resolver (imported into the fake io) needs a home
  // directory for the credential path; neither HOME nor XDG_CONFIG_HOME is
  // set in the io environment.
  io.resolveHelperConfig = resolveHelperConfig;
  io.baseEnv = { CANARY_SECRET_ENV: CANARY_SECRET };
  const exit = await runCli(["run", "--pipeline-root=/p", "--config-root=/c", "--project=/pr"], io);
  expect(exit).toBe(2);
  expect(pipelineV2Calls).toBe(0);
  expect(runnerCalls).toBe(0);
  expect(err.join("\n")).toContain("error:");
  expect(err.join("\n")).toContain("docker-helper credential");
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
});

test("an injected throwing resolveHelperConfig is a CLI configuration error", async () => {
  const { io, err } = makeIo();
  let runnerCalls = 0;
  io.runner.run = () => {
    runnerCalls += 1;
    return Promise.resolve({ code: 0 });
  };
  let pipelineV2Calls = 0;
  io.runPipelineV2 = (async () => {
    pipelineV2Calls += 1;
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  let registrations = 0;
  io.resolveStateRootProjection = () => ({ localRoot: "/state", daemonRoot: "/state" });
  io.resolveHelperConfig = () => {
    throw new Error("helper configuration exploded");
  };
  const exit = await runCli(["run", "--pipeline-root=/p", "--config-root=/c", "--project=/pr"], io);
  expect(exit).toBe(2);
  expect(pipelineV2Calls).toBe(0);
  expect(runnerCalls).toBe(0);
  expect(err.join("\n")).toContain("helper configuration exploded");
  expect(registrations).toBe(0);
});

test("with both resolvers impossible the state-root error is reported first", async () => {
  const { io, err } = makeIo();
  let helperConfigCalls = 0;
  io.resolveStateRootProjection = () => {
    throw new Error("cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)");
  };
  io.resolveHelperConfig = () => {
    helperConfigCalls += 1;
    throw new Error("helper configuration exploded");
  };
  const exit = await runCli(["run", "--pipeline-root=/p", "--config-root=/c", "--project=/pr"], io);
  expect(exit).toBe(2);
  expect(helperConfigCalls).toBe(0);
  expect(err.join("\n")).toContain("cannot build the orchestrator state root");
  expect(err.join("\n")).not.toContain("helper configuration exploded");
});

test("a parse error happens before both configuration resolvers", async () => {
  const { io, err } = makeIo();
  let projectionCalls = 0;
  let helperConfigCalls = 0;
  io.resolveStateRootProjection = () => {
    projectionCalls += 1;
    return { localRoot: "/state", daemonRoot: "/state" };
  };
  io.resolveHelperConfig = () => {
    helperConfigCalls += 1;
    return { socketPath: "/run/dh.sock", credentialFile: "/creds/token" };
  };
  const exit = await runCli(["run"], io);
  expect(exit).toBe(2);
  expect(projectionCalls).toBe(0);
  expect(helperConfigCalls).toBe(0);
  expect(err.join("\n")).toContain("--pipeline-root ABSOLUTE_PATH is required for run");
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

test("JSON mode maps only the inherit-asked calls to the streaming stderr mode", async () => {
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
    await cli(["session", "delete"], {}, "capture");
    return runOutcome({});
  }) as unknown as CliIo["runPipelineV2"];
  const stdios: CliStdio[] = [];
  const opts: Array<CliRunOptions | undefined> = [];
  io.runner.run = (args, env, stdio, options) => {
    stdios.push(stdio);
    opts.push(options);
    if (stdio === "capture") {
      return Promise.resolve({ code: 0, stdout: "STRUCTURED-ANSWER\n", stderr: "helper-warning\n" });
    }
    // A streaming call never returns captured output.
    return Promise.resolve({ code: 0 });
  };
  const exit = await runCli([...RUN_ARGS, "--json"], io);
  expect(exit).toBe(0);
  expect(stdios).toEqual(["stderr", "stderr", "capture", "capture"]);
  expect(opts).toEqual([
    undefined,
    { signalOnAbort: true, timeoutSeconds: 60 },
    undefined,
    undefined,
  ]);
  expect(out).toHaveLength(1);
  expect(out[0]!.startsWith("{")).toBe(true);
  expect(out[0]!.endsWith("\n")).toBe(true);
  // The stdout carries exactly one PipelineV2RunOutcome: no worker events,
  // no pull progress, and the streaming wrapper forwards nothing itself.
  expect(out.join("")).not.toContain("WORKER-EVENTS");
  expect(out.join("")).not.toContain("STRUCTURED-ANSWER");
  expect(out.join("")).not.toContain("PULL-PROGRESS");
  // The CLI wrapper performs no post-hoc forwarding: a fake streaming
  // result that carried stdout would never be re-emitted.
  expect(err.join("\n")).not.toContain("PULL-PROGRESS");
  expect(err.join("\n")).not.toContain("WORKER-EVENTS");
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
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

// --- resume dispatcher ---------------------------------------------------------

const RESUME_ARGS = ["resume", "--run-id", "rid-1", "--config-root", "/abs/config", "--launcher-id", "dhl_l1"];

test("resume dispatches exactly once to resumePipelineV2 with the exact options mapping", async () => {
  const { io, err } = makeIo();
  const calls: Array<{ options: unknown; deps: unknown }> = [];
  io.resumePipelineV2 = (async (options: unknown, deps: unknown) => {
    calls.push({ options, deps });
    return runOutcome({});
  }) as unknown as CliIo["resumePipelineV2"];
  io.runPipelineV2 = (async () => {
    throw new Error("runPipelineV2 must not run");
  }) as unknown as CliIo["runPipelineV2"];

  const exit = await runCli(RESUME_ARGS, io);
  expect(exit).toBe(0);
  expect(calls.length).toBe(1);
  const { options, deps } = calls[0]!;
  expect(options).toEqual({
    runId: "rid-1",
    configRoot: "/abs/config",
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

test("resume parse failures return exit 2 before any resolver, runner or subprocess call", async () => {
  for (const argv of [
    ["resume"],
    ["resume", "--run-id", "rid"],
    ["resume", "--config-root", "/c"],
    ["resume", "--run-id", "bad/id", "--config-root", "/c"],
    ["resume", "--run-id", "rid", "--config-root", "rel"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--pipeline-root", "/p"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--project", "/p"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--input", "a=/p"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--workspace", "/w"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--image", "x:1"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--profile", "coder"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--state-root", "/s"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--json", "--json"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "positional"],
    ["resume", "--run-id", "rid", "--config-root", "/c", "--unknown"],
  ]) {
    const { io, err } = makeIo();
    let resolverCalls = 0;
    let runnerCalls = 0;
    let resumeCalls = 0;
    io.resolveStateRootProjection = () => {
      resolverCalls += 1;
      return { localRoot: "/state", daemonRoot: "/state" };
    };
    io.resolveHelperConfig = () => {
      resolverCalls += 1;
      return { socketPath: "/run/dh.sock", credentialFile: "/creds/token" };
    };
    io.runner.run = () => {
      runnerCalls += 1;
      return Promise.resolve({ code: 0 });
    };
    io.resumePipelineV2 = (async () => {
      resumeCalls += 1;
      return runOutcome({});
    }) as unknown as CliIo["resumePipelineV2"];
    const exit = await runCli(argv, io);
    expect(exit).toBe(2);
    expect(resolverCalls).toBe(0);
    expect(runnerCalls).toBe(0);
    expect(resumeCalls).toBe(0);
    expect(err.join("\n")).not.toContain(CANARY_SECRET);
  }
});

test("resume state-root and helper-config resolution failures are CLI configuration errors", async () => {
  const { io, err } = makeIo();
  let resumeCalls = 0;
  io.resumePipelineV2 = (async () => {
    resumeCalls += 1;
    return runOutcome({});
  }) as unknown as CliIo["resumePipelineV2"];
  io.resolveStateRootProjection = () => {
    throw new Error("cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)");
  };
  const exit = await runCli(RESUME_ARGS, io);
  expect(exit).toBe(2);
  expect(resumeCalls).toBe(0);
  expect(err.join("\n")).toContain("cannot build the orchestrator state root");

  const { io: io2, err: err2 } = makeIo();
  io2.resumePipelineV2 = (async () => {
    resumeCalls += 1;
    return runOutcome({});
  }) as unknown as CliIo["resumePipelineV2"];
  io2.resolveHelperConfig = () => {
    throw new Error("helper configuration exploded");
  };
  const exit2 = await runCli(RESUME_ARGS, io2);
  expect(exit2).toBe(2);
  expect(resumeCalls).toBe(0);
  expect(err2.join("\n")).toContain("helper configuration exploded");
});

test("resume human mode prints the content-free summary lines", async () => {
  const { io, err, out } = makeIo();
  io.resumePipelineV2 = (async () => runOutcome({})) as unknown as CliIo["resumePipelineV2"];
  const exit = await runCli(RESUME_ARGS, io);
  expect(exit).toBe(0);
  expect(out).toEqual([]);
  expect(err).toEqual([
    "orchestrator: resume ok (run rid-1, state /state/root/pipeline-runs/rid-1/state.json, outputs /state/root/pipeline-runs/rid-1/outputs)",
  ]);

  const { io: io2, err: err2 } = makeIo();
  io2.resumePipelineV2 = (async () =>
    runOutcome({ ok: false, exitCode: 1, reason: "invalid_state" })) as unknown as CliIo["resumePipelineV2"];
  const exit2 = await runCli(RESUME_ARGS, io2);
  expect(exit2).toBe(1);
  expect(err2).toEqual([
    "orchestrator: resume failed (run rid-1, reason invalid_state, state /state/root/pipeline-runs/rid-1/state.json)",
  ]);

  // a pre-run-root refusal prints no summary line (the runner diagnostic
  // already carries the content-free cause on stderr)
  const { io: io3, err: err3 } = makeIo();
  io3.resumePipelineV2 = (async () =>
    runOutcome({ ok: false, exitCode: 1, runId: "", runRoot: null })) as unknown as CliIo["resumePipelineV2"];
  const exit3 = await runCli(RESUME_ARGS, io3);
  expect(exit3).toBe(1);
  expect(err3).toEqual([]);
});

test("resume JSON mode prints exactly one outcome document on stdout", async () => {
  const { io, out, err } = makeIo();
  const result = runOutcome({ ok: false, exitCode: 1, reason: "run_input_modified" });
  io.resumePipelineV2 = (async () => {
    io.writeError("orchestrator: launcher credential ok (launcher dhl_l1)");
    return result;
  }) as unknown as CliIo["resumePipelineV2"];
  const exit = await runCli([...RESUME_ARGS, "--json"], io);
  expect(exit).toBe(1);
  expect(out).toEqual([`${JSON.stringify(result)}\n`]);
  expect(err).toContain("orchestrator: launcher credential ok (launcher dhl_l1)");
  expect(out.join("")).not.toContain(CANARY_SECRET);
  const document = JSON.parse(out[0]!) as Record<string, unknown>;
  expect(Object.keys(document).sort()).toEqual(
    ["exitCode", "ok", "runId", "runRoot", "reason", "state"].sort(),
  );
  expect(document.reason).toBe("run_input_modified");
});

test("resume JSON mode maps only the inherit-asked calls to the streaming stderr mode", async () => {
  const { io, out, runnerCalls } = makeIo();
  io.resumePipelineV2 = (async (_options: unknown, deps: unknown) => {
    const cli = (deps as Record<string, unknown>).cli as (
      args: string[],
      env: Record<string, string>,
      stdio: CliStdio,
      opts?: CliRunOptions,
    ) => Promise<CliResult>;
    await cli(["pull", "--endpoint", "/sock", "img:1"], {}, "inherit");
    await cli(["run", "--format", "json"], {}, "inherit", { signalOnAbort: true, timeoutSeconds: 60 });
    await cli(["session", "create"], {}, "capture");
    await cli(["session", "delete"], {}, "capture");
    return runOutcome({});
  }) as unknown as CliIo["resumePipelineV2"];
  await runCli([...RESUME_ARGS, "--json"], io);
  const inheritCalls = runnerCalls.filter((call) => call.args[0] === "pull" || call.args[0] === "run");
  expect(inheritCalls.length).toBe(2);
  for (const call of inheritCalls) {
    expect(call.stdio).toBe("stderr");
  }
  const captureCalls = runnerCalls.filter((call) => call.args[0] === "session");
  expect(captureCalls.length).toBe(2);
  for (const call of captureCalls) {
    expect(call.stdio).toBe("capture");
  }
  expect(out).toEqual([`${JSON.stringify(runOutcome({}))}\n`]);
});

test("resume exit codes 0/1/130/143 pass through unchanged", async () => {
  for (const code of [0, 1, 130, 143]) {
    const { io } = makeIo();
    io.resumePipelineV2 = (async () =>
      runOutcome({ ok: code === 0, exitCode: code })) as unknown as CliIo["resumePipelineV2"];
    const exit = await runCli(RESUME_ARGS, io);
    expect(exit).toBe(code);
  }
});

// --- respond dispatcher ---------------------------------------------------------

const RESPOND_ARGS = ["respond", "--run-id", "rid-1", "--wait-index", "1", "--action", "continue_stage", "--json"];

function respondOutcome(overrides: Record<string, unknown>): PipelineV2WaitResponseOutcome {
  return {
    ok: true,
    exitCode: 0,
    runId: "rid-1",
    runRoot: "/state/root/pipeline-runs/rid-1",
    waitIndex: 1,
    actionId: "continue_stage",
    actionTo: "ship",
    requestSha256: "a".repeat(64),
    responseSha256: "b".repeat(64),
    state: null,
    ...overrides,
  } as unknown as PipelineV2WaitResponseOutcome;
}

test("respond dispatches exactly once to respondPipelineV2Wait with the exact options mapping", async () => {
  const { io, err } = makeIo();
  const calls: Array<{ options: unknown; deps: unknown }> = [];
  io.respondPipelineV2Wait = (async (options: unknown, deps: unknown) => {
    calls.push({ options, deps });
    return respondOutcome({});
  }) as unknown as CliIo["respondPipelineV2Wait"];
  io.runPipelineV2 = (async () => {
    throw new Error("runPipelineV2 must not run");
  }) as unknown as CliIo["runPipelineV2"];
  io.resumePipelineV2 = (async () => {
    throw new Error("resumePipelineV2 must not run");
  }) as unknown as CliIo["resumePipelineV2"];

  const exit = await runCli(RESPOND_ARGS, io);
  expect(exit).toBe(0);
  expect(calls.length).toBe(1);
  const { options, deps } = calls[0]!;
  expect(options).toEqual({ runId: "rid-1", waitIndex: 1, actionId: "continue_stage" });
  expect(deps).toEqual({ stateRootProjection: { localRoot: "/state/root", daemonRoot: "/daemon/root" } });
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
});

test("respond parse failures return exit 2 before any resolver, runner or subprocess call", async () => {
  for (const argv of [
    ["respond"],
    ["respond", "--run-id", "rid"],
    ["respond", "--run-id", "rid", "--wait-index", "1"],
    ["respond", "--wait-index", "1", "--action", "a"],
    ["respond", "--run-id", "bad/id", "--wait-index", "1", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "0", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "01", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "+1", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "1.5", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "1e3", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "1 ", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "99999999999999999999", "--action", "a"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "bad id"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--config-root", "/c"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--launcher-id", "dhl_x"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--pipeline-root", "/p"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--project", "/p"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--input", "a=/p"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--workspace", "/w"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--image", "x:1"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--profile", "coder"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--state-root", "/s"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--target", "ship"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "positional"],
    ["respond", "--run-id", "rid", "--wait-index", "1", "--action", "a", "--unknown"],
  ]) {
    const { io, err } = makeIo();
    let resolverCalls = 0;
    let runnerCalls = 0;
    let respondCalls = 0;
    io.resolveStateRootProjection = () => {
      resolverCalls += 1;
      return { localRoot: "/state", daemonRoot: "/state" };
    };
    io.resolveHelperConfig = () => {
      resolverCalls += 1;
      return { socketPath: "/run/dh.sock", credentialFile: "/creds/token" };
    };
    io.runner.run = () => {
      runnerCalls += 1;
      return Promise.resolve({ code: 0 });
    };
    io.respondPipelineV2Wait = (async () => {
      respondCalls += 1;
      return respondOutcome({});
    }) as unknown as CliIo["respondPipelineV2Wait"];
    const exit = await runCli(argv, io);
    expect(exit).toBe(2);
    expect(resolverCalls).toBe(0);
    expect(runnerCalls).toBe(0);
    expect(respondCalls).toBe(0);
    expect(err.join("\n")).not.toContain(CANARY_SECRET);
  }
});

test("respond state-root resolution failure is a CLI configuration error (exit 2, no API call)", async () => {
  const { io, err } = makeIo();
  let respondCalls = 0;
  io.respondPipelineV2Wait = (async () => {
    respondCalls += 1;
    return respondOutcome({});
  }) as unknown as CliIo["respondPipelineV2Wait"];
  io.resolveStateRootProjection = () => {
    throw new Error("cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)");
  };
  const exit = await runCli(RESPOND_ARGS, io);
  expect(exit).toBe(2);
  expect(respondCalls).toBe(0);
  expect(err.join("\n")).toContain("cannot build the orchestrator state root");
});

test("respond never resolves the helper configuration, runs no subprocess and registers no signal", async () => {
  const { io, err, runnerCalls } = makeIo();
  io.respondPipelineV2Wait = (async (_options: unknown, deps: unknown) => {
    // the deps carry only the projection: no helper config, no cli, no auth
    expect(deps).toEqual({ stateRootProjection: { localRoot: "/state/root", daemonRoot: "/daemon/root" } });
    return respondOutcome({});
  }) as unknown as CliIo["respondPipelineV2Wait"];
  io.resolveHelperConfig = () => {
    throw new Error("the helper configuration resolver must not be called by respond");
  };
  const exit = await runCli(RESPOND_ARGS, io);
  expect(exit).toBe(0);
  expect(runnerCalls).toEqual([]);
  expect(err.join("\n")).not.toContain("must not be called by respond");
});

test("respond human mode prints the content-free summary lines", async () => {
  const humanArgs = ["respond", "--run-id", "rid-1", "--wait-index", "1", "--action", "continue_stage"];
  const { io, err, out } = makeIo();
  io.respondPipelineV2Wait = (async () => respondOutcome({})) as unknown as CliIo["respondPipelineV2Wait"];
  const exit = await runCli(humanArgs, io);
  expect(exit).toBe(0);
  expect(out).toEqual([]);
  expect(err).toEqual([
    "orchestrator: respond ok (run rid-1, wait 1, action continue_stage, to ship, state /state/root/pipeline-runs/rid-1/state.json)",
  ]);

  const { io: io2, err: err2 } = makeIo();
  io2.respondPipelineV2Wait = (async () =>
    respondOutcome({ ok: false, exitCode: 1, state: null, reason: "wait_conflict" })) as unknown as CliIo["respondPipelineV2Wait"];
  const exit2 = await runCli(humanArgs, io2);
  expect(exit2).toBe(1);
  expect(err2).toEqual([
    "orchestrator: respond failed (run rid-1, reason wait_conflict, state /state/root/pipeline-runs/rid-1/state.json)",
  ]);

  // a pre-layout refusal carries no run root and still prints one line
  const { io: io3, err: err3 } = makeIo();
  io3.respondPipelineV2Wait = (async () =>
    respondOutcome({ ok: false, exitCode: 1, state: null, runRoot: null, reason: "run_layout_invalid" })) as unknown as CliIo["respondPipelineV2Wait"];
  const exit3 = await runCli(humanArgs, io3);
  expect(exit3).toBe(1);
  expect(err3).toEqual([
    "orchestrator: respond failed (run rid-1, reason run_layout_invalid)",
  ]);
});

test("respond JSON mode prints exactly one outcome document on stdout", async () => {
  const { io, out, err } = makeIo();
  const result = respondOutcome({ ok: false, exitCode: 1, state: null, reason: "state_persist_failed" });
  io.respondPipelineV2Wait = (async () => {
    io.writeError("orchestrator: pipeline v2 wait response failed: state_persist_failed");
    return result;
  }) as unknown as CliIo["respondPipelineV2Wait"];
  const exit = await runCli(RESPOND_ARGS, io);
  expect(exit).toBe(1);
  expect(out).toEqual([`${JSON.stringify(result)}\n`]);
  expect(err).toContain("orchestrator: pipeline v2 wait response failed: state_persist_failed");
  expect(out.join("")).not.toContain(CANARY_SECRET);
  const document = JSON.parse(out[0]!) as Record<string, unknown>;
  expect(Object.keys(document).sort()).toEqual(
    ["actionId", "actionTo", "exitCode", "ok", "reason", "requestSha256", "responseSha256", "runId", "runRoot", "state", "waitIndex"].sort(),
  );
  expect(document.reason).toBe("state_persist_failed");
});

test("respond exit codes 0 and 1 pass through unchanged", async () => {
  for (const code of [0, 1]) {
    const { io } = makeIo();
    io.respondPipelineV2Wait = (async () =>
      respondOutcome({ ok: code === 0, exitCode: code })) as unknown as CliIo["respondPipelineV2Wait"];
    const exit = await runCli(RESPOND_ARGS, io);
    expect(exit).toBe(code);
  }
});

test("the respond command appears in the command list and unknown commands are still rejected", async () => {
  const { io, err } = makeIo();
  const exit = await runCli(["deploy"], io);
  expect(exit).toBe(2);
  expect(err.join("\n")).toContain("'orchestrator respond'");
});

test("resume deps.cli is wired to the single runner instance", async () => {
  const { io, runnerCalls } = makeIo();
  let capturedCli: unknown;
  io.resumePipelineV2 = (async (_options: unknown, deps: unknown) => {
    capturedCli = (deps as Record<string, unknown>).cli;
    return runOutcome({});
  }) as unknown as CliIo["resumePipelineV2"];
  await runCli(RESUME_ARGS, io);
  const cli = capturedCli as (args: string[], env: Record<string, string>, stdio: CliStdio) => Promise<CliResult>;
  await cli(["session", "list"], {}, "capture");
  expect(runnerCalls).toEqual([{ args: ["session", "list"], env: {}, stdio: "capture", opts: undefined }]);
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
