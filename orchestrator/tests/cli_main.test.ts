import { expect, test } from "bun:test";
import { chmodSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runCli, type CliIo } from "../src/main.ts";
import type { CliResult, CliRunOptions, CliStdio } from "../src/docker_helper.ts";
import { resolveHelperConfig } from "../src/launcher.ts";
import {
  continuePipelineV2Stage,
  revisePipelineV2Task,
  type PipelineV2RunnerDeps,
  type PipelineV2RunOutcome,
} from "../src/pipeline_v2_runner.ts";
import { respondPipelineV2Wait, type PipelineV2WaitResponseOutcome } from "../src/pipeline_v2_wait_respond.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { enterPipelineV2Wait } from "../src/pipeline_v2_wait_controller.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { parsePipelineV2RunState, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import {
  preparePlanRevisionManifest,
  prepareTaskRevisionManifest,
} from "../src/pipeline_v2_run_plan_manifests.ts";
import { preparePipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_candidate.ts";
import { acceptPipelineV2RunPlanCandidate } from "../src/pipeline_v2_run_plan_controller.ts";
import { ensurePipelineV2StageIteration } from "../src/pipeline_v2_stage_iteration_controller.ts";
import {
  acceptActivationOutputs,
  prepareActivationData,
  prepareRunProject,
  snapshotRunInputs,
  type AcceptedStateOutput,
  type PreparedActivationData,
  type RunInputBinding,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { startRoleArgs } from "./pipeline_v2_state_fixtures.ts";

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
    continuePipelineV2Stage: (async () => {
      throw new Error("fake continuePipelineV2Stage not configured");
    }) as unknown as CliIo["continuePipelineV2Stage"],
    revisePipelineV2Task: (async () => {
      throw new Error("fake revisePipelineV2Task not configured");
    }) as unknown as CliIo["revisePipelineV2Task"],
    respondPipelineV2Wait: (async () => {
      throw new Error("fake respondPipelineV2Wait not configured");
    }) as unknown as CliIo["respondPipelineV2Wait"],
    runSmoke: (async () => {
      throw new Error("fake runSmoke not configured");
    }) as unknown as CliIo["runSmoke"],
    runAgentSmoke: (async () => {
      throw new Error("fake runAgentSmoke not configured");
    }) as unknown as CliIo["runAgentSmoke"],
    readTaskFile: async () => {
      throw new Error("fake readTaskFile not configured");
    },
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

const CONTINUE_STAGE_ARGS = [
  "continue-stage",
  "--run-id", "rid-1",
  "--wait-index", "2",
  "--additional-iterations", "3",
  "--config-root", "/abs/config",
  "--launcher-id", "dhl_l1",
];

test("runCli routes continue-stage to continuePipelineV2Stage exactly once", async () => {
  const { io, out, err } = makeIo();
  const calls: Array<{ options: unknown }> = [];
  io.continuePipelineV2Stage = (async (options: unknown) => {
    calls.push({ options });
    return runOutcome({});
  }) as unknown as CliIo["continuePipelineV2Stage"];
  const exit = await runCli(CONTINUE_STAGE_ARGS, io);
  expect(exit).toBe(0);
  expect(calls.length).toBe(1);
  expect(calls[0]!.options).toEqual({
    runId: "rid-1",
    waitIndex: 2,
    additionalIterations: 3,
    configRoot: "/abs/config",
    launcherId: "dhl_l1",
  });
  // human mode: the summary line goes to stderr, stdout stays empty
  expect(out).toEqual([]);
  expect(err.join("\n")).toContain("orchestrator: continue-stage ok (run rid-1");
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
});

test("continue-stage parse failures return exit 2 before any resolver, runner or facade call", async () => {
  for (const argv of [
    ["continue-stage"],
    ["continue-stage", "--run-id", "rid"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1"],
    ["continue-stage", "--run-id", "rid", "--additional-iterations", "2", "--config-root", "/c"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--config-root", "/c"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "2"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "0", "--additional-iterations", "2", "--config-root", "/c"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "0", "--config-root", "/c"],
    ["continue-stage", "--run-id", "../x", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/c"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "relative"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/c", "--action", "continue_stage"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/c", "--launcher-id", "x"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/c", "--json", "--json"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/c", "positional"],
    ["continue-stage", "--run-id", "rid", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/c", "--unknown"],
  ]) {
    const { io, err } = makeIo();
    let resolverCalls = 0;
    let runnerCalls = 0;
    let facadeCalls = 0;
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
    io.continuePipelineV2Stage = (async () => {
      facadeCalls += 1;
      return runOutcome({});
    }) as unknown as CliIo["continuePipelineV2Stage"];
    const exit = await runCli(argv, io);
    expect(exit).toBe(2);
    expect(resolverCalls).toBe(0);
    expect(runnerCalls).toBe(0);
    expect(facadeCalls).toBe(0);
    expect(err.join("\n")).not.toContain(CANARY_SECRET);
  }
});

test("continue-stage state-root and helper-config resolution failures are CLI configuration errors", async () => {
  const { io, err } = makeIo();
  let facadeCalls = 0;
  io.continuePipelineV2Stage = (async () => {
    facadeCalls += 1;
    return runOutcome({});
  }) as unknown as CliIo["continuePipelineV2Stage"];
  io.resolveStateRootProjection = () => {
    throw new Error("cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)");
  };
  const exit = await runCli(CONTINUE_STAGE_ARGS, io);
  expect(exit).toBe(2);
  expect(facadeCalls).toBe(0);
  expect(err.join("\n")).toContain("cannot build the orchestrator state root");

  const { io: io2, err: err2 } = makeIo();
  io2.continuePipelineV2Stage = (async () => {
    facadeCalls += 1;
    return runOutcome({});
  }) as unknown as CliIo["continuePipelineV2Stage"];
  io2.resolveHelperConfig = () => {
    throw new Error("helper configuration exploded");
  };
  const exit2 = await runCli(CONTINUE_STAGE_ARGS, io2);
  expect(exit2).toBe(2);
  expect(facadeCalls).toBe(0);
  expect(err2.join("\n")).toContain("helper configuration exploded");
});

test("continue-stage resolves the state-root projection before the helper configuration", async () => {
  const { io, err } = makeIo();
  let helperResolverCalls = 0;
  io.continuePipelineV2Stage = (async () => runOutcome({})) as unknown as CliIo["continuePipelineV2Stage"];
  io.resolveStateRootProjection = () => {
    if (helperResolverCalls > 0) {
      throw new Error("the helper configuration was resolved before the state-root projection");
    }
    return { localRoot: "/state/root", daemonRoot: "/daemon/root" };
  };
  io.resolveHelperConfig = () => {
    helperResolverCalls += 1;
    return { socketPath: "/run/dh.sock", credentialFile: "/creds/token" };
  };
  const exit = await runCli(CONTINUE_STAGE_ARGS, io);
  expect(exit).toBe(0);
  expect(helperResolverCalls).toBe(1);
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
});

test("continue-stage human mode prints the content-free summary lines", async () => {
  const { io, err, out } = makeIo();
  io.continuePipelineV2Stage = (async () => runOutcome({})) as unknown as CliIo["continuePipelineV2Stage"];
  const exit = await runCli(CONTINUE_STAGE_ARGS, io);
  expect(exit).toBe(0);
  expect(out).toEqual([]);
  expect(err).toEqual([
    "orchestrator: continue-stage ok (run rid-1, state /state/root/pipeline-runs/rid-1/state.json, outputs /state/root/pipeline-runs/rid-1/outputs)",
  ]);

  const { io: io2, err: err2 } = makeIo();
  io2.continuePipelineV2Stage = (async () =>
    runOutcome({ ok: false, exitCode: 1, reason: "invalid_state" })) as unknown as CliIo["continuePipelineV2Stage"];
  const exit2 = await runCli(CONTINUE_STAGE_ARGS, io2);
  expect(exit2).toBe(1);
  expect(err2).toEqual([
    "orchestrator: continue-stage failed (run rid-1, reason invalid_state, state /state/root/pipeline-runs/rid-1/state.json)",
  ]);

  // a pre-run-root refusal prints no summary line
  const { io: io3, err: err3 } = makeIo();
  io3.continuePipelineV2Stage = (async () =>
    runOutcome({ ok: false, exitCode: 1, runId: "", runRoot: null })) as unknown as CliIo["continuePipelineV2Stage"];
  const exit3 = await runCli(CONTINUE_STAGE_ARGS, io3);
  expect(exit3).toBe(1);
  expect(err3).toEqual([]);
});

test("continue-stage JSON mode prints exactly one outcome document on stdout", async () => {
  const { io, out, err } = makeIo();
  const result = runOutcome({ ok: false, exitCode: 1, reason: "run_input_modified" });
  io.continuePipelineV2Stage = (async () => {
    io.writeError("orchestrator: launcher credential ok (launcher dhl_l1)");
    return result;
  }) as unknown as CliIo["continuePipelineV2Stage"];
  const exit = await runCli([...CONTINUE_STAGE_ARGS, "--json"], io);
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

test("continue-stage JSON mode maps only the inherit-asked calls to the streaming stderr mode", async () => {
  const { io, out, runnerCalls } = makeIo();
  io.continuePipelineV2Stage = (async (_options: unknown, deps: unknown) => {
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
  }) as unknown as CliIo["continuePipelineV2Stage"];
  await runCli([...CONTINUE_STAGE_ARGS, "--json"], io);
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

test("continue-stage exit codes 0/1/130/143 pass through unchanged", async () => {
  for (const code of [0, 1, 130, 143]) {
    const { io } = makeIo();
    io.continuePipelineV2Stage = (async () =>
      runOutcome({ ok: code === 0, exitCode: code })) as unknown as CliIo["continuePipelineV2Stage"];
    const exit = await runCli(CONTINUE_STAGE_ARGS, io);
    expect(exit).toBe(code);
  }
});

test("continue-stage deps.cli is wired to the single runner instance", async () => {
  const { io, runnerCalls } = makeIo();
  let capturedCli: unknown;
  io.continuePipelineV2Stage = (async (_options: unknown, deps: unknown) => {
    capturedCli = (deps as Record<string, unknown>).cli;
    return runOutcome({});
  }) as unknown as CliIo["continuePipelineV2Stage"];
  await runCli(CONTINUE_STAGE_ARGS, io);
  const cli = capturedCli as (args: string[], env: Record<string, string>, stdio: CliStdio) => Promise<CliResult>;
  await cli(["session", "list"], {}, "capture");
  expect(runnerCalls).toEqual([{ args: ["session", "list"], env: {}, stdio: "capture", opts: undefined }]);
});

test("the continue-stage command appears in the command list and unknown commands are still rejected", async () => {
  const { io, err } = makeIo();
  const exit = await runCli(["deploy"], io);
  expect(exit).toBe(2);
  expect(err.join("\n")).toContain("'orchestrator continue-stage'");
});

// --- revise-task dispatcher --------------------------------------------------

const REVISE_TASK_ARGS = [
  "revise-task",
  "--run-id", "rid-1",
  "--wait-index", "2",
  "--task-id", "task-a",
  "--task-file", "/abs/body.md",
  "--config-root", "/abs/config",
  "--launcher-id", "dhl_l1",
];

test("runCli routes revise-task to revisePipelineV2Task exactly once with the task file read once", async () => {
  const { io, out, err } = makeIo();
  const calls: Array<{ options: unknown }> = [];
  io.revisePipelineV2Task = (async (options: unknown) => {
    calls.push({ options });
    return runOutcome({});
  }) as unknown as CliIo["revisePipelineV2Task"];
  let fileReads = 0;
  io.readTaskFile = async () => {
    fileReads += 1;
    return "REVISED BODY\n";
  };
  const exit = await runCli(REVISE_TASK_ARGS, io);
  expect(exit).toBe(0);
  expect(calls.length).toBe(1);
  expect(fileReads).toBe(1);
  expect(calls[0]!.options).toEqual({
    runId: "rid-1",
    waitIndex: 2,
    taskId: "task-a",
    taskBody: "REVISED BODY\n",
    configRoot: "/abs/config",
    launcherId: "dhl_l1",
  });
  // human mode: the summary line goes to stderr, stdout stays empty
  expect(out).toEqual([]);
  expect(err.join("\n")).toContain("orchestrator: revise-task ok (run rid-1");
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
  // the body and the path never appear in any output or diagnostic
  expect(JSON.stringify(err)).not.toContain("REVISED BODY");
  expect(JSON.stringify(err)).not.toContain("/abs/body.md");
  expect(JSON.stringify(out)).not.toContain("/abs/body.md");
});

test("revise-task passes the exact file content unchanged, including whitespace and the final newline", async () => {
  const { io } = makeIo();
  const bodies: string[] = [];
  io.revisePipelineV2Task = (async (options: unknown) => {
    bodies.push((options as Record<string, unknown>).taskBody as string);
    return runOutcome({});
  }) as unknown as CliIo["revisePipelineV2Task"];
  let fileReads = 0;
  io.readTaskFile = async () => {
    fileReads += 1;
    return "\n  revised body with leading/trailing space  \t\n\n";
  };
  const exit = await runCli(REVISE_TASK_ARGS, io);
  expect(exit).toBe(0);
  expect(fileReads).toBe(1);
  // no trim, no newline normalization, no added trailing newline
  expect(bodies).toEqual(["\n  revised body with leading/trailing space  \t\n\n"]);
});

test("revise-task calls no other production API", async () => {
  const { io } = makeIo();
  for (const key of [
    "runPipelineV2",
    "resumePipelineV2",
    "continuePipelineV2Stage",
    "respondPipelineV2Wait",
    "runSmoke",
    "runAgentSmoke",
  ] as const) {
    io[key] = (async () => {
      throw new Error(`${key} must not run`);
    }) as never;
  }
  io.revisePipelineV2Task = (async () => runOutcome({})) as unknown as CliIo["revisePipelineV2Task"];
  io.readTaskFile = async () => "BODY\n";
  const exit = await runCli(REVISE_TASK_ARGS, io);
  expect(exit).toBe(0);
});

test("revise-task parse failures return exit 2 before any resolver, file or runner call", async () => {
  for (const argv of [
    ["revise-task"],
    ["revise-task", "--run-id", "rid"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--config-root", "/c"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md"],
    ["revise-task", "--run-id", "rid", "--wait-index", "0", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "/c"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "relative", "--config-root", "/c"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "relative"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "/c", "--task-body", "inline"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "/c", "--action", "revise_task"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "/c", "--launcher-id", "x"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "/c", "--json", "--json"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "/c", "positional"],
    ["revise-task", "--run-id", "rid", "--wait-index", "1", "--task-id", "task", "--task-file", "/abs/b.md", "--config-root", "/c", "--unknown"],
  ]) {
    const { io, err } = makeIo();
    let resolverCalls = 0;
    let fileReads = 0;
    let runnerCalls = 0;
    let facadeCalls = 0;
    io.resolveStateRootProjection = () => {
      resolverCalls += 1;
      return { localRoot: "/state", daemonRoot: "/state" };
    };
    io.resolveHelperConfig = () => {
      resolverCalls += 1;
      return { socketPath: "/run/dh.sock", credentialFile: "/creds/token" };
    };
    io.readTaskFile = async () => {
      fileReads += 1;
      return "BODY";
    };
    io.runner.run = () => {
      runnerCalls += 1;
      return Promise.resolve({ code: 0 });
    };
    io.revisePipelineV2Task = (async () => {
      facadeCalls += 1;
      return runOutcome({});
    }) as unknown as CliIo["revisePipelineV2Task"];
    const exit = await runCli(argv, io);
    expect(exit).toBe(2);
    expect(resolverCalls).toBe(0);
    expect(fileReads).toBe(0);
    expect(runnerCalls).toBe(0);
    expect(facadeCalls).toBe(0);
    expect(err.join("\n")).not.toContain(CANARY_SECRET);
  }
});

test("revise-task state-root and helper-config resolution failures are CLI configuration errors", async () => {
  const { io, err } = makeIo();
  let fileReads = 0;
  let facadeCalls = 0;
  io.revisePipelineV2Task = (async () => {
    facadeCalls += 1;
    return runOutcome({});
  }) as unknown as CliIo["revisePipelineV2Task"];
  io.readTaskFile = async () => {
    fileReads += 1;
    return "BODY";
  };
  io.resolveStateRootProjection = () => {
    throw new Error("cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)");
  };
  const exit = await runCli(REVISE_TASK_ARGS, io);
  expect(exit).toBe(2);
  expect(fileReads).toBe(0);
  expect(facadeCalls).toBe(0);
  expect(err.join("\n")).toContain("cannot build the orchestrator state root");

  const { io: io2, err: err2 } = makeIo();
  let fileReads2 = 0;
  io2.revisePipelineV2Task = (async () => {
    throw new Error("the runner must not run");
  }) as never;
  io2.readTaskFile = async () => {
    fileReads2 += 1;
    return "BODY";
  };
  io2.resolveHelperConfig = () => {
    throw new Error("helper configuration exploded");
  };
  const exit2 = await runCli(REVISE_TASK_ARGS, io2);
  expect(exit2).toBe(2);
  expect(fileReads2).toBe(0);
  expect(err2.join("\n")).toContain("helper configuration exploded");
});

test("revise-task resolves the state-root projection before the helper configuration", async () => {
  const { io, err } = makeIo();
  let helperResolverCalls = 0;
  io.revisePipelineV2Task = (async () => runOutcome({})) as unknown as CliIo["revisePipelineV2Task"];
  io.readTaskFile = async () => "BODY";
  io.resolveStateRootProjection = () => {
    if (helperResolverCalls > 0) {
      throw new Error("the helper configuration was resolved before the state-root projection");
    }
    return { localRoot: "/state/root", daemonRoot: "/daemon/root" };
  };
  io.resolveHelperConfig = () => {
    helperResolverCalls += 1;
    return { socketPath: "/run/dh.sock", credentialFile: "/creds/token" };
  };
  const exit = await runCli(REVISE_TASK_ARGS, io);
  expect(exit).toBe(0);
  expect(helperResolverCalls).toBe(1);
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
});

test("revise-task task-file read failures and an empty file are contract errors with content-free messages", async () => {
  // a read failure: no path, no body, no hostile filesystem error echo
  const { io, err } = makeIo();
  let facadeCalls = 0;
  let resolverCalls = 0;
  io.revisePipelineV2Task = (async () => {
    facadeCalls += 1;
    return runOutcome({});
  }) as unknown as CliIo["revisePipelineV2Task"];
  io.resolveStateRootProjection = () => {
    resolverCalls += 1;
    return { localRoot: "/state", daemonRoot: "/state" };
  };
  io.resolveHelperConfig = () => {
    resolverCalls += 1;
    return { socketPath: "/run/dh.sock", credentialFile: "/creds/token" };
  };
  const hostilePath = "/abs/secret/body.md";
  io.readTaskFile = async () => {
    const cause = new Error("EACCES: permission denied, open '/hostile/machine/path/body.md'");
    (cause as { code?: string }).code = "EACCES";
    throw cause;
  };
  const exit = await runCli(
    ["revise-task", "--run-id", "rid-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", hostilePath, "--config-root", "/abs/config"],
    io,
  );
  expect(exit).toBe(2);
  expect(facadeCalls).toBe(0);
  expect(resolverCalls).toBe(2);
  // the path, the hostile error text and the errno code are absent
  expect(err.join("\n")).not.toContain(hostilePath);
  expect(err.join("\n")).not.toContain("/hostile/machine/path");
  expect(err.join("\n")).not.toContain("EACCES");
  expect(err.join("\n")).not.toContain("permission denied");
  expect(err.join("\n")).toContain("revise-task could not read the task file");
  expect(err.join("\n")).toContain("usage: orchestrator");

  // an empty file: the same contract-error shape, content-free
  const { io: io2, err: err2 } = makeIo();
  let facadeCalls2 = 0;
  io2.revisePipelineV2Task = (async () => {
    facadeCalls2 += 1;
    return runOutcome({});
  }) as unknown as CliIo["revisePipelineV2Task"];
  io2.readTaskFile = async () => "";
  const exit2 = await runCli(
    ["revise-task", "--run-id", "rid-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/empty.md", "--config-root", "/abs/config"],
    io2,
  );
  expect(exit2).toBe(2);
  expect(facadeCalls2).toBe(0);
  expect(err2.join("\n")).not.toContain("/abs/empty.md");
  expect(err2.join("\n")).toContain("revise-task requires a non-empty task body");
  expect(err2.join("\n")).toContain("usage: orchestrator");
});

test("revise-task human mode prints the content-free summary lines", async () => {
  const { io, err, out } = makeIo();
  io.revisePipelineV2Task = (async () => runOutcome({})) as unknown as CliIo["revisePipelineV2Task"];
  io.readTaskFile = async () => "BODY";
  const exit = await runCli(REVISE_TASK_ARGS, io);
  expect(exit).toBe(0);
  expect(out).toEqual([]);
  expect(err).toEqual([
    "orchestrator: revise-task ok (run rid-1, state /state/root/pipeline-runs/rid-1/state.json, outputs /state/root/pipeline-runs/rid-1/outputs)",
  ]);

  const { io: io2, err: err2 } = makeIo();
  io2.revisePipelineV2Task = (async () =>
    runOutcome({ ok: false, exitCode: 1, reason: "invalid_state" })) as unknown as CliIo["revisePipelineV2Task"];
  io2.readTaskFile = async () => "BODY";
  const exit2 = await runCli(REVISE_TASK_ARGS, io2);
  expect(exit2).toBe(1);
  expect(err2).toEqual([
    "orchestrator: revise-task failed (run rid-1, reason invalid_state, state /state/root/pipeline-runs/rid-1/state.json)",
  ]);

  // a pre-run-root refusal prints no summary line
  const { io: io3, err: err3 } = makeIo();
  io3.revisePipelineV2Task = (async () =>
    runOutcome({ ok: false, exitCode: 1, runId: "", runRoot: null })) as unknown as CliIo["revisePipelineV2Task"];
  io3.readTaskFile = async () => "BODY";
  const exit3 = await runCli(REVISE_TASK_ARGS, io3);
  expect(exit3).toBe(1);
  expect(err3).toEqual([]);
});

test("revise-task JSON mode prints exactly one outcome document on stdout", async () => {
  const { io, out, err } = makeIo();
  const result = runOutcome({ ok: false, exitCode: 1, reason: "run_input_modified" });
  io.revisePipelineV2Task = (async () => {
    io.writeError("orchestrator: launcher credential ok (launcher dhl_l1)");
    return result;
  }) as unknown as CliIo["revisePipelineV2Task"];
  io.readTaskFile = async () => "BODY";
  const exit = await runCli([...REVISE_TASK_ARGS, "--json"], io);
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

test("revise-task JSON mode maps only the inherit-asked calls to the streaming stderr mode", async () => {
  const { io, out, runnerCalls } = makeIo();
  io.revisePipelineV2Task = (async (_options: unknown, deps: unknown) => {
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
  }) as unknown as CliIo["revisePipelineV2Task"];
  io.readTaskFile = async () => "BODY";
  await runCli([...REVISE_TASK_ARGS, "--json"], io);
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

test("revise-task exit codes 0/1/130/143 pass through unchanged", async () => {
  for (const code of [0, 1, 130, 143]) {
    const { io } = makeIo();
    io.revisePipelineV2Task = (async () =>
      runOutcome({ ok: code === 0, exitCode: code })) as unknown as CliIo["revisePipelineV2Task"];
    io.readTaskFile = async () => "BODY";
    const exit = await runCli(REVISE_TASK_ARGS, io);
    expect(exit).toBe(code);
  }
});

test("revise-task deps.cli is wired to the single runner instance", async () => {
  const { io, out, err, runnerCalls } = makeIo();
  io.revisePipelineV2Task = (async (_options: unknown, deps: unknown) => {
    const cli = (deps as Record<string, unknown>).cli as (
      args: string[],
      env: Record<string, string>,
      stdio: CliStdio,
      opts?: CliRunOptions,
    ) => Promise<CliResult>;
    await cli(["session", "list"], {}, "capture");
    return runOutcome({});
  }) as unknown as CliIo["revisePipelineV2Task"];
  io.readTaskFile = async () => "BODY";
  const exit = await runCli(REVISE_TASK_ARGS, io);
  expect(exit).toBe(0);
  expect(runnerCalls).toHaveLength(1);
  expect(runnerCalls[0]!.args).toEqual(["session", "list"]);
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
  expect(out).toEqual([]);
});

test("the revise-task command appears in the command list and unknown commands are still rejected", async () => {
  const { io, err } = makeIo();
  const exit = await runCli(["deploy"], io);
  expect(exit).toBe(2);
  expect(err.join("\n")).toContain("'orchestrator revise-task'");
});

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

test("respond end-to-end with the real production module: a reserved intervention action exits 1 with no orphan file and no machinery", async () => {
  const root = mkdtempSync(join(tmpdir(), "cli-respond-reserved-"));
  const stateRoot = join(root, "state");
  mkdirSync(stateRoot, { recursive: true });
  const sink = new PipelineV2RunStateSink({
    stateRoot,
    runId: "rid-reserved",
    now: () => new Date(0),
  });
  await sink.dispatch({
    kind: "create_run",
    runId: "rid-reserved",
    pipeline: {
      schema_version: 2,
      bundle_root: join(root, "bundle"),
      execution_snapshot_sha256: createHash("sha256").update("bundle").digest("hex"),
      entry_state: "s01",
      max_transitions: 20,
    },
    inputs: [],
  });
  const runRoot = join(stateRoot, "pipeline-runs", "rid-reserved");
  chmodSync(runRoot, 0o700);
  await enterPipelineV2Wait({
    runRoot,
    sink,
    reason: "stage_iteration_limit_exhausted",
    actions: [{ id: "continue_stage", to: "s01" }],
  });
  const stateBefore = await import("node:fs/promises").then((m) => m.readFile(join(runRoot, "state.json"), "utf8"));
  let runnerCalls = 0;
  let fetchAuthCalls = 0;
  const { io, out, err } = makeIo();
  io.runner.run = () => {
    runnerCalls += 1;
    return Promise.resolve({ code: 0 });
  };
  io.fetchAuth = () => {
    fetchAuthCalls += 1;
    return Promise.resolve({ status: 200, body: {} });
  };
  io.resolveStateRootProjection = () => ({ localRoot: stateRoot, daemonRoot: stateRoot });
  io.respondPipelineV2Wait = respondPipelineV2Wait as unknown as CliIo["respondPipelineV2Wait"];
  const exit = await runCli(
    ["respond", "--run-id", "rid-reserved", "--wait-index", "1", "--action", "continue_stage", "--json"],
    io,
  );
  expect(exit).toBe(1);
  // exactly one JSON outcome document on stdout: the ordinary invalid_state failure
  const documents = out.filter((line) => line.trim() !== "");
  expect(documents).toHaveLength(1);
  const parsed = JSON.parse(documents[0]!) as Record<string, unknown>;
  expect(parsed["ok"]).toBe(false);
  expect(parsed["exitCode"]).toBe(1);
  expect(parsed["reason"]).toBe("invalid_state");
  expect(parsed["runId"]).toBe("rid-reserved");
  expect(parsed["runRoot"]).toBe(runRoot);
  const failedState = parsed["state"] as Record<string, unknown>;
  expect(failedState["status"]).toBe("waiting");
  expect(failedState["failure"]).toBeUndefined();
  // no response manifest was published and the durable state is untouched
  const files = rmSync;
  void files;
  const { readdirSync: rds } = await import("node:fs");
  expect(rds(join(runRoot, "waits")).filter((name) => name.includes("response"))).toEqual([]);
  const stateAfter = await import("node:fs/promises").then((m) => m.readFile(join(runRoot, "state.json"), "utf8"));
  expect(stateAfter).toBe(stateBefore);
  // no runner subprocess, no auth machinery, no signal registration
  expect(runnerCalls).toBe(0);
  expect(fetchAuthCalls).toBe(0);
  expect(err.join("\n")).not.toContain(CANARY_SECRET);
  rmSync(root, { recursive: true, force: true });
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

// --- the real continue-stage CLI integration proof ---------------------------

const STAGE_PIPELINE = `
schema_version: 2
entry_state: architect
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev_entry
  execution_roles:
    - state_id: architect
      role: planning
      plan_output: plan
    - state_id: dev_entry
      role: stage
      stage_template: development

states:
  - id: architect
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_entry
  - id: dev_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect
  - id: done
    type: terminal
    result: success
`;

// --- the real revise-task end-to-end proof -----------------------------------

test("revise-task end-to-end with the real production runner: the honest waiting prefix, the exact eleven-command suffix and the task revision digest of the exact file body", async () => {
  const root = mkdtempSync(join(tmpdir(), "cli-revise-task-"));
  try {
    const bundle = join(root, "bundle");
    mkdirSync(join(bundle, "prompts"), { recursive: true });
    mkdirSync(join(bundle, "schemas"), { recursive: true });
    writeFileSync(join(bundle, "pipeline.yaml"), STAGE_PIPELINE);
    writeFileSync(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    writeFileSync(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
    const configRoot = join(root, "config");
    mkdirSync(join(configRoot, "profiles"), { recursive: true });
    mkdirSync(join(configRoot, "opencode"), { recursive: true });
    writeFileSync(
      join(configRoot, "profiles", "coder.yaml"),
      [
        "schema_version: 1",
        "image: ghcr.io/example/worker:1",
        "opencode_config: opencode/coder.json",
        "env:",
        "  MODEL_API_KEY:",
        "    from_env: CODER_SOURCE_VAR_1",
        "    required: true",
        "",
      ].join("\n"),
    );
    writeFileSync(join(configRoot, "opencode", "coder.json"), JSON.stringify({ model: "glm53-flash" }));
    const sources = join(root, "userdata");
    mkdirSync(sources, { recursive: true });
    writeFileSync(join(sources, "task.md"), "TASK-BODY\n");
    const projectSource = join(root, "project-source");
    mkdirSync(projectSource, { recursive: true });
    const stateRoot = join(root, "state");
    mkdirSync(stateRoot, { recursive: true });
    const credDir = join(root, "cred", "docker-helper");
    mkdirSync(credDir, { recursive: true, mode: 0o700 });
    const credentialFile = join(credDir, "credential.token");
    writeFileSync(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
    const RUN_ID = "revise-task-run";
    const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
    mkdirSync(join(stateRoot, "pipeline-runs"), { mode: 0o700 });
    mkdirSync(runRoot, { mode: 0o700 });

    // the real task-body file: exact content with whitespace and a final
    // newline, passed to the runner byte-for-byte
    const TASK_BODY_FILE = join(root, "revised-task.md");
    const TASK_BODY = "REVISED-CLI-TASK-BODY\n\n  with trailing spaces  \n";
    writeFileSync(TASK_BODY_FILE, TASK_BODY);

    let clockValue = 0;
    const nextTick = (): Date => {
      clockValue += 1;
      return new Date(Date.UTC(2026, 0, 1, 0, 0, clockValue));
    };

    // the honest waiting prefix through the production facades only
    const pipeline = await loadPipelineV2(bundle);
    const sink = new PipelineV2RunStateSink({ stateRoot, runId: RUN_ID, now: nextTick });
    await prepareRunProject(projectSource, runRoot);
    const runInputs: RunInputsSnapshot = await snapshotRunInputs(
      pipeline,
      [{ id: "task", path: join(sources, "task.md") }] as readonly RunInputBinding[],
      runRoot,
    );
    await sink.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(pipeline),
      inputs: runInputs.inputs.map((entry) => ({
        id: entry.id,
        type: entry.type,
        protected: entry.protected,
        digest: entry.digest,
      })),
    });
    const accepted: AcceptedStateOutput[] = [];
    const runAgentStep = async (stateId: string, executionIndex: number, commit: boolean): Promise<void> => {
      const activation: PreparedActivationData = await prepareActivationData(
        pipeline,
        runInputs,
        accepted,
        stateId,
        executionIndex,
      );
      await sink.dispatch({
        kind: "start_agent_execution",
        stateId,
        profile: "coder",
        ...startRoleArgs(pipeline, stateId, sink.snapshot),
      });
      await sink.dispatch({ kind: "agent_data_prepared" });
      await sink.dispatch({ kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` });
      await sink.dispatch({ kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` });
      await sink.dispatch({ kind: "agent_running" });
      if (stateId === "architect") {
        writeFileSync(join(activation.outputs_root, "plan"), "{}", { mode: 0o600 });
      }
      const records = await acceptActivationOutputs(pipeline, activation);
      await sink.dispatch({
        kind: "agent_outputs_accepted",
        outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
      });
      await sink.dispatch({ kind: "agent_cleanup_completed" });
      if (commit) {
        await sink.dispatch({
          kind: "transition_committed",
          step: { from: stateId, outcome: "completed", to: stateId === "architect" ? "dev_entry" : "architect", transition_index: 0 },
          executionIndex,
        });
      }
      accepted.push(...records);
    };
    await runAgentStep("architect", 1, false);
    const taskA = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: RUN_ID,
      task_id: "task-a",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: "PLAN-TASK-BODY",
    });
    const plan1 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: runInputs.inputs[0]?.digest ?? "" },
      origin_execution: 1,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: taskA.sha256, depends_on: [] }],
        },
      ],
    });
    const candidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [taskA],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: runInputs.inputs[0]?.digest ?? "",
    });
    const acceptedPlan = await acceptPipelineV2RunPlanCandidate({
      pipeline,
      runRoot,
      sink,
      candidate,
    });
    await ensurePipelineV2StageIteration({
      compiledPlan: acceptedPlan.compiled_plan,
      stageId: "stage-1",
      initialBudget: 2,
      sink,
    });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await runAgentStep("dev_entry", 2, true);
    await enterPipelineV2Wait({
      runRoot,
      sink,
      reason: "stage_iteration_limit_exhausted",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    const prefixState = parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));

    // the real CLI call with the real production module and the real file
    // read: no in-memory pipeline, compiled plan or intent is passed - the
    // CLI options are the only inputs
    const { io, out, err } = makeIo();
    io.baseEnv = { ...io.baseEnv, CODER_SOURCE_VAR_1: "tester-secret" };
    let sessionCreates = 0;
    const sessionDeletes: string[] = [];
    io.runner.run = async (args: string[]) => {
      if (args[0] === "session" && args[1] === "create") {
        sessionCreates += 1;
        return {
          code: 0,
          stdout: JSON.stringify({
            ok: true,
            session: { id: `dhs_${sessionCreates}`, launcher_id: "dhl_revise" },
            token: `dhc_${sessionCreates}`,
          }),
        };
      }
      if (args[0] === "session" && args[1] === "delete") {
        sessionDeletes.push(args[args.length - 1] as string);
        return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
      }
      if (args[0] === "pull") {
        return { code: 1, stderr: "PULL-FAILED-BY-TEST" };
      }
      return { code: 0 };
    };
    io.resolveStateRootProjection = () => ({ localRoot: stateRoot, daemonRoot: stateRoot });
    io.resolveHelperConfig = () => ({ socketPath: "/run/dh.sock", credentialFile });
    io.fetchAuth = () =>
      Promise.resolve({
        status: 200,
        body: { authority: "launcher", principal: "tester", launcher_id: "dhl_revise" },
      });
    io.revisePipelineV2Task = revisePipelineV2Task as unknown as CliIo["revisePipelineV2Task"];
    // the production default reader (one UTF-8 read of the file)
    io.readTaskFile = (path: string) => readFile(path, "utf8");
    const exit = await runCli(
      [
        "revise-task",
        "--run-id", RUN_ID,
        "--wait-index", "1",
        "--task-id", "task-a",
        "--task-file", TASK_BODY_FILE,
        "--config-root", configRoot,
        "--launcher-id", "dhl_revise",
        "--json",
      ],
      io,
    );

    expect(exit).toBe(1);
    // exactly one JSON outcome on stdout
    const documents = out.filter((line) => line.trim() !== "");
    expect(documents).toHaveLength(1);
    const parsed = JSON.parse(documents[0]!) as Record<string, unknown>;
    expect(parsed["ok"]).toBe(false);
    expect(parsed["exitCode"]).toBe(1);
    expect(parsed["reason"]).toBe("worker_failed");
    expect(parsed["runId"]).toBe(RUN_ID);
    expect(parsed["runRoot"]).toBe(runRoot);
    // ordinary worker failure, never a refusal and never a pipeline mismatch
    expect(parsed["reason"]).not.toBe("pipeline_mismatch");
    expect(JSON.stringify(parsed)).not.toContain("refused");
    // the body and the file path never appear in the outcome or diagnostics
    expect(err.join("\n")).not.toContain("userdata");
    expect(err.join("\n")).not.toContain("project-source");
    expect(err.join("\n")).not.toContain(TASK_BODY);
    expect(err.join("\n")).not.toContain(TASK_BODY_FILE);
    expect(JSON.stringify(parsed)).not.toContain(TASK_BODY);
    expect(JSON.stringify(parsed)).not.toContain(TASK_BODY_FILE);
    expect(JSON.stringify(parsed)).not.toContain("revised-task.md");

    // the durable projection of the exact eleven-command suffix:
    // plan_intent_accepted -> task_revision_accepted -> stage_iteration_closed
    // -> wait_response_recorded, then the resumed planning execution 3
    // (architect, planning, no iteration index) and the ordinary worker_failed
    // failure finalization
    const state = parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8"));
    const wait0 = state.waits[0]!;
    expect(wait0.index).toBe(1);
    expect(wait0.intent?.intent_sha256).toBeDefined();
    expect(wait0.response?.action_id).toBe("revise_task");
    // the task revision carries the digest of exactly the unchanged file
    // body: the expected revision-2 manifest over the exact file content
    const taskA2 = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: RUN_ID,
      task_id: "task-a",
      revision: 2,
      previous_sha256: taskA.sha256,
      origin: "user_response",
      body: TASK_BODY,
    });
    expect(state.task_revisions.map((record) => `${record.task_id}@${record.revision}`)).toEqual(["task-a@1", "task-a@2"]);
    expect(state.task_revisions[1]!.sha256).toBe(taskA2.sha256);
    expect(state.task_revisions[1]!.previous_sha256).toBe(taskA.sha256);
    const generation = state.generations[0]!;
    expect(generation.stage_id).toBe("stage-1");
    expect(generation.closed).toBeUndefined();
    expect(generation.iterations[0]?.closed).toEqual({ by: "replanned", wait_index: 1, closed_transition_count: 2 });
    expect(generation.open_iteration).toBeUndefined();
    expect(state.cursor).toEqual({ current_state: "architect", transition_count: 2 });
    expect(state.transitions).toHaveLength(2);
    const execution3 = state.executions[2];
    expect(execution3).toMatchObject({
      index: 3,
      state_id: "architect",
      execution_role: "planning",
      phase: "failed",
      failure_reason: "worker_failed",
    });
    expect(execution3?.iteration_index).toBeUndefined();
    if (execution3?.type !== "agent") {
      throw new Error("expected the resumed execution to be an agent execution");
    }
    expect(execution3.session_cleanup).toEqual({ execution: "completed", tool: "completed" });
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "worker_failed" });
    // one session pair only (the resumed execution), cleaned exactly once
    // each and tool-first
    expect(sessionCreates).toBe(2);
    expect(sessionDeletes).toEqual(["dhs_2", "dhs_1"]);
    // the task body never enters the durable state
    expect(JSON.stringify(state)).not.toContain(TASK_BODY);
    // exactly eleven durable commits: four intervention + seven resume
    expect(state.revision).toBe(prefixState.revision + 11);
    // the loader round-trip
    expect(state).toEqual(parsePipelineV2RunState(await readFile(join(runRoot, "state.json"), "utf8")));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("continue-stage end-to-end with the real production runner: the honest waiting prefix, the full intervention suffix and the resumed successor execution", async () => {
  const root = mkdtempSync(join(tmpdir(), "cli-continue-stage-"));
  try {
    const bundle = join(root, "bundle");
    mkdirSync(join(bundle, "prompts"), { recursive: true });
    mkdirSync(join(bundle, "schemas"), { recursive: true });
    writeFileSync(join(bundle, "pipeline.yaml"), STAGE_PIPELINE);
    writeFileSync(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    writeFileSync(join(bundle, "prompts", "coder.md"), "IMPLEMENT-THE-TASK\n");
    const configRoot = join(root, "config");
    mkdirSync(join(configRoot, "profiles"), { recursive: true });
    mkdirSync(join(configRoot, "opencode"), { recursive: true });
    writeFileSync(
      join(configRoot, "profiles", "coder.yaml"),
      [
        "schema_version: 1",
        "image: ghcr.io/example/worker:1",
        "opencode_config: opencode/coder.json",
        "env:",
        "  MODEL_API_KEY:",
        "    from_env: CODER_SOURCE_VAR_1",
        "    required: true",
        "",
      ].join("\n"),
    );
    writeFileSync(join(configRoot, "opencode", "coder.json"), JSON.stringify({ model: "glm53-flash" }));
    const sources = join(root, "userdata");
    mkdirSync(sources, { recursive: true });
    writeFileSync(join(sources, "task.md"), "TASK-BODY\n");
    const projectSource = join(root, "project-source");
    mkdirSync(projectSource, { recursive: true });
    const stateRoot = join(root, "state");
    mkdirSync(stateRoot, { recursive: true });
    const credDir = join(root, "cred", "docker-helper");
    mkdirSync(credDir, { recursive: true, mode: 0o700 });
    const credentialFile = join(credDir, "credential.token");
    writeFileSync(credentialFile, "cred-token-not-real\n", { mode: 0o600 });
    const RUN_ID = "continue-run";
    const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
    mkdirSync(join(stateRoot, "pipeline-runs"), { mode: 0o700 });
    mkdirSync(runRoot, { mode: 0o700 });

    let clockValue = 0;
    const nextTick = (): Date => {
      clockValue += 1;
      return new Date(Date.UTC(2026, 0, 1, 0, 0, clockValue));
    };

    // the honest waiting prefix through the production facades only
    const pipeline = await loadPipelineV2(bundle);
    const sink = new PipelineV2RunStateSink({ stateRoot, runId: RUN_ID, now: nextTick });
    await prepareRunProject(projectSource, runRoot);
    const runInputs: RunInputsSnapshot = await snapshotRunInputs(
      pipeline,
      [{ id: "task", path: join(sources, "task.md") }] as readonly RunInputBinding[],
      runRoot,
    );
    await sink.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: pipelineV2RunPipelineIdentity(pipeline),
      inputs: runInputs.inputs.map((entry) => ({
        id: entry.id,
        type: entry.type,
        protected: entry.protected,
        digest: entry.digest,
      })),
    });
    const accepted: AcceptedStateOutput[] = [];
    const runAgentStep = async (stateId: string, executionIndex: number, commit: boolean): Promise<void> => {
      const activation: PreparedActivationData = await prepareActivationData(
        pipeline,
        runInputs,
        accepted,
        stateId,
        executionIndex,
      );
      await sink.dispatch({
        kind: "start_agent_execution",
        stateId,
        profile: "coder",
        ...startRoleArgs(pipeline, stateId, sink.snapshot),
      });
      await sink.dispatch({ kind: "agent_data_prepared" });
      await sink.dispatch({ kind: "agent_execution_session_created", sessionId: `exec-${executionIndex}` });
      await sink.dispatch({ kind: "agent_tool_session_created", sessionId: `tool-${executionIndex}` });
      await sink.dispatch({ kind: "agent_running" });
      if (stateId === "architect") {
        writeFileSync(join(activation.outputs_root, "plan"), "{}", { mode: 0o600 });
      }
      const records = await acceptActivationOutputs(pipeline, activation);
      await sink.dispatch({
        kind: "agent_outputs_accepted",
        outputs: records.map((record) => ({ id: record.output, digest: record.digest })),
      });
      await sink.dispatch({ kind: "agent_cleanup_completed" });
      if (commit) {
        await sink.dispatch({
          kind: "transition_committed",
          step: { from: stateId, outcome: "completed", to: stateId === "architect" ? "dev_entry" : "architect", transition_index: 0 },
          executionIndex,
        });
      }
      accepted.push(...records);
    };
    await runAgentStep("architect", 1, false);
    const taskA = prepareTaskRevisionManifest({
      schema_version: 1,
      kind: "task_revision",
      run_id: RUN_ID,
      task_id: "task-a",
      revision: 1,
      previous_sha256: null,
      origin: "planning_proposal",
      body: "PLAN-TASK-BODY",
    });
    const plan1 = preparePlanRevisionManifest({
      schema_version: 1,
      kind: "plan_revision",
      run_id: RUN_ID,
      revision: 1,
      previous_sha256: null,
      root_task: { input_id: "task", sha256: runInputs.inputs[0]?.digest ?? "" },
      origin_execution: 1,
      stages: [
        {
          id: "stage-1",
          template: "development",
          tasks: [{ id: "task-a", revision: 1, sha256: taskA.sha256, depends_on: [] }],
        },
      ],
    });
    const candidate = preparePipelineV2RunPlanCandidate({
      plan: plan1,
      taskRevisions: [taskA],
      previousPlan: null,
      previousTaskRevisions: [],
      protectedInputDigest: runInputs.inputs[0]?.digest ?? "",
    });
    const acceptedPlan = await acceptPipelineV2RunPlanCandidate({
      pipeline,
      runRoot,
      sink,
      candidate,
    });
    await ensurePipelineV2StageIteration({
      compiledPlan: acceptedPlan.compiled_plan,
      stageId: "stage-1",
      initialBudget: 2,
      sink,
    });
    await sink.dispatch({
      kind: "transition_committed",
      step: { from: "architect", outcome: "completed", to: "dev_entry", transition_index: 0 },
      executionIndex: 1,
    });
    await runAgentStep("dev_entry", 2, true);
    await enterPipelineV2Wait({
      runRoot,
      sink,
      reason: "stage_iteration_limit_exhausted",
      actions: [
        { id: "continue_stage", to: "dev_entry" },
        { id: "revise_task", to: "architect" },
      ],
    });
    const prefixState = parsePipelineV2RunState(
      await import("node:fs/promises").then((m) => m.readFile(join(runRoot, "state.json"), "utf8")),
    );

    // the real CLI call: no in-memory pipeline, compiled plan or intent is
    // passed - the four external scalars are the only inputs
    const { io, out, err } = makeIo();
    io.baseEnv = { ...io.baseEnv, CODER_SOURCE_VAR_1: "tester-secret" };
    let sessionCreates = 0;
    io.runner.run = async (args: string[]) => {
      if (args[0] === "session" && args[1] === "create") {
        sessionCreates += 1;
        return {
          code: 0,
          stdout: JSON.stringify({
            ok: true,
            session: { id: `dhs_${sessionCreates}`, launcher_id: "dhl_continue" },
            token: `dhc_${sessionCreates}`,
          }),
        };
      }
      if (args[0] === "session" && args[1] === "delete") {
        return { code: 0, stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] }) };
      }
      if (args[0] === "pull") {
        return { code: 1, stderr: "PULL-FAILED-BY-TEST" };
      }
      return { code: 0 };
    };
    io.resolveStateRootProjection = () => ({ localRoot: stateRoot, daemonRoot: stateRoot });
    io.resolveHelperConfig = () => ({ socketPath: "/run/dh.sock", credentialFile });
    io.fetchAuth = () =>
      Promise.resolve({
        status: 200,
        body: { authority: "launcher", principal: "tester", launcher_id: "dhl_continue" },
      });
    io.continuePipelineV2Stage = continuePipelineV2Stage as unknown as CliIo["continuePipelineV2Stage"];
    const exit = await runCli(
      [
        "continue-stage",
        "--run-id", RUN_ID,
        "--wait-index", "1",
        "--additional-iterations", "2",
        "--config-root", configRoot,
        "--launcher-id", "dhl_continue",
        "--json",
      ],
      io,
    );

    expect(exit).toBe(1);
    const documents = out.filter((line) => line.trim() !== "");
    expect(documents).toHaveLength(1);
    const parsed = JSON.parse(documents[0]!) as Record<string, unknown>;
    expect(parsed["ok"]).toBe(false);
    expect(parsed["exitCode"]).toBe(1);
    expect(parsed["reason"]).toBe("worker_failed");
    expect(parsed["runId"]).toBe(RUN_ID);
    expect(parsed["runRoot"]).toBe(runRoot);
    expect(err.join("\n")).not.toContain("userdata");
    expect(err.join("\n")).not.toContain("project-source");
    expect(err.join("\n")).not.toContain(CANARY_SECRET);
    expect(JSON.stringify(parsed)).not.toContain("TASK-BODY");

    // the durable projection: the exact intervention suffix
    // plan_intent_accepted -> iteration_grant_recorded -> stage_iteration_closed
    // -> wait_response_recorded -> stage_iteration_opened, then the successor
    // execution start in dev_entry (role stage, iteration 2) and the ordinary
    // worker_failed failure finalization
    const state = parsePipelineV2RunState(
      await import("node:fs/promises").then((m) => m.readFile(join(runRoot, "state.json"), "utf8")),
    );
    const wait0 = state.waits[0]!;
    expect(wait0.index).toBe(1);
    expect(wait0.intent?.intent_sha256).toBeDefined();
    expect(wait0.response?.action_id).toBe("continue_stage");
    expect(state.grants).toHaveLength(1);
    expect(state.grants[0]).toMatchObject({
      generation_index: 1,
      wait_index: 1,
      additional_iterations: 2,
      intent_sha256: wait0.intent?.intent_sha256,
    });
    const generation = state.generations[0]!;
    expect(generation.stage_id).toBe("stage-1");
    expect(generation.closed).toBeUndefined();
    expect(generation.iterations[0]?.closed).toEqual({ by: "grant", wait_index: 1, closed_transition_count: 2 });
    expect(generation.open_iteration).toEqual({ index: 2, opened_transition_count: 2 });
    const execution3 = state.executions[2];
    expect(execution3).toMatchObject({
      index: 3,
      state_id: "dev_entry",
      execution_role: "stage",
      iteration_index: 2,
      phase: "failed",
    });
    expect(state.status).toBe("failed");
    expect(state.failure).toEqual({ reason: "worker_failed" });
    // exactly twelve durable commits: five intervention + seven resume
    expect(state.revision).toBe(prefixState.revision + 12);
    // the loader round-trip
    expect(state).toEqual(
      parsePipelineV2RunState(
        await import("node:fs/promises").then((m) => m.readFile(join(runRoot, "state.json"), "utf8")),
      ),
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
