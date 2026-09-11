/**
 * The orchestrator CLI. Three commands are routed here:
 *
 * - `smoke` and `agent-smoke` — the v1 diagnostic commands (unchanged);
 * - `run` — the single production entry into pipeline v2: the CLI only
 *   parses arguments, resolves the trusted state-root projection from the
 *   environment, assembles the per-call dependencies and invokes
 *   `runPipelineV2` exactly once, then reports the outcome. Every
 *   pipeline/profile/auth/run-root/sink/runtime/coordinator decision lives
 *   inside the runner; the CLI creates no second gate, no Sessions, no
 *   mounts and no Docker Helper argv of its own.
 *
 * The dispatcher is testable through per-call dependency injection
 * (`runCli(argv, io)`); no module-global mutable state exists.
 */
import {
  fetchAuthOverSocket,
  type AuthFetcher,
  type CliRunOptions,
  type CliRunner,
  type CliStdio,
  SubprocessCliRunner,
} from "./docker_helper.ts";
import { resolveHelperConfig, type HelperConfig } from "./launcher.ts";
import { runAgentSmoke, type AgentSmokeOptions } from "./agent_smoke.ts";
import { runSmoke } from "./smoke.ts";
import { parseCommand, usage } from "./cli_args.ts";
import type { LifecycleDeps } from "./lifecycle.ts";
import {
  resolvePipelineV2StateRootProjection,
} from "./pipeline_v2_state_root.ts";
import {
  runPipelineV2,
  type PipelineV2RunOutcome,
  type PipelineV2RunnerDeps,
} from "./pipeline_v2_runner.ts";

/**
 * The per-call seam of the dispatcher. The production default builds one
 * `SubprocessCliRunner` per invocation; tests inject recorders.
 */
export interface CliIo {
  baseEnv: Readonly<Record<string, string | undefined>>;
  runner: {
    run: CliRunner;
    killActive: (signal: "SIGINT" | "SIGTERM") => boolean;
  };
  fetchAuth: AuthFetcher;
  resolveHelperConfig: (env: Readonly<Record<string, string | undefined>>) => HelperConfig;
  resolveStateRootProjection: typeof resolvePipelineV2StateRootProjection;
  runPipelineV2: typeof runPipelineV2;
  runSmoke: typeof runSmoke;
  runAgentSmoke: typeof runAgentSmoke;
  /** One raw write to the real stdout; the caller adds the trailing newline. */
  writeStdout: (text: string) => void;
  /** One line to the real stderr (a trailing newline is added). */
  writeError: (text: string) => void;
}

function productionCliIo(): CliIo {
  const runner = new SubprocessCliRunner();
  return {
    baseEnv: process.env,
    runner: {
      run: (args, env, stdio, opts) => runner.run(args, env, stdio, opts),
      killActive: (signal) => runner.killActive(signal),
    },
    fetchAuth: fetchAuthOverSocket,
    resolveHelperConfig: resolveHelperConfig,
    resolveStateRootProjection: resolvePipelineV2StateRootProjection,
    runPipelineV2,
    runSmoke,
    runAgentSmoke,
    writeStdout: (text) => process.stdout.write(text),
    writeError: (text) => console.error(text),
  };
}

/**
 * The shared v1 signal wiring: the first SIGINT/SIGTERM is forwarded to a
 * running worker `run` CLI process (signalable only) via `killActive`, and
 * the caller's handler records a lifecycle abort only when the signal may
 * still be classified as a user abort. One registration per process.
 */
function v1SignalRegistration(
  io: CliIo,
  handler: (signal: "SIGINT" | "SIGTERM") => void,
): void {
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.on(signal, () => {
      // First-wins: forward the signal to a running worker `run` CLI
      // process (signalable only) and record a lifecycle abort unless a
      // runner timeout already claimed the active worker. killActive
      // answers whether this signal may still be classified as a user
      // abort.
      if (io.runner.killActive(signal)) {
        handler(signal);
      }
    });
  }
}

const COMMANDS = ["smoke", "agent-smoke", "run"] as const;

export async function runCli(argv: readonly string[], io: CliIo = productionCliIo()): Promise<number> {
  const command = argv[0];

  if (command !== "smoke" && command !== "agent-smoke" && command !== "run") {
    const expected = COMMANDS.map((name) => `'orchestrator ${name}'`).join(", ");
    const suffix = command !== undefined ? `, got ${JSON.stringify(command)}` : "";
    io.writeError(`error: expected ${expected}${suffix}`);
    io.writeError(usage());
    return 2;
  }

  let parsed;
  try {
    parsed = parseCommand(command, argv.slice(1));
  } catch (cause) {
    io.writeError(`error: ${cause instanceof Error ? cause.message : String(cause)}`);
    io.writeError(usage());
    return 2;
  }

  if (parsed.kind === "run") {
    // The production pipeline v2 command resolves its whole CLI
    // configuration inside one protected boundary (state-root projection
    // first, then the helper configuration); the shared v1 helper-config
    // resolution below runs only for the v1 commands, whose observable
    // behavior stays unchanged.
    return await runPipelineV2Command(parsed, io);
  }

  const config = io.resolveHelperConfig(io.baseEnv);

  const deps: LifecycleDeps = {
    cli: (args, env, stdio, opts) => io.runner.run(args, env, stdio, opts),
    fetchAuth: io.fetchAuth,
    config,
    baseEnv: io.baseEnv,
    onSignal: (handler) => {
      v1SignalRegistration(io, handler);
    },
  };

  if (parsed.kind === "smoke") {
    const outcome = await io.runSmoke(
      {
        workspace: parsed.workspace,
        workerImage: parsed.workerImage,
        launcherId: parsed.launcherId,
      },
      deps,
    );
    if (outcome.ok) {
      io.writeError(
        `orchestrator: smoke ok (run ${outcome.runId}, session ${outcome.sessionId ?? "?"}, status ${outcome.status})`,
      );
    }
    return outcome.exitCode;
  }

  const options: AgentSmokeOptions = {
    workspace: parsed.workspace,
    configRoot: parsed.configRoot,
    pipelineRoot: parsed.pipelineRoot,
    launcherId: parsed.launcherId,
  };
  const outcome = await io.runAgentSmoke(options, deps);
  if (outcome.ok) {
    io.writeError(
      `orchestrator: agent-smoke ok (run ${outcome.runId}, session ${outcome.sessionId ?? "?"}, status ${outcome.status})`,
    );
  }
  return outcome.exitCode;
}

/**
 * The production pipeline v2 command: parse already done. The whole CLI
 * configuration lives inside one protected boundary — the state-root
 * projection is resolved first, then the helper configuration; any failure
 * (including a helper-configuration failure with no usable HOME/
 * XDG_CONFIG_HOME) is a CLI configuration error: exit 2 with the usage
 * text, `runPipelineV2` never called, no auth, no filesystem, no runner
 * subprocess, no signal registration, and `runCli` never rejects. The deps
 * are assembled over the single runner instance and the v1 signal wiring;
 * `runPipelineV2` is invoked exactly once and its outcome reported.
 */
async function runPipelineV2Command(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "run" }>,
  io: CliIo,
): Promise<number> {
  let stateRootProjection;
  let config: HelperConfig;
  try {
    stateRootProjection = io.resolveStateRootProjection(io.baseEnv);
    config = io.resolveHelperConfig(io.baseEnv);
  } catch (cause) {
    io.writeError(`error: ${cause instanceof Error ? cause.message : String(cause)}`);
    io.writeError(usage());
    return 2;
  }

  // In JSON mode the stdout must carry exactly one JSON document, so only
  // the calls the Docker Helper runtime asks to inherit (image pull,
  // worker run) are switched to the streaming stderr mode: the child's
  // stdout and stderr flow directly onto the parent's stderr with no
  // buffering and no post-hoc forwarding. argv, env, options and every
  // runtime decision stay untouched; `capture` calls (structured session
  // answers) stay captured, and in human mode the adapter's own
  // inheritance is kept.
  const cli: CliRunner = parsed.json
    ? (args, env, stdio: CliStdio, opts?: CliRunOptions) =>
        io.runner.run(args, env, stdio === "inherit" ? "stderr" : stdio, opts)
    : (args, env, stdio: CliStdio, opts?: CliRunOptions) => io.runner.run(args, env, stdio, opts);

  const deps: PipelineV2RunnerDeps = {
    cli,
    fetchAuth: io.fetchAuth,
    helperConfig: config,
    baseEnv: io.baseEnv,
    stateRootProjection,
    onSignal: (handler) => {
      v1SignalRegistration(io, handler);
    },
  };

  const outcome: PipelineV2RunOutcome = await io.runPipelineV2(
    {
      pipelineRoot: parsed.pipelineRoot,
      configRoot: parsed.configRoot,
      projectSourcePath: parsed.projectSourcePath,
      inputBindings: parsed.inputBindings,
      launcherId: parsed.launcherId,
    },
    deps,
  );

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    io.writeError(
      `orchestrator: run ok (run ${outcome.runId}, state ${outcome.runRoot}/state.json, outputs ${outcome.runRoot}/outputs)`,
    );
  } else if (outcome.runRoot !== null) {
    const reasonPart = outcome.reason !== undefined ? ` reason ${outcome.reason},` : "";
    io.writeError(
      `orchestrator: run failed (run ${outcome.runId},${reasonPart} state ${outcome.runRoot}/state.json)`,
    );
  }
  // A failure before the run root was created needs no summary line: the
  // runner already printed its content-free diagnostic to stderr.
  return outcome.exitCode;
}

if (import.meta.main) {
  process.exit(await runCli(process.argv.slice(2)));
}
