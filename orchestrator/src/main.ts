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
 * - `resume` — the production continuation of an already durable v2 run:
 *   the same protected CLI-configuration boundary and per-call dependency
 *   assembly, invoking `resumePipelineV2` exactly once. The run id and the
 *   configuration root are the only user inputs; the pipeline, the project
 *   copy, the input snapshot and the accepted outputs come only from the
 *   durable state and the run-owned layout.
 * - `respond` — the production wait-response command: it resolves only the
 *   state-root projection (the same resolver; deliberately no helper
 *   configuration, no auth, no subprocess and no signal registration) and
 *   invokes `respondPipelineV2Wait` exactly once. The run id, the wait
 *   index and the action id are the only user inputs; the routing target
 *   comes only from the durable wait request.
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
  resumePipelineV2,
  runPipelineV2,
  type PipelineV2RunOutcome,
  type PipelineV2RunnerDeps,
} from "./pipeline_v2_runner.ts";
import {
  respondPipelineV2Wait,
  type PipelineV2WaitResponseOutcome,
} from "./pipeline_v2_wait_respond.ts";

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
  resumePipelineV2: typeof resumePipelineV2;
  respondPipelineV2Wait: typeof respondPipelineV2Wait;
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
    resumePipelineV2,
    respondPipelineV2Wait,
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

const COMMANDS = ["smoke", "agent-smoke", "run", "resume", "respond"] as const;

export async function runCli(argv: readonly string[], io: CliIo = productionCliIo()): Promise<number> {
  const command = argv[0];

  if (
    command !== "smoke" &&
    command !== "agent-smoke" &&
    command !== "run" &&
    command !== "resume" &&
    command !== "respond"
  ) {
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
  if (parsed.kind === "resume") {
    // The production pipeline v2 resume command: the same protected
    // CLI-configuration boundary and per-call dependency assembly, then
    // exactly one `resumePipelineV2` invocation.
    return await runPipelineV2ResumeCommand(parsed, io);
  }
  if (parsed.kind === "respond") {
    // The production pipeline v2 wait-response command: it resolves only
    // the state-root projection (no helper configuration, no auth, no
    // subprocess, no signal registration) and invokes the response
    // production API exactly once.
    return await runPipelineV2RespondCommand(parsed, io);
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
/**
 * The shared CLI-configuration boundary of the production pipeline v2
 * commands: the state-root projection is resolved first (the same env
 * resolver both commands use), then the helper configuration; any failure
 * (including a helper-configuration failure with no usable HOME/
 * XDG_CONFIG_HOME) is a CLI configuration error: exit 2 with the usage
 * text, the runner never called, no auth, no filesystem, no runner
 * subprocess, no signal registration, and `runCli` never rejects.
 */
async function resolvePipelineV2CliConfiguration(
  io: CliIo,
): Promise<{ stateRootProjection: Awaited<ReturnType<typeof resolvePipelineV2StateRootProjection>>; config: HelperConfig } | null> {
  try {
    const stateRootProjection = io.resolveStateRootProjection(io.baseEnv);
    const config = io.resolveHelperConfig(io.baseEnv);
    return { stateRootProjection, config };
  } catch (cause) {
    io.writeError(`error: ${cause instanceof Error ? cause.message : String(cause)}`);
    io.writeError(usage());
    return null;
  }
}

/**
 * In JSON mode the stdout must carry exactly one JSON document, so only
 * the calls the Docker Helper runtime asks to inherit (image pull, worker
 * run) are switched to the streaming stderr mode: the child's stdout and
 * stderr flow directly onto the parent's stderr with no buffering and no
 * post-hoc forwarding. argv, env, options and every runtime decision stay
 * untouched; `capture` calls (structured session answers) stay captured,
 * and in human mode the adapter's own inheritance is kept.
 */
function pipelineV2CommandCli(parsedJson: boolean, io: CliIo): CliRunner {
  return parsedJson
    ? (args, env, stdio: CliStdio, opts?: CliRunOptions) =>
        io.runner.run(args, env, stdio === "inherit" ? "stderr" : stdio, opts)
    : (args, env, stdio: CliStdio, opts?: CliRunOptions) => io.runner.run(args, env, stdio, opts);
}

function pipelineV2CommandDeps(
  parsedJson: boolean,
  configuration: { stateRootProjection: PipelineV2RunnerDeps["stateRootProjection"]; config: HelperConfig },
  io: CliIo,
): PipelineV2RunnerDeps {
  return {
    cli: pipelineV2CommandCli(parsedJson, io),
    fetchAuth: io.fetchAuth,
    helperConfig: configuration.config,
    baseEnv: io.baseEnv,
    stateRootProjection: configuration.stateRootProjection,
    onSignal: (handler) => {
      v1SignalRegistration(io, handler);
    },
  };
}

async function runPipelineV2Command(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "run" }>,
  io: CliIo,
): Promise<number> {
  const configuration = await resolvePipelineV2CliConfiguration(io);
  if (configuration === null) {
    return 2;
  }

  const deps = pipelineV2CommandDeps(parsed.json, configuration, io);

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

  return reportPipelineV2Outcome("run", parsed.json, outcome, io);
}

async function runPipelineV2ResumeCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "resume" }>,
  io: CliIo,
): Promise<number> {
  const configuration = await resolvePipelineV2CliConfiguration(io);
  if (configuration === null) {
    return 2;
  }

  const deps = pipelineV2CommandDeps(parsed.json, configuration, io);

  const outcome: PipelineV2RunOutcome = await io.resumePipelineV2(
    {
      runId: parsed.runId,
      configRoot: parsed.configRoot,
      launcherId: parsed.launcherId,
    },
    deps,
  );

  return reportPipelineV2Outcome("resume", parsed.json, outcome, io);
}

/**
 * The production pipeline v2 wait-response command: parse already done.
 * Only the state-root projection is resolved inside the protected
 * CLI-configuration boundary (the same env resolver `run` and `resume`
 * use); the helper configuration is deliberately NOT resolved because the
 * response command carries no Launcher credential, no Docker Helper
 * transport and no signal lifecycle. Any resolution failure is exit 2
 * with the usage text and the production API never called.
 */
async function runPipelineV2RespondCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "respond" }>,
  io: CliIo,
): Promise<number> {
  let stateRootProjection;
  try {
    stateRootProjection = io.resolveStateRootProjection(io.baseEnv);
  } catch (cause) {
    io.writeError(`error: ${cause instanceof Error ? cause.message : String(cause)}`);
    io.writeError(usage());
    return 2;
  }

  const outcome: PipelineV2WaitResponseOutcome = await io.respondPipelineV2Wait(
    {
      runId: parsed.runId,
      waitIndex: parsed.waitIndex,
      actionId: parsed.actionId,
    },
    { stateRootProjection },
  );

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    io.writeError(
      `orchestrator: respond ok (run ${outcome.runId}, wait ${outcome.waitIndex}, action ${outcome.actionId}, to ${outcome.actionTo}, state ${outcome.runRoot}/state.json)`,
    );
  } else if (outcome.runRoot !== null) {
    io.writeError(
      `orchestrator: respond failed (run ${outcome.runId}, reason ${outcome.reason}, state ${outcome.runRoot}/state.json)`,
    );
  } else {
    io.writeError(
      `orchestrator: respond failed (run ${outcome.runId}, reason ${outcome.reason})`,
    );
  }
  return outcome.exitCode;
}

function reportPipelineV2Outcome(
  command: "run" | "resume",
  json: boolean,
  outcome: PipelineV2RunOutcome,
  io: CliIo,
): number {
  if (json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    io.writeError(
      `orchestrator: ${command} ok (run ${outcome.runId}, state ${outcome.runRoot}/state.json, outputs ${outcome.runRoot}/outputs)`,
    );
  } else if (outcome.runRoot !== null) {
    const reasonPart = outcome.reason !== undefined ? ` reason ${outcome.reason},` : "";
    io.writeError(
      `orchestrator: ${command} failed (run ${outcome.runId},${reasonPart} state ${outcome.runRoot}/state.json)`,
    );
  }
  // A failure before the run root was created needs no summary line: the
  // runner already printed its content-free diagnostic to stderr.
  return outcome.exitCode;
}

if (import.meta.main) {
  process.exit(await runCli(process.argv.slice(2)));
}
