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
 * - `continue-stage` — the production `continue_stage` stage intervention:
 *   the same protected CLI-configuration boundary and per-call dependency
 *   assembly, invoking `continuePipelineV2Stage` exactly once. The run id,
 *   the wait index, the additional iteration count and the configuration
 *   root are the only user inputs; the action is fixed by the runner
 *   entrypoint and every internal intervention parameter is derived from
 *   the authoritative durable state.
 * - `revise-task` — the production `revise_task` task revision
 *   intervention: the same protected CLI-configuration boundary, then
 *   exactly one UTF-8 read of the `--task-file` (the revised task body is
 *   never passed inline or through argv; an empty file is a CLI contract
 *   error), the per-call dependency assembly and exactly one
 *   `revisePipelineV2Task` invocation. The run id, the wait index, the
 *   task id, the task-body file and the configuration root are the only
 *   user inputs; the action is fixed by the runner entrypoint and every
 *   internal intervention parameter is derived from the authoritative
 *   durable state. The file path and the body never appear in the
 *   outcome, the durable state or any diagnostic.
 * - `resume-plan` — the production planning continuation: the same
 *   protected CLI-configuration boundary and per-call dependency assembly,
 *   invoking `resumePipelineV2PlanningRunPlan` exactly once. The run id,
 *   the stage id, the initial budget and the configuration root are the
 *   only user inputs; the stage id and the initial budget are explicit
 *   operator policy (never derived from the durable state), the command is
 *   not a respond action and accepts no `--action` flag, and every
 *   internal handoff parameter is derived by the composition controller
 *   from the authoritative durable state. There is no automatic planning
 *   loop and no automatic stage/budget selection.
 * - `inspect` — the read-only versioned run projection: it resolves only
 *   the state-root projection (the same resolver as `respond`; no helper
 *   configuration, no Launcher credential, no auth, no subprocess, no
 *   signal registration and no durable change) and invokes
 *   `inspectPipelineV2Run` exactly once. The run id and the configuration
 *   root are the only user inputs; the projection comes only from the
 *   durable state, the run-owned layout and the verified pipeline. The
 *   dispatcher builds no `SubprocessCliRunner` command dependencies and
 *   no mutating command deps object for it.
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
import type { PipelineV2WorkerOperationRunner } from "./pipeline_v2_docker_runtime.ts";
import { resolveHelperConfig, type HelperConfig } from "./launcher.ts";
import { runAgentSmoke, type AgentSmokeOptions } from "./agent_smoke.ts";
import { runSmoke } from "./smoke.ts";
import { parseCommand, usage } from "./cli_args.ts";
import type { LifecycleDeps } from "./lifecycle.ts";
import {
  resolvePipelineV2StateRootProjection,
} from "./pipeline_v2_state_root.ts";
import {
  continuePipelineV2Stage,
  resumePipelineV2,
  resumePipelineV2PlanningRunPlan,
  revisePipelineV2Task,
  runPipelineV2,
  type PipelineV2RunOutcome,
  type PipelineV2RunnerDeps,
} from "./pipeline_v2_runner.ts";
import { readFile } from "node:fs/promises";
import {
  respondPipelineV2Wait,
  type PipelineV2WaitResponseOutcome,
} from "./pipeline_v2_wait_respond.ts";
import {
  inspectPipelineV2Run,
  type PipelineV2InspectOutcome,
} from "./pipeline_v2_inspect.ts";
import {
  eventsPipelineV2Run,
  type PipelineV2EventsOutcome,
} from "./pipeline_v2_events.ts";
import {
  logsPipelineV2Run,
  type PipelineV2LogsOutcome,
} from "./pipeline_v2_logs.ts";
import {
  projectChangesPipelineV2Run,
  type PipelineV2ProjectChangesOutcome,
} from "./pipeline_v2_project_changes.ts";
import {
  projectChangeContentPipelineV2Run,
  type PipelineV2ProjectChangeContentOutcome,
} from "./pipeline_v2_project_change_content.ts";
import {
  applyPipelineV2ProjectOperation,
  inspectPipelineV2ProjectApplyOperationForRun,
  type PipelineV2ProjectApplyCommandOutcome,
} from "./pipeline_v2_project_apply.ts";

/**
 * The per-call seam of the dispatcher. The production default builds one
 * `SubprocessCliRunner` per invocation; tests inject recorders.
 */
export interface CliIo {
  baseEnv: Readonly<Record<string, string | undefined>>;
  /**
   * The optional worker operation runner seam (production default: the
   * real adapter-owned HTTP operation). Test-only; never set in
   * production CLI IO.
   */
  operationRunner?: PipelineV2WorkerOperationRunner;
  runner: {
    run: CliRunner;
    killActive: (signal: "SIGINT" | "SIGTERM") => boolean;
  };
  fetchAuth: AuthFetcher;
  resolveHelperConfig: (env: Readonly<Record<string, string | undefined>>) => HelperConfig;
  resolveStateRootProjection: typeof resolvePipelineV2StateRootProjection;
  runPipelineV2: typeof runPipelineV2;
  resumePipelineV2: typeof resumePipelineV2;
  continuePipelineV2Stage: typeof continuePipelineV2Stage;
  revisePipelineV2Task: typeof revisePipelineV2Task;
  resumePipelineV2PlanningRunPlan: typeof resumePipelineV2PlanningRunPlan;
  respondPipelineV2Wait: typeof respondPipelineV2Wait;
  inspectPipelineV2Run: typeof inspectPipelineV2Run;
  eventsPipelineV2Run: typeof eventsPipelineV2Run;
  logsPipelineV2Run: typeof logsPipelineV2Run;
  projectChangesPipelineV2Run: typeof projectChangesPipelineV2Run;
  projectChangeContentPipelineV2Run: typeof projectChangeContentPipelineV2Run;
  projectApplyPipelineV2: typeof applyPipelineV2ProjectOperation;
  projectApplyInspectPipelineV2: typeof inspectPipelineV2ProjectApplyOperationForRun;
  runSmoke: typeof runSmoke;
  runAgentSmoke: typeof runAgentSmoke;
  /**
   * The one read-only seam the revise-task command needs: reading the
   * task-body file as UTF-8 text. The production default reads the file
   * exactly once; read errors surface content-free (no path, no body, no
   * hostile filesystem message).
   */
  readTaskFile: (path: string) => Promise<string>;
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
    continuePipelineV2Stage,
    revisePipelineV2Task,
    resumePipelineV2PlanningRunPlan,
    respondPipelineV2Wait,
    inspectPipelineV2Run,
    eventsPipelineV2Run,
    logsPipelineV2Run,
    projectChangesPipelineV2Run,
    projectChangeContentPipelineV2Run,
    projectApplyPipelineV2: applyPipelineV2ProjectOperation,
    projectApplyInspectPipelineV2: inspectPipelineV2ProjectApplyOperationForRun,
    runSmoke,
    runAgentSmoke,
    readTaskFile: (path: string) => readFile(path, "utf8"),
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

const COMMANDS = ["smoke", "agent-smoke", "run", "resume", "respond", "continue-stage", "revise-task", "resume-plan", "inspect", "events", "logs", "project-changes", "project-change-content", "project-apply", "project-apply-inspect"] as const;

export async function runCli(argv: readonly string[], io: CliIo = productionCliIo()): Promise<number> {
  const command = argv[0];

  if (
    command !== "smoke" &&
    command !== "agent-smoke" &&
    command !== "run" &&
    command !== "resume" &&
    command !== "respond" &&
    command !== "continue-stage" &&
    command !== "revise-task" &&
    command !== "resume-plan" &&
    command !== "inspect" &&
    command !== "events" &&
    command !== "logs" &&
    command !== "project-changes" &&
    command !== "project-change-content" &&
    command !== "project-apply" &&
    command !== "project-apply-inspect"
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
  if (parsed.kind === "inspect") {
    // The read-only pipeline v2 inspect command: like respond, it
    // resolves only the state-root projection (no helper configuration,
    // no Launcher credential, no auth, no subprocess, no signal
    // registration, no durable change) and invokes the inspect production
    // API exactly once. No SubprocessCliRunner command dependencies and
    // no mutating command deps object are built for it.
    return await runPipelineV2InspectCommand(parsed, io);
  }
  if (parsed.kind === "events") {
    // The read-only pipeline v2 events command: like inspect, it resolves
    // only the state-root projection (no helper configuration, no
    // Launcher credential, no auth, no subprocess, no signal
    // registration, no mutation lock, no durable change) and invokes the
    // events production API exactly once. No SubprocessCliRunner command
    // dependencies and no mutating command deps object are built for it.
    return await runPipelineV2EventsCommand(parsed, io);
  }
  if (parsed.kind === "logs") {
    // The read-only pipeline v2 worker logs command: like inspect and
    // events, it resolves only the state-root projection (no helper
    // configuration, no Launcher credential, no auth, no subprocess, no
    // signal registration, no mutation lock, no durable change) and
    // invokes the logs production API exactly once. No
    // SubprocessCliRunner command dependencies and no mutating command
    // deps object are built for it.
    return await runPipelineV2LogsCommand(parsed, io);
  }
  if (parsed.kind === "project-change-content") {
    // The read-only pipeline v2 project-change-content command: like
    // inspect, events, logs and project-changes, it resolves only the
    // state-root projection (no helper configuration, no Launcher
    // credential, no auth, no subprocess, no signal registration, no
    // mutation lock, no durable change) and invokes the content
    // production API exactly once. No SubprocessCliRunner command
    // dependencies and no mutating command deps object are built for it.
    return await runPipelineV2ProjectChangeContentCommand(parsed, io);
  }
  if (parsed.kind === "project-apply") {
    // The explicit pipeline v2 Project Apply command: it resolves only
    // the state-root projection (no helper configuration, no Launcher
    // credential, no auth, no subprocess, no signal registration) and
    // invokes the Apply production API exactly once. The Apply runs
    // under the cooperative project lock inside the existing facade;
    // no run mutation lock is taken (the operation is keyed by the
    // caller-owned apply id and every write is idempotent), and no
    // worker, Docker Helper call or session is ever started.
    return await runPipelineV2ProjectApplyCommand(parsed, io);
  }
  if (parsed.kind === "project-apply-inspect") {
    // The read-only pipeline v2 Project Apply operation inspection: like
    // project-changes it resolves only the state-root projection and
    // invokes the read-only inspection exactly once. It never runs
    // recovery and never changes any durable status.
    return await runPipelineV2ProjectApplyInspectCommand(parsed, io);
  }
  if (parsed.kind === "project-changes") {
    // The read-only pipeline v2 project-changes command: like inspect and
    // events, it resolves only the state-root projection (no helper
    // configuration, no Launcher credential, no auth, no subprocess, no
    // signal registration, no mutation lock, no durable change) and
    // invokes the project-changes production API exactly once with the
    // caller-provided trusted operator project path. No
    // SubprocessCliRunner command dependencies and no mutating command
    // deps object are built for it.
    return await runPipelineV2ProjectChangesCommand(parsed, io);
  }
  if (parsed.kind === "continue-stage") {
    // The production pipeline v2 continue-stage intervention: the same
    // protected CLI-configuration boundary and per-call dependency
    // assembly, then exactly one `continuePipelineV2Stage` invocation.
    return await runPipelineV2ContinueStageCommand(parsed, io);
  }
  if (parsed.kind === "revise-task") {
    // The production pipeline v2 revise-task intervention: the same
    // protected CLI-configuration boundary, then exactly one UTF-8 read
    // of the task-body file (never inline or through argv), the per-call
    // dependency assembly, and exactly one `revisePipelineV2Task`
    // invocation.
    return await runPipelineV2ReviseTaskCommand(parsed, io);
  }
  if (parsed.kind === "resume-plan") {
    // The production pipeline v2 planning continuation: the same
    // protected CLI-configuration boundary and per-call dependency
    // assembly, then exactly one `resumePipelineV2PlanningRunPlan`
    // invocation. The stage id and the initial budget stay explicit
    // operator policy; every internal handoff parameter is derived by the
    // composition controller.
    return await runPipelineV2ResumePlanCommand(parsed, io);
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

  if (parsed.kind !== "agent-smoke") {
    throw new Error(`unhandled command kind: ${parsed.kind}`);
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
    operationRunner: io.operationRunner,
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
      runId: parsed.runId,
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
 * The production pipeline v2 continue-stage intervention command: parse
 * already done. The same protected CLI-configuration boundary (the
 * state-root projection first, then the helper configuration) and the same
 * per-call dependency assembly as `run`/`resume`, then exactly one
 * `continuePipelineV2Stage` invocation. The intervention action is fixed
 * by the runner entrypoint; the run id, the wait index, the additional
 * iteration count and the configuration root are the only user inputs.
 */
async function runPipelineV2ContinueStageCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "continue-stage" }>,
  io: CliIo,
): Promise<number> {
  const configuration = await resolvePipelineV2CliConfiguration(io);
  if (configuration === null) {
    return 2;
  }

  const deps = pipelineV2CommandDeps(parsed.json, configuration, io);

  const outcome: PipelineV2RunOutcome = await io.continuePipelineV2Stage(
    {
      runId: parsed.runId,
      waitIndex: parsed.waitIndex,
      additionalIterations: parsed.additionalIterations,
      configRoot: parsed.configRoot,
      launcherId: parsed.launcherId,
    },
    deps,
  );

  return reportPipelineV2Outcome("continue-stage", parsed.json, outcome, io);
}

/**
 * The production pipeline v2 revise-task intervention command: parse
 * already done. The same protected CLI-configuration boundary (the
 * state-root projection first, then the helper configuration), then
 * exactly one UTF-8 read of the `--task-file` (the revised task body is
 * never passed inline or through argv), the non-empty check, the same
 * per-call dependency assembly as `run`/`resume`/`continue-stage`, and
 * exactly one `revisePipelineV2Task` invocation. A read failure or an
 * empty file is a CLI contract error (exit 2 with the usage text) whose
 * message is content-free: no file path, no file body and no hostile
 * filesystem error echo ever reaches a diagnostic.
 */
async function runPipelineV2ReviseTaskCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "revise-task" }>,
  io: CliIo,
): Promise<number> {
  const configuration = await resolvePipelineV2CliConfiguration(io);
  if (configuration === null) {
    return 2;
  }

  let taskBody: string;
  try {
    taskBody = await io.readTaskFile(parsed.taskFile);
  } catch {
    io.writeError("error: revise-task could not read the task file");
    io.writeError(usage());
    return 2;
  }
  if (taskBody === "") {
    io.writeError("error: revise-task requires a non-empty task body");
    io.writeError(usage());
    return 2;
  }

  const deps = pipelineV2CommandDeps(parsed.json, configuration, io);

  const outcome: PipelineV2RunOutcome = await io.revisePipelineV2Task(
    {
      runId: parsed.runId,
      waitIndex: parsed.waitIndex,
      taskId: parsed.taskId,
      taskBody,
      configRoot: parsed.configRoot,
      launcherId: parsed.launcherId,
    },
    deps,
  );

  return reportPipelineV2Outcome("revise-task", parsed.json, outcome, io);
}

/**
 * The production pipeline v2 planning continuation command: parse already
 * done. The same protected CLI-configuration boundary (the state-root
 * projection first, then the helper configuration) and the same per-call
 * dependency assembly as `run`/`resume`/`continue-stage`/`revise-task`,
 * then exactly one `resumePipelineV2PlanningRunPlan` invocation. The run
 * id, the stage id, the initial budget and the configuration root are the
 * only user inputs; the stage id and the initial budget stay explicit
 * operator policy and are never derived from the durable state; the
 * command is not a respond action and accepts no `--action` flag; every
 * internal handoff parameter is derived by the composition controller from
 * the authoritative durable state.
 */
async function runPipelineV2ResumePlanCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "resume-plan" }>,
  io: CliIo,
): Promise<number> {
  const configuration = await resolvePipelineV2CliConfiguration(io);
  if (configuration === null) {
    return 2;
  }

  const deps = pipelineV2CommandDeps(parsed.json, configuration, io);

  const outcome: PipelineV2RunOutcome = await io.resumePipelineV2PlanningRunPlan(
    {
      runId: parsed.runId,
      stageId: parsed.stageId,
      initialBudget: parsed.initialBudget,
      configRoot: parsed.configRoot,
      launcherId: parsed.launcherId,
    },
    deps,
  );

  return reportPipelineV2Outcome("resume-plan", parsed.json, outcome, io);
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

/**
 * The read-only pipeline v2 inspect command: parse already done. Only the
 * state-root projection is resolved inside the protected CLI-configuration
 * boundary (the same env resolver `run`, `resume` and `respond` use); the
 * helper configuration is deliberately NOT resolved because inspect needs
 * no Launcher credential, no Docker Helper transport and no signal
 * lifecycle. Any resolution failure is exit 2 with the usage text and the
 * production API never called. The dispatcher builds no
 * `SubprocessCliRunner` and no mutating command deps object for inspect.
 */
async function runPipelineV2InspectCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "inspect" }>,
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

  const outcome: PipelineV2InspectOutcome = await io.inspectPipelineV2Run({
    runId: parsed.runId,
    stateRoot: stateRootProjection.localRoot,
  });

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    let extra = "";
    if (outcome.boundary.kind === "waiting") {
      extra = `, wait ${outcome.boundary.waitIndex}`;
    } else if (outcome.boundary.kind === "succeeded") {
      extra = `, terminal ${outcome.boundary.terminalStateId}`;
    }
    io.writeError(
      `orchestrator: inspect ${outcome.status}/${outcome.boundary.kind} (run ${outcome.runId}, revision ${outcome.revision}${extra})`,
    );
  } else {
    io.writeError(
      `orchestrator: inspect failed (run ${outcome.runId}, reason ${outcome.reason})`,
    );
  }
  return outcome.exitCode;
}

/**
 * The read-only pipeline v2 events command: parse already done. Only the
 * state-root projection is resolved inside the protected CLI-configuration
 * boundary; the helper configuration is deliberately NOT resolved because
 * events need no Launcher credential, no Docker Helper transport, no
 * mutation lock and no signal lifecycle. Any resolution failure is exit 2
 * with the usage text and the production API never called. The dispatcher
 * builds no `SubprocessCliRunner` and no mutating command deps object.
 */
async function runPipelineV2EventsCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "events" }>,
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

  const outcome: PipelineV2EventsOutcome = await io.eventsPipelineV2Run({
    runId: parsed.runId,
    stateRoot: stateRootProjection.localRoot,
    after: parsed.after,
    limit: parsed.limit,
  });

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    io.writeError(
      `orchestrator: events ok (run ${outcome.runId}, revision ${outcome.revision}, events ${outcome.events.length}, next-after ${outcome.nextAfter === null ? "none" : outcome.nextAfter})`,
    );
  } else {
    io.writeError(
      `orchestrator: events failed (run ${outcome.runId}, reason ${outcome.reason})`,
    );
  }
  return outcome.exitCode;
}

async function runPipelineV2LogsCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "logs" }>,
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

  const outcome: PipelineV2LogsOutcome = await io.logsPipelineV2Run({
    runId: parsed.runId,
    stateRoot: stateRootProjection.localRoot,
    ...(parsed.executionIndex === null ? {} : { executionIndex: parsed.executionIndex }),
    ...(parsed.executionIndex === null ? {} : { offset: parsed.offset }),
    ...(parsed.executionIndex === null ? {} : { limit: parsed.limit }),
  });

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    if ("logs" in outcome) {
      io.writeError(
        `orchestrator: logs ok (run ${outcome.runId}, revision ${outcome.revision}, agent logs ${outcome.logs.length})`,
      );
    } else {
      io.writeError(
        `orchestrator: logs ok (run ${outcome.runId}, execution ${outcome.executionIndex}, status ${outcome.status})`,
      );
    }
  } else {
    io.writeError(
      `orchestrator: logs failed (run ${outcome.runId}, reason ${outcome.reason})`,
    );
  }
  return outcome.exitCode;
}

/**
 * The read-only pipeline v2 project-change-content command: like inspect,
 * events, logs and project-changes, it resolves only the state-root
 * projection (no helper configuration, no Launcher credential, no auth,
 * no subprocess, no signal registration, no mutation lock, no durable
 * change) and invokes the content production API exactly once. The
 * caller-provided `--project` path is a trusted operator surface; the
 * observed path is derived exclusively from the verified change entry.
 */
async function runPipelineV2ProjectChangeContentCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "project-change-content" }>,
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

  const outcome: PipelineV2ProjectChangeContentOutcome = await io.projectChangeContentPipelineV2Run({
    runId: parsed.runId,
    stateRoot: stateRootProjection.localRoot,
    projectPath: parsed.project,
    changeIndex: parsed.changeIndex,
    side: parsed.side,
    observation: parsed.observation,
    ...(parsed.offset === 0 ? {} : { offset: parsed.offset }),
    ...(parsed.limit === 65536 ? {} : { limit: parsed.limit }),
  });

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    io.writeError(
      `orchestrator: project-change-content ok (run ${outcome.runId}, revision ${outcome.revision}, change ${outcome.changeIndex}, side ${outcome.side}, status ${outcome.status})`,
    );
  } else {
    io.writeError(
      `orchestrator: project-change-content failed (run ${outcome.runId}, reason ${outcome.reason})`,
    );
  }
  return outcome.exitCode;
}

/**
 * The read-only pipeline v2 project-changes command: like inspect and
 * events, it resolves only the state-root projection (no helper
 * configuration, no Launcher credential, no auth, no subprocess, no
 * signal registration, no mutation lock, no durable change) and invokes
 * the project-changes production API exactly once. The caller-provided
 * `--project` path is a trusted operator surface: it names the current
 * operator project the comparison observes read-only.
 */
async function runPipelineV2ProjectChangesCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "project-changes" }>,
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

  const outcome: PipelineV2ProjectChangesOutcome = await io.projectChangesPipelineV2Run({
    runId: parsed.runId,
    stateRoot: stateRootProjection.localRoot,
    projectPath: parsed.project,
    ...(parsed.after === 0 ? {} : { after: parsed.after }),
    ...(parsed.limit === 100 ? {} : { limit: parsed.limit }),
    ...(parsed.observation === null ? {} : { observation: parsed.observation }),
  });

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }

  if (outcome.ok) {
    io.writeError(
      `orchestrator: project-changes ok (run ${outcome.runId}, revision ${outcome.revision}, changes ${outcome.summary.total}, next-after ${outcome.nextAfter === null ? "none" : outcome.nextAfter})`,
    );
  } else {
    io.writeError(
      `orchestrator: project-changes failed (run ${outcome.runId}, reason ${outcome.reason})`,
    );
  }
  return outcome.exitCode;
}

/** The one human summary line of the explicit Project Apply commands. */
function reportPipelineV2ProjectApplyOutcome(
  command: "project-apply" | "project-apply-inspect",
  outcome: PipelineV2ProjectApplyCommandOutcome,
  io: CliIo,
): number {
  if (outcome.ok) {
    io.writeError(
      `orchestrator: ${command} ok (run ${outcome.runId}, apply ${outcome.applyId}, status ${outcome.status}, steps ${outcome.completedStepCount}/${outcome.totalStepCount})`,
    );
  } else {
    io.writeError(
      `orchestrator: ${command} failed (run ${outcome.runId}, apply ${outcome.applyId}, reason ${outcome.reason})`,
    );
  }
  return outcome.exitCode;
}

async function runPipelineV2ProjectApplyCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "project-apply" }>,
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

  const outcome: PipelineV2ProjectApplyCommandOutcome = await io.projectApplyPipelineV2({
    applyStoreRoot: parsed.applyStoreRoot,
    applyId: parsed.applyId,
    runId: parsed.runId,
    projectId: parsed.projectId,
    projectRoot: parsed.project,
    observation: parsed.observation,
    stateRoot: stateRootProjection.localRoot,
  });

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }
  return reportPipelineV2ProjectApplyOutcome("project-apply", outcome, io);
}

async function runPipelineV2ProjectApplyInspectCommand(
  parsed: Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "project-apply-inspect" }>,
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

  const outcome: PipelineV2ProjectApplyCommandOutcome = await io.projectApplyInspectPipelineV2({
    applyStoreRoot: parsed.applyStoreRoot,
    applyId: parsed.applyId,
    runId: parsed.runId,
    projectId: parsed.projectId,
  });

  if (parsed.json) {
    io.writeStdout(`${JSON.stringify(outcome)}\n`);
    return outcome.exitCode;
  }
  return reportPipelineV2ProjectApplyOutcome("project-apply-inspect", outcome, io);
}

function reportPipelineV2Outcome(
  command: "run" | "resume" | "continue-stage" | "revise-task" | "resume-plan",
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
  } else if (outcome.waiting === true) {
    // The controlled suspension at the trusted stage-wait boundary: one
    // summary line, no failure/reason wording, exit 0 — the declared
    // intervention commands are the operator's next step.
    io.writeError(
      `orchestrator: ${command} waiting (run ${outcome.runId}, state ${outcome.runRoot}/state.json)`,
    );
  } else if (outcome.planReady === true) {
    // The controlled suspension at the settled-but-unbound planning
    // boundary: one summary line, no failure/reason wording, exit 0 —
    // `orchestrator resume-plan` with the explicit stage/budget policy is
    // the operator's next step (no automatic stage or budget exists).
    io.writeError(
      `orchestrator: ${command} plan-ready (run ${outcome.runId}, state ${outcome.runRoot}/state.json)`,
    );
  } else if (outcome.reason === "run_already_exists") {
    // The fresh-run collision: the run root names an existing object the
    // runner never opened. `orchestrator inspect --run-id R` is the one
    // safe next step; the existing run's state is never disclosed here.
    io.writeError(
      `orchestrator: pipeline v2 run failed: run "${outcome.runId}" already exists; use inspect`,
    );
  } else if (outcome.reason === "run_busy") {
    // The per-run mutation lock is held by another process: a retryable
    // transport/control-plane condition, not a durable run failure. The
    // run tree was never touched by this command.
    io.writeError(
      `orchestrator: pipeline v2 ${command} failed: run "${outcome.runId}" is busy`,
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
