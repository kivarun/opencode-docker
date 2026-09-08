import {
  fetchAuthOverSocket,
  type CliRunOptions,
  type CliStdio,
  SubprocessCliRunner,
} from "./docker_helper.ts";
import { resolveHelperConfig } from "./launcher.ts";
import { runAgentSmoke, type AgentSmokeOptions } from "./agent_smoke.ts";
import { runSmoke } from "./smoke.ts";
import { parseCommand, usage } from "./cli_args.ts";
import type { LifecycleDeps } from "./lifecycle.ts";

async function main(): Promise<number> {
  const argv = process.argv.slice(2);
  const command = argv[0];

  if (command !== "smoke" && command !== "agent-smoke") {
    console.error(
      `error: expected 'orchestrator smoke' or 'orchestrator agent-smoke'${command !== undefined ? `, got ${JSON.stringify(command)}` : ""}`,
    );
    console.error(usage());
    return 2;
  }

  let parsed;
  try {
    parsed = parseCommand(command, argv.slice(1));
  } catch (cause) {
    console.error(`error: ${cause instanceof Error ? cause.message : String(cause)}`);
    console.error(usage());
    return 2;
  }

  const config = resolveHelperConfig(process.env);
  const runner = new SubprocessCliRunner();
  const deps: LifecycleDeps = {
    cli: (args, env, stdio, opts) => runner.run(args, env, stdio, opts),
    fetchAuth: fetchAuthOverSocket,
    config,
    baseEnv: process.env,
    onSignal: (handler: (signal: "SIGINT" | "SIGTERM") => void) => {
      for (const signal of ["SIGINT", "SIGTERM"] as const) {
        process.on(signal, () => {
          // forward the same signal to a running worker `run` CLI process only;
          // session create/delete and pull always run to completion
          runner.killActive(signal);
          handler(signal);
        });
      }
    },
  };

  if (parsed.kind === "smoke") {
    const outcome = await runSmoke(
      {
        workspace: parsed.workspace,
        workerImage: parsed.workerImage,
        launcherId: parsed.launcherId,
      },
      deps,
    );
    if (outcome.ok) {
      console.error(
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
  const outcome = await runAgentSmoke(options, deps);
  if (outcome.ok) {
    console.error(
      `orchestrator: agent-smoke ok (run ${outcome.runId}, session ${outcome.sessionId ?? "?"}, status ${outcome.status})`,
    );
  }
  return outcome.exitCode;
}

process.exit(await main());
