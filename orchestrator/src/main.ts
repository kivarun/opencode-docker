import {
  fetchAuthOverSocket,
  type CliResult,
  type CliRunner,
  type CliStdio,
} from "./docker_helper.ts";
import { resolveHelperConfig } from "./launcher.ts";
import { runAgentSmoke, type AgentSmokeOptions } from "./agent_smoke.ts";
import { runSmoke, type SmokeOptions } from "./smoke.ts";
import { DEFAULT_WORKER_IMAGE } from "./worker.ts";

interface ParsedCommon {
  workspace: string;
  workerImage: string | null;
  workerImageExplicit: boolean;
  launcherId?: string;
}

interface ParsedSmokeArgs {
  kind: "smoke";
  workspace: string;
  workerImage: string;
  launcherId?: string;
}

interface ParsedAgentSmokeArgs {
  kind: "agent-smoke";
  workspace: string;
  workerImage: string;
  launcherId?: string;
  taskPath?: string;
}

type ParsedCommand = ParsedSmokeArgs | ParsedAgentSmokeArgs;

function usage(): string {
  return [
    "usage: orchestrator <command> [flags]",
    "",
    "commands:",
    "  smoke        one-shot launcher-delegation smoke test:",
    "               launcher credential -> child session -> worker container ->",
    "               workspace artifact -> verified cleanup",
    "  agent-smoke  OpenCode agent smoke test:",
    "               launcher credential -> child session -> OpenCode agent ->",
    "               work product -> validated result.json -> unchanged TASK.md ->",
    "               verified cleanup",
    "",
    "common flags:",
    "  --workspace PATH        workspace passed to 'docker-helper session create'; must exist",
    "                          and be visible to the orchestrator at the same absolute path",
    "                          (default: ${SMOKE_WORKSPACE:-/workspace})",
    "  --launcher-id DHL_ID    fail unless the installed credential belongs to this launcher",
    "",
    "smoke flags:",
    `  --image WORKER_IMAGE    worker container image (default: ${DEFAULT_WORKER_IMAGE})`,
    "",
    "agent-smoke flags:",
    "  --image WORKER_IMAGE    OpenCode agent image (required; explicit opt-in)",
    "  --task PATH             task file for the agent; must be a regular file inside the",
    "                          workspace (default: <workspace>/TASK.md); the file content is",
    "                          never embedded in command lines, environment or logs",
    "",
    "The agent worker environment is an explicit allowlist: LLM_SERVER, LLM_KEY,",
    "OPENCODE_CONFIG_CONTENT, OPENCODE_ENABLE_EXA, OPENCODE_EXPERIMENTAL_LSP_TOOL",
    "(when present in the orchestrator environment), the child session bearer token",
    "and non-secret run parameters (AGENT_SMOKE_RUN_ID, AGENT_SMOKE_TASK_PATH,",
    "AGENT_SMOKE_RESULT_PATH). Launcher credentials, admin tokens and the orchestrator",
    "state path are never forwarded. OPENCODE_CONFIG_CONTENT may carry the operator's",
    "opencode.json so the agent image runs with the existing model/provider config.",
    "",
    "The launcher credential is read from",
    "  ${XDG_CONFIG_HOME:-$HOME/.config}/docker-helper/credential.token",
    "install it with: XDG_CONFIG_HOME=<private-dir> docker-helper credential install",
  ].join("\n");
}

function parseValue(argv: string[], i: number, flag: string): { value: string; next: number } {
  const arg = argv[i] ?? "";
  if (arg === flag) {
    const value = argv[i + 1];
    if (value === undefined || value === "") {
      throw new Error(`${flag} requires a value`);
    }
    return { value, next: i + 1 };
  }
  if (arg.startsWith(`${flag}=`)) {
    const value = arg.slice(`${flag}=`.length);
    if (value === "") {
      throw new Error(`${flag} requires a value`);
    }
    return { value, next: i };
  }
  throw new Error(`unknown argument: ${arg}`);
}

function parseCommand(kind: "smoke" | "agent-smoke", argv: string[]): ParsedCommand {
  const defaultWorkspace = process.env.SMOKE_WORKSPACE?.trim() || "/workspace";
  let workspace: string | null = null;
  let workerImage: string | null = null;
  let workerImageExplicit = false;
  let launcherId: string | undefined;
  let taskPath: string | undefined;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      const { value, next } = parseValue(argv, i, "--workspace");
      if (!value.startsWith("/")) {
        throw new Error("--workspace must be an absolute path");
      }
      workspace = value;
      i = next;
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      const { value, next } = parseValue(argv, i, "--image");
      workerImage = value;
      workerImageExplicit = true;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      launcherId = value;
      i = next;
    } else if (kind === "agent-smoke" && (arg === "--task" || arg.startsWith("--task="))) {
      const { value, next } = parseValue(argv, i, "--task");
      taskPath = value;
      i = next;
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  const common: ParsedCommon = {
    workspace: workspace ?? defaultWorkspace,
    workerImage,
    workerImageExplicit,
    launcherId,
  };
  if (kind === "smoke") {
    return {
      kind,
      workspace: common.workspace,
      workerImage: common.workerImage ?? DEFAULT_WORKER_IMAGE,
      launcherId: common.launcherId,
    };
  }
  if (!common.workerImageExplicit) {
    throw new Error(
      "--image WORKER_IMAGE is required for agent-smoke (explicit opt-in for the OpenCode agent image)",
    );
  }
  return {
    kind: "agent-smoke",
    workspace: common.workspace,
    workerImage: common.workerImage ?? DEFAULT_WORKER_IMAGE,
    launcherId: common.launcherId,
    taskPath,
  };
}

class SubprocessCliRunner {
  private active: { kill: (signal: "SIGTERM") => void } | null = null;

  killActive(): void {
    if (this.active !== null) {
      try {
        this.active.kill("SIGTERM");
      } catch {
        this.active = null;
      }
    }
  }

  async run(
    args: string[],
    env: Record<string, string>,
    stdio: CliStdio,
  ): Promise<CliResult> {
    const proc = Bun.spawn(["docker-helper", ...args], {
      env,
      stdin: "ignore",
      stdout: stdio === "inherit" ? "inherit" : "pipe",
      stderr: stdio === "inherit" ? "inherit" : "pipe",
    });
    this.active = proc;
    try {
      if (stdio === "inherit") {
        return { code: await proc.exited };
      }
      const [stdout, stderr] = await Promise.all([
        new Response(proc.stdout as ReadableStream).text(),
        new Response(proc.stderr as ReadableStream).text(),
      ]);
      return { code: await proc.exited, stdout, stderr };
    } finally {
      this.active = null;
    }
  }
}

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

  let parsed: ParsedCommand;
  try {
    parsed = parseCommand(command, argv.slice(1));
  } catch (cause) {
    console.error(`error: ${cause instanceof Error ? cause.message : String(cause)}`);
    console.error(usage());
    return 2;
  }

  const config = resolveHelperConfig(process.env);
  const runner = new SubprocessCliRunner();
  const deps = {
    cli: (args: string[], env: Record<string, string>, stdio: CliStdio) => runner.run(args, env, stdio),
    fetchAuth: fetchAuthOverSocket,
    config,
    baseEnv: process.env,
    onSignal: (handler: (signal: "SIGINT" | "SIGTERM") => void) => {
      for (const signal of ["SIGINT", "SIGTERM"] as const) {
        process.on(signal, () => {
          runner.killActive();
          handler(signal);
        });
      }
    },
  };

  if (parsed.kind === "smoke") {
    const options: SmokeOptions = {
      workspace: parsed.workspace,
      workerImage: parsed.workerImage,
      launcherId: parsed.launcherId,
    };
    const outcome = await runSmoke(options, deps);
    if (outcome.ok) {
      console.error(
        `orchestrator: smoke ok (run ${outcome.runId}, session ${outcome.sessionId ?? "?"}, status ${outcome.status})`,
      );
    }
    return outcome.exitCode;
  }

  const options: AgentSmokeOptions = {
    workspace: parsed.workspace,
    workerImage: parsed.workerImage,
    launcherId: parsed.launcherId,
    taskPath: parsed.taskPath ?? `${parsed.workspace.replace(/\/+$/, "")}/TASK.md`,
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
