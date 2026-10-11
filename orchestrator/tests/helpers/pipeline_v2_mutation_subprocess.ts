/**
 * Test-only subprocess helper: runs ONE pipeline v2 mutating command in a
 * separate OS process with a controlled fake transport, synchronized with
 * the parent through stdin gates. The parent reads progress lines from
 * stdout and the final `RESULT <json>` line; the durable effects are
 * observed in the parent through the filesystem.
 *
 * This file is test infrastructure only: production code never imports it.
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { respondPipelineV2Wait } from "../../src/pipeline_v2_wait_respond.ts";
import {
  continuePipelineV2Stage,
  resumePipelineV2,
  resumePipelineV2PlanningRunPlan,
  revisePipelineV2Task,
  runPipelineV2,
} from "../../src/pipeline_v2_runner.ts";
import type { AuthFetcher, CliRunner } from "../../src/docker_helper.ts";

interface SubprocessSpec {
  command:
    | "run"
    | "resume"
    | "continue-stage"
    | "revise-task"
    | "resume-plan"
    | "respond";
  stateRoot: string;
  bundle: string;
  configRoot: string;
  projectSource?: string;
  inputBindings?: ReadonlyArray<{ id: string; path: string }>;
  runId: string;
  waitIndex?: number;
  additionalIterations?: number;
  taskId?: string;
  taskBody?: string;
  stageId?: string;
  initialBudget?: number;
  actionId?: string;
  profileEnv: Readonly<Record<string, string>>;
  runCode?: number;
  gateAuth?: boolean;
  gateSessionCreate?: number;
  gateWorkerRun?: boolean;
  failSessionCreateAt?: number;
  launcherId?: string;
  /** Blocks on a stdin line right before the mutating call starts. */
  waitStart?: boolean;
  /** Output files the fake worker writes into the activation outputs mount. */
  workerOutputs?: Readonly<Record<string, Readonly<Record<string, string>>>>;
}

interface StdinQueue {
  next(): Promise<string>;
}

function makeStdinQueue(): StdinQueue {
  const pending: string[] = [];
  const queue: { waiter: ((line: string) => void) | null; closed: boolean } = {
    waiter: null,
    closed: false,
  };
  void (async () => {
    let buffer = "";
    const stream = Bun.stdin.stream() as unknown as AsyncIterable<Uint8Array>;
    for await (const chunk of stream) {
      buffer += new TextDecoder().decode(chunk);
      for (;;) {
        const newline = buffer.indexOf("\n");
        if (newline < 0) break;
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (line === "") continue;
        if (queue.waiter !== null) {
          const w = queue.waiter;
          queue.waiter = null;
          w(line);
        } else {
          pending.push(line);
        }
      }
    }
    queue.closed = true;
    if (queue.waiter !== null) {
      const w = queue.waiter;
      queue.waiter = null;
      w("<closed>");
    }
  })();
  return {
    next(): Promise<string> {
      const line = pending.shift();
      if (line !== undefined) return Promise.resolve(line);
      if (queue.closed) return Promise.resolve("<closed>");
      return new Promise<string>((resolve) => {
        queue.waiter = resolve;
      });
    },
  };
}

async function run(): Promise<void> {
  const specIndex = process.argv.indexOf("--spec");
  if (specIndex < 0) throw new Error("missing --spec");
  const spec = (await Bun.file(process.argv[specIndex + 1]!).json()) as SubprocessSpec;
  const stdin = makeStdinQueue();

  const stats = { cli: 0, create: 0, "delete": 0, pull: 0, run: 0, auth: 0 };
  // Session ids are globally unique across every execution of the run:
  // the reducer rejects a duplicate id, and separate OS processes must
  // never mint the same fake id twice.
  const processTag = Math.random().toString(36).slice(2, 8);
  const cli: CliRunner = async (args) => {
    stats.cli += 1;
    if (args[0] === "session" && args[1] === "create") {
      stats.create += 1;
      if (spec.failSessionCreateAt === stats.create) {
        return { code: 1, stderr: "create rejected" };
      }
      if (spec.gateSessionCreate === stats.create) {
        process.stdout.write(`GATE create-${stats.create}\n`);
        await stdin.next();
      }
      return {
        code: 0,
        stdout: JSON.stringify({
          ok: true,
          session: { id: `dhs_${processTag}_${stats.create}`, launcher_id: "dhl_runner" },
          token: `dhc_${processTag}_${stats.create}`,
        }),
      };
    }
    if (args[0] === "session" && args[1] === "delete") {
      stats["delete"] += 1;
      return {
        code: 0,
        stdout: JSON.stringify({ ok: true, deleted: true, id: args[args.length - 1] ?? "" }),
      };
    }
    if (args[0] === "pull") {
      stats.pull += 1;
      return { code: 0 };
    }
    if (args[0] === "run") {
      stats.run += 1;
      if (spec.gateWorkerRun) {
        process.stdout.write("GATE run\n");
        await stdin.next();
      }
      // The fake worker materializes the declared outputs into the
      // activation outputs mount, exactly like a real worker would.
      let mountStart = -1;
      for (let i = 0; i < args.length; i += 1) {
        if (args[i] === "--mount") {
          mountStart = i;
          break;
        }
      }
      const runRoot = `${spec.stateRoot}/pipeline-runs/${spec.runId}`;
      for (let i = mountStart; i >= 0 && i < args.length && args[i] === "--mount"; i += 2) {
        const mountSpec = args[i + 1] ?? "";
        const [source, target] = mountSpec.split(":");
        if (target === "/pipeline/outputs") {
          const segments = (source ?? "").split("/");
          const dash = segments[1] ?? "";
          const stateId = dash.slice(dash.indexOf("-") + 1);
          const outputs = spec.workerOutputs?.[stateId];
          if (outputs !== undefined) {
            const dir = `${runRoot}/${source}`;
            mkdirSync(dir, { recursive: true });
            for (const [name, content] of Object.entries(outputs)) {
              writeFileSync(`${dir}/${name}`, content);
            }
          }
        }
      }
      return { code: spec.runCode ?? 1, timedOut: false };
    }
    return { code: 1 };
  };
  const fetchAuth: AuthFetcher = async () => {
    stats.auth += 1;
    if (spec.gateAuth) {
      process.stdout.write("GATE auth\n");
      await stdin.next();
    }
    return {
      status: 200,
      body: { authority: "launcher", principal: "michael", launcher_id: "dhl_runner" },
    };
  };

  const projection = { localRoot: spec.stateRoot, daemonRoot: spec.stateRoot };
  const runnerDeps = {
    cli,
    fetchAuth,
    helperConfig: {
      socketPath: "/run/docker-helper/docker-helper.sock",
      credentialFile: `${spec.configRoot}/credential.token`,
    },
    baseEnv: { HOME: "/home/u", XDG_CONFIG_HOME: "/cfg", ...spec.profileEnv },
    stateRootProjection: projection,
  };

  process.stdout.write("READY\n");
  if (spec.waitStart) {
    await stdin.next();
  }
  let outcome: unknown;
  if (spec.command === "run") {
    outcome = await runPipelineV2(
      {
        pipelineRoot: spec.bundle,
        configRoot: spec.configRoot,
        projectSourcePath: spec.projectSource!,
        inputBindings: spec.inputBindings ?? [],
        runId: spec.runId,
      },
      runnerDeps,
    );
  } else if (spec.command === "resume") {
    outcome = await resumePipelineV2(
      { runId: spec.runId, configRoot: spec.configRoot },
      runnerDeps,
    );
  } else if (spec.command === "continue-stage") {
    outcome = await continuePipelineV2Stage(
      {
        runId: spec.runId,
        waitIndex: spec.waitIndex!,
        additionalIterations: spec.additionalIterations!,
        configRoot: spec.configRoot,
      },
      runnerDeps,
    );
  } else if (spec.command === "revise-task") {
    outcome = await revisePipelineV2Task(
      {
        runId: spec.runId,
        waitIndex: spec.waitIndex!,
        taskId: spec.taskId!,
        taskBody: spec.taskBody!,
        configRoot: spec.configRoot,
      },
      runnerDeps,
    );
  } else if (spec.command === "resume-plan") {
    outcome = await resumePipelineV2PlanningRunPlan(
      {
        runId: spec.runId,
        stageId: spec.stageId!,
        initialBudget: spec.initialBudget!,
        configRoot: spec.configRoot,
      },
      runnerDeps,
    );
  } else {
    outcome = await respondPipelineV2Wait(
      { runId: spec.runId, waitIndex: spec.waitIndex!, actionId: spec.actionId! },
      { stateRootProjection: projection },
    );
  }
  process.stdout.write(`RESULT ${JSON.stringify({ outcome, stats })}\n`);
  process.exit((outcome as { exitCode?: number }).exitCode ?? 1);
}

await run();
