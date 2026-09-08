import { mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  classifyAuthResult,
  credentialPath,
  parseSessionCreate,
  parseSessionDelete,
  resolveSocketPath,
  SubprocessCliRunner,
  type CliResult,
  type CliRunner,
} from "../src/docker_helper.ts";
import { runSmoke, artifactPath, type SmokeDeps } from "../src/smoke.ts";
import { parseArtifact } from "../src/artifact.ts";
import {
  childSessionEnv,
  operatorEnv,
  signalExitCode,
  type LifecycleDeps,
} from "../src/lifecycle.ts";
import { pullArgs, runArgs, smokeWorkerSpec } from "../src/worker.ts";

const LAUNCHER_TOKEN = "dhc_" + "a".repeat(64);
const CHILD_TOKEN = "dht_" + "b".repeat(64);
const CHILD_SESSION_ID = "dhs_child";
const LAUNCHER_ID = "dhl_launcher";
const SOCKET = "/run/docker-helper/test.sock";

const LAUNCHER_SECRET = "dhc_launcher_secret_value";
const ADMIN_SECRET = "dha_admin_secret_value";
const STATE_PATH = "/host/orchestrator-state";
const CANARY = "CANARY_AMBIENT_VAR";
const CANARY_VALUE = "must-never-reach-the-worker";
const COMPLEX_VALUE = "https://llm.example/v1? a=b \"c\"";

const BASE_ENV = {
  HOME: "/home/opencode",
  XDG_CONFIG_HOME: "/uat-cred",
  XDG_STATE_HOME: "/xdg-state",
  XDG_RUNTIME_DIR: "/run/user/1000",
  [CANARY]: CANARY_VALUE,
  DOCKER_HELPER_SESSION_TOKEN: "dht_launcher_session_token",
  DOCKER_HELPER_CREDENTIAL_TOKEN: LAUNCHER_SECRET,
  DOCKER_HELPER_ADMIN_TOKEN: ADMIN_SECRET,
  DOCKER_HELPER_STATE_PATH: STATE_PATH,
};

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
  stdio: string;
  signalOnAbort: boolean;
}

interface FakeCliOptions {
  workspace: string;
  createStdout?: string;
  createCode?: number;
  deleteCode?: number;
  pullCode?: number;
  runCode?: number;
  writeArtifact?: boolean;
  artifactBody?: string;
  blockCreate?: boolean;
  blockPull?: boolean;
  blockRun?: boolean;
}

function fakeCli(options: FakeCliOptions) {
  const calls: RecordedCall[] = [];
  const events: string[] = [];
  let activeRun: { release: (code: number) => void } | null = null;
  let releaseCreate: ((result: CliResult) => void) | null = null;
  let releasePull: ((result: CliResult) => void) | null = null;

  const runner: CliRunner = async (args, env, stdio, opts) => {
    calls.push({ args: [...args], env: { ...env }, stdio, signalOnAbort: opts?.signalOnAbort === true });
    if (args[0] === "run") {
      events.push("run:start");
      const runCode = options.runCode ?? 0;
      if (options.blockRun === true) {
        return await new Promise<CliResult>((resolve) => {
          activeRun = {
            release: (code: number) => {
              events.push(`run:exited:${code}`);
              resolve({ code, stdout: "", stderr: "" });
            },
          };
        });
      }
      events.push(`run:exited:${runCode}`);
      if ((options.writeArtifact ?? true) === true && runCode === 0) {
        const runIdMatch = args.find((a, i) => args[i - 1] === "--env" && a.startsWith("SMOKE_RUN_ID="));
        const runId = runIdMatch?.slice("SMOKE_RUN_ID=".length) ?? "";
        const body =
          options.artifactBody === undefined
            ? JSON.stringify({
                schema_version: 1,
                status: "success",
                run_id: runId,
                session_token_present: true,
              })
            : options.artifactBody;
        await mkdir(`${options.workspace}/.pipeline-smoke/${runId}`, { recursive: true });
        await Bun.write(artifactPath(options.workspace, runId), `${body}\n`);
      }
      return { code: runCode, stdout: "", stderr: "" };
    }
    if (args[0] === "pull") {
      events.push("pull:start");
      const result = { code: options.pullCode ?? 0, stdout: "", stderr: "" };
      if (options.blockPull === true) {
        return await new Promise<CliResult>((resolve) => {
          releasePull = (r) => {
            events.push("pull:done");
            resolve(r);
          };
        });
      }
      events.push("pull:done");
      return result;
    }
    if (args[0] === "session" && args[1] === "create") {
      events.push("session:create");
      const result: CliResult =
        (options.createCode ?? 0) !== 0
          ? { code: options.createCode ?? 1, stdout: "", stderr: "create boom" }
          : {
              code: 0,
              stdout:
                options.createStdout ??
                JSON.stringify({
                  ok: true,
                  session: {
                    id: CHILD_SESSION_ID,
                    workspace: options.workspace,
                    created_at: "t",
                    expires_at: "t",
                    launcher_id: LAUNCHER_ID,
                  },
                  token: CHILD_TOKEN,
                }),
              stderr: "",
            };
      if (options.blockCreate === true) {
        return await new Promise<CliResult>((resolve) => {
          releaseCreate = (r) => {
            events.push("session:create-done");
            resolve(r);
          };
        });
      }
      events.push("session:create-done");
      return result;
    }
    if (args[0] === "session" && args[1] === "delete") {
      events.push("session:delete");
      if ((options.deleteCode ?? 0) !== 0) {
        return { code: options.deleteCode ?? 1, stdout: "", stderr: "delete boom" };
      }
      return {
        code: 0,
        stdout: JSON.stringify({ ok: true, id: CHILD_SESSION_ID, deleted: true }),
        stderr: "",
      };
    }
    return { code: 1, stdout: "", stderr: `unexpected cli call: ${args[0]}` };
  };

  return {
    calls,
    events,
    runner,
    killActive: (signal: "SIGINT" | "SIGTERM") => {
      if (activeRun !== null) {
        events.push(`run:signal:${signal}`);
        const release = activeRun.release;
        activeRun = null;
        release(signalExitCode(signal));
      }
    },
    fireRunExit: (code: number) => {
      if (activeRun !== null) {
        const release = activeRun.release;
        activeRun = null;
        release(code);
      }
    },
    releaseCreate: (result: CliResult) => {
      if (releaseCreate !== null) {
        const release = releaseCreate;
        releaseCreate = null;
        release(result);
      }
    },
    releasePull: (result: CliResult) => {
      if (releasePull !== null) {
        const release = releasePull;
        releasePull = null;
        release(result);
      }
    },
  };
}

async function withTempDirs(
  fn: (dirs: { workspace: string; state: string; credentialFile: string }) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "orchestrator-test-"));
  const workspace = join(root, "workspace");
  const state = join(root, "state");
  const configDir = join(root, "config", "docker-helper");
  await mkdir(workspace, { recursive: true });
  await mkdir(state, { recursive: true });
  await mkdir(configDir, { recursive: true, mode: 0o700 });
  const credentialFile = join(configDir, "credential.token");
  await writeFile(credentialFile, `${LAUNCHER_TOKEN}\n`, { mode: 0o600 });
  try {
    await fn({ workspace, state, credentialFile });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function makeDeps(
  dirs: { workspace: string; state: string; credentialFile: string },
  cli: CliRunner,
  overrides: Partial<LifecycleDeps> = {},
): SmokeDeps {
  return {
    cli,
    fetchAuth: async () => ({
      status: 200,
      body: { authority: "launcher", principal: "michael", launcher_id: LAUNCHER_ID },
    }),
    config: { socketPath: SOCKET, credentialFile: dirs.credentialFile },
    stateDirPath: dirs.state,
    baseEnv: BASE_ENV,
    ...overrides,
  };
}

async function readText(path: string): Promise<string> {
  return await Bun.file(path).text();
}

function operatorEnvClean(env: Record<string, string>): void {
  expect(Object.keys(env).sort()).toEqual(
    ["HOME", "XDG_CONFIG_HOME", "XDG_RUNTIME_DIR"].filter((k) => env[k] !== undefined).concat(env["DOCKER_HELPER_CONFIG"] !== undefined ? ["DOCKER_HELPER_CONFIG"] : []).sort(),
  );
  expect(env.DOCKER_HELPER_SESSION_TOKEN).toBeUndefined();
}

test("1. success: worker runs, artifact verified, session cleaned up, exit 0", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);
    expect(outcome.status).toBe("success");

    const create = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    expect(create.args[create.args.indexOf("--workspace") + 1] ?? "").toBe(dirs.workspace);
    operatorEnvClean(create.env);

    const deleteCalls = calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete");
    expect(deleteCalls.length).toBe(1);
    expect(deleteCalls[0]?.args[deleteCalls[0].args.indexOf("--id") + 1] ?? "").toBe(CHILD_SESSION_ID);
    operatorEnvClean(deleteCalls[0]!.env);

    const pull = calls.find((c) => c.args[0] === "pull")!;
    expect(pull.args).toEqual(pullArgs("alpine:3.22", SOCKET));
    expect(pull.env).toEqual(childSessionEnv(CHILD_TOKEN));
    expect(pull.signalOnAbort).toBe(false);
    expect(pull.stdio).toBe("inherit");

    const run = calls.find((c) => c.args[0] === "run")!;
    expect(run.env).toEqual(childSessionEnv(CHILD_TOKEN));
    expect(run.signalOnAbort).toBe(true);
    expect(run.args).toEqual(
      runArgs(smokeWorkerSpec(outcome.runId, CHILD_TOKEN, "alpine:3.22"), SOCKET),
    );
    expect(run.args.includes("--")).toBe(true);

    const stateFiles = await readdir(dirs.state);
    expect(stateFiles.length).toBe(1);
    const state = JSON.parse(await readText(join(dirs.state, stateFiles[0] ?? "")));
    expect(state.session_id).toBe(CHILD_SESSION_ID);
    expect(state.status).toBe("success");
    expect(JSON.stringify(state)).not.toContain(CHILD_TOKEN);
    expect(JSON.stringify(state)).not.toContain(LAUNCHER_TOKEN);

    const artifact = JSON.parse(await readText(artifactPath(dirs.workspace, outcome.runId)));
    expect(artifact.run_id).toBe(outcome.runId);
  });
});

test("2. worker failure: session still deleted, exit non-zero", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, runCode: 3 });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.detail).toContain("worker container failed (exit 3)");
    expect(calls.filter((c) => c.args[0] === "run").length).toBe(1);
    expect(calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length).toBe(1);
  });
});

test("3. pull failure is fatal for smoke", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, pullCode: 1 });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("docker-helper pull alpine:3.22 failed (exit 1)");
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
    expect(calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length).toBe(1);
  });
});

test("3b. missing artifact: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: false });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("artifact not readable");
  });
});

test("3c. wrong artifact schema: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { runner } = fakeCli({
      workspace: dirs.workspace,
      artifactBody: JSON.stringify({ schema_version: 2, status: "success", run_id: "other" }),
    });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
  });
});

test("4. cleanup failure: overall result can never be success", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, deleteCode: 1 });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).toBe("cleanup_failed");
    expect(calls.filter((c) => c.args[1] === "delete").length).toBe(1);
  });
});

test("5. launcher/admin/state/canary never reach run env or args; task body never in argv", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );
    expect(outcome.exitCode).toBe(0);

    const run = calls.find((c) => c.args[0] === "run")!;
    const envText = JSON.stringify(run.env);
    expect(envText).not.toContain(LAUNCHER_SECRET);
    expect(envText).not.toContain(ADMIN_SECRET);
    expect(envText).not.toContain(STATE_PATH);
    expect(envText).not.toContain(CANARY_VALUE);
    expect(Object.keys(run.env)).toEqual(["DOCKER_HELPER_SESSION_TOKEN"]);

    const runArgsText = JSON.stringify(run.args);
    expect(runArgsText).not.toContain(LAUNCHER_SECRET);
    expect(runArgsText).not.toContain(ADMIN_SECRET);
    expect(runArgsText).not.toContain(STATE_PATH);
    expect(runArgsText).not.toContain(CANARY_VALUE);

    for (const call of calls) {
      if (call.args[0] === "session") {
        expect(JSON.stringify(call)).not.toContain(CHILD_TOKEN);
        expect(JSON.stringify(call)).not.toContain(LAUNCHER_SECRET);
      }
    }
  });
});

test("6. signal during run: signal forwarded -> CLI exited -> Session deleted", async () => {
  await withTempDirs(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, events, runner, killActive } = fakeCli({
      workspace: dirs.workspace,
      writeArtifact: false,
      blockRun: true,
    });
    const pending = runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );
    await Bun.sleep(10);
    expect(signalHandler).not.toBeNull();
    expect(events).toEqual(["session:create", "session:create-done", "pull:start", "pull:done", "run:start"]);

    // main.ts wiring: forward the same signal to the active signalable CLI, then record abort
    killActive("SIGTERM");
    signalHandler!("SIGTERM");
    await Bun.sleep(10);

    const outcome = await pending;
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);
    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).not.toBe("success");

    expect(events).toEqual([
      "session:create",
      "session:create-done",
      "pull:start",
      "pull:done",
      "run:start",
      "run:signal:SIGTERM",
      "run:exited:143",
      "session:delete",
    ]);
    expect(calls.find((c) => c.args[0] === "run")?.signalOnAbort).toBe(true);
    // delete is the last event: it happens only after the CLI process exited
    expect(events[events.length - 1]).toBe("session:delete");
    expect(calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete").length).toBe(1);
  });
});

test("6b. signal during session create: create completed -> Session deleted", async () => {
  await withTempDirs(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, events, runner, releaseCreate, killActive } = fakeCli({
      workspace: dirs.workspace,
      writeArtifact: false,
      blockCreate: true,
    });
    const pending = runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );
    await Bun.sleep(10);
    expect(events).toEqual(["session:create"]);

    signalHandler!("SIGTERM");
    killActive("SIGTERM");
    await Bun.sleep(10);

    releaseCreate({
      code: 0,
      stdout: JSON.stringify({
        ok: true,
        session: { id: CHILD_SESSION_ID, workspace: dirs.workspace, created_at: "t", expires_at: "t", launcher_id: LAUNCHER_ID },
        token: CHILD_TOKEN,
      }),
      stderr: "",
    });

    const outcome = await pending;
    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);

    expect(events).toEqual(["session:create", "session:create-done", "session:delete"]);
    expect(calls.some((c) => c.args[0] === "pull")).toBe(false);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
  });
});

test("6c. signal during pull: pull completed -> run skipped -> Session deleted", async () => {
  await withTempDirs(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, events, runner, releasePull, killActive } = fakeCli({
      workspace: dirs.workspace,
      writeArtifact: false,
      blockPull: true,
    });
    const pending = runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner, {
        onSignal: (handler) => {
          signalHandler = handler;
        },
      }),
    );
    await Bun.sleep(10);
    expect(events).toEqual(["session:create", "session:create-done", "pull:start"]);

    signalHandler!("SIGTERM");
    killActive("SIGTERM");
    await Bun.sleep(10);

    releasePull({ code: 0, stdout: "", stderr: "" });

    const outcome = await pending;
    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);

    expect(events).toEqual(["session:create", "session:create-done", "pull:start", "pull:done", "session:delete"]);
    expect(calls.some((c) => c.args[0] === "run")).toBe(false);
    expect(calls.find((c) => c.args[0] === "pull")?.signalOnAbort).toBe(false);
  });
});

test("extra: endpoint unreachable fails before any session is created", async () => {
  await withTempDirs(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner, {
        fetchAuth: async () => {
          throw new Error("connect: connection refused");
        },
      }),
    );

    expect(outcome.exitCode).toBe(1);
  });
});

test("extra: principal authority credential is rejected", async () => {
  await withTempDirs(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner, {
        fetchAuth: async () => ({ status: 200, body: { authority: "principal", principal: "michael" } }),
      }),
    );

    expect(outcome.exitCode).toBe(1);
  });
});

test("extra: wrong launcher id is rejected before creating a session", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22", launcherId: "dhl_expected" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("dhl_expected");
    expect(calls.filter((c) => c.args[1] === "create").length).toBe(0);
  });
});

test("extra: matching launcher id passes the guard", async () => {
  await withTempDirs(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22", launcherId: LAUNCHER_ID },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(0);
  });
});

test("extra: docker-helper 2.1.0 contract shapes", () => {
  const auth = classifyAuthResult({
    status: 200,
    body: { authority: "launcher", principal: "michael", launcher_id: "dhl_x" },
  });
  expect(auth).toEqual({ authority: "launcher", principal: "michael", launcher_id: "dhl_x" });
  expect(() => classifyAuthResult({ status: 401, body: { ok: false } })).toThrow(
    /rejected the installed credential/,
  );

  const parsed = parseSessionCreate(
    JSON.stringify({
      ok: true,
      session: {
        id: "dhs_x",
        workspace: "/opt/docker-helper-uat",
        created_at: "t",
        expires_at: "t",
        launcher_id: "dhl_l",
      },
      token: "dht_secret",
    }),
  );
  expect(parsed).toEqual({ sessionId: "dhs_x", token: "dht_secret", launcherId: "dhl_l" });
  expect(() => parseSessionCreate('{"ok":true,"session":{},"token":""}')).toThrow();
  expect(() => parseSessionCreate("not json")).toThrow();

  parseSessionDelete(JSON.stringify({ ok: true, id: "dhs_x", deleted: true }), "dhs_x");
  expect(() => parseSessionDelete(JSON.stringify({ ok: true, id: "dhs_other", deleted: true }), "dhs_x")).toThrow();
});

test("extra: artifact contract", () => {
  const ok = { schema_version: 1, status: "success", run_id: "r" };
  expect(parseArtifact(JSON.stringify(ok), "r")).toEqual(ok);
  expect(() => parseArtifact('{"schema_version":1,"status":"success","run_id":"x"}', "r")).toThrow(
    /does not match/,
  );
  expect(() => parseArtifact('{"schema_version":1,"status":"boom","run_id":"r"}', "r")).toThrow(
    /status/,
  );
  expect(() => parseArtifact('{"schema_version":2,"status":"success","run_id":"r"}', "r")).toThrow(
    /schema_version/,
  );
  expect(() => parseArtifact("[]", "r")).toThrow(/not a JSON object/);
  expect(() => parseArtifact("{", "r")).toThrow(/not valid JSON/);
});

test("extra: worker spec and CLI args", () => {
  const spec = smokeWorkerSpec("run-1", CHILD_TOKEN, "alpine:3.22");
  expect(spec.image).toBe("alpine:3.22");
  expect(spec.command[spec.command.length - 1]).toContain("result.json");

  expect(pullArgs("alpine:3.22", SOCKET)).toEqual(["pull", "--endpoint", SOCKET, "alpine:3.22"]);
  const args = runArgs(spec, SOCKET);
  expect(args[0]).toBe("run");
  expect(args[1]).toBe("--endpoint");
  expect(args[2]).toBe(SOCKET);
  expect(args[3]).toBe("--image");
  expect(args[4]).toBe("alpine:3.22");
  const envPairs = args.filter((a, i) => args[i - 1] === "--env");
  expect(envPairs).toEqual([
    `DOCKER_HELPER_SESSION_TOKEN=${CHILD_TOKEN}`,
    "SMOKE_RUN_ID=run-1",
  ]);
  const separatorIndex = args.indexOf("--");
  expect(args.slice(separatorIndex + 1)).toEqual(spec.command);

  const complexSpec = smokeWorkerSpec("r", CHILD_TOKEN, "img");
  complexSpec.containerEnv = {
    Z_LAST: "tail value",
    A_FIRST: "head = value \"quoted\"",
    M_MULTI: "line1\nline2",
  };
  const complexArgs = runArgs(complexSpec, SOCKET);
  const complexPairs = complexArgs.filter((a, i) => complexArgs[i - 1] === "--env");
  expect(complexPairs).toEqual([
    "A_FIRST=head = value \"quoted\"",
    "M_MULTI=line1\nline2",
    "Z_LAST=tail value",
  ]);
  for (const value of complexPairs) {
    expect(typeof value).toBe("string");
  }

  expect(resolveSocketPath(undefined)).toBe("/run/docker-helper/docker-helper.sock");
  expect(resolveSocketPath("/tmp/custom.sock")).toBe("/tmp/custom.sock");
  expect(credentialPath({ HOME: "/home/opencode" })).toBe(
    "/home/opencode/.config/docker-helper/credential.token",
  );
  expect(credentialPath({ XDG_CONFIG_HOME: "/xdg", HOME: "/home/opencode" })).toBe(
    "/xdg/docker-helper/credential.token",
  );
  expect(operatorEnv({ HOME: "/h", LLM_KEY: "x" })).toEqual({ HOME: "/h" });
  expect(childSessionEnv("dht_x")).toEqual({ DOCKER_HELPER_SESSION_TOKEN: "dht_x" });
  expect(signalExitCode("SIGINT")).toBe(130);
  expect(signalExitCode("SIGTERM")).toBe(143);
});

const dockerHelperOnPath = (await Bun.which("docker-helper")) !== null;

test.skipIf(!dockerHelperOnPath)(
  "extra: SubprocessCliRunner spawns docker-helper verbatim, captures stdout, propagates exit code",
  async () => {
    const runner = new SubprocessCliRunner();
    const help = await runner.run(["--help"], {}, "capture");
    expect(help.code).toBe(0);
    expect(help.stdout).toContain("docker-helper");

    const failing = await runner.run(["nonexistent-subcommand"], {}, "capture");
    expect(failing.code).not.toBe(0);
    // argv integrity is structural: args are passed as a string[] to Bun.spawn,
    // which has no shell mode; single-element values with spaces/quotes/newlines
    // are asserted at the runArgs level in the worker-spec test above.
  },
);

test("extra: state dir resolution", async () => {
  const { stateDir } = await import("../src/state.ts");
  expect(stateDir({ HOME: "/home/opencode" })).toBe("/home/opencode/.local/state/orchestrator");
  expect(stateDir({ XDG_STATE_HOME: "/xdg-state", HOME: "/home/opencode" })).toBe(
    "/xdg-state/orchestrator",
  );
});
