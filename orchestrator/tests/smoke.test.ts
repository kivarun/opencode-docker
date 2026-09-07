import { mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, test } from "bun:test";
import {
  classifyAuthResult,
  credentialPath,
  parseSessionCreate,
  parseSessionDelete,
  resolveSocketPath,
  type CliRunner,
  type CliStdio,
} from "../src/docker_helper.ts";
import { runSmoke, artifactPath, type SmokeDeps } from "../src/smoke.ts";
import { parseArtifact } from "../src/artifact.ts";
import { runArgs, smokeWorkerSpec } from "../src/worker.ts";

const LAUNCHER_TOKEN = "dhc_" + "a".repeat(64);
const CHILD_TOKEN = "dht_" + "b".repeat(64);
const CHILD_SESSION_ID = "dhs_child";
const LAUNCHER_ID = "dhl_launcher";

interface FakeCliOptions {
  workspace: string;
  createStdout?: string;
  createCode?: number;
  pullCode?: number;
  runCode?: number;
  writeArtifact?: boolean;
  artifactBody?: string;
  blockRun?: boolean;
  deleteCode?: number;
}

function fakeCli(options: FakeCliOptions) {
  const calls: { args: string[]; env: Record<string, string>; stdio: string }[] = [];
  let releaseRun: ((code: number) => void) | null = null;

  const runner: CliRunner = async (args, env, stdio) => {
    calls.push({ args: [...args], env: { ...env }, stdio });
    if (args[0] === "session" && args[1] === "create") {
      if (options.createCode !== undefined && options.createCode !== 0) {
        return { code: options.createCode, stdout: "", stderr: "create boom" };
      }
      return {
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
    }
    if (args[0] === "session" && args[1] === "delete") {
      if ((options.deleteCode ?? 0) !== 0) {
        return { code: options.deleteCode ?? 1, stdout: "", stderr: "delete boom" };
      }
      return {
        code: 0,
        stdout: JSON.stringify({ ok: true, id: CHILD_SESSION_ID, deleted: true }),
        stderr: "",
      };
    }
    if (args[0] === "pull") {
      return { code: options.pullCode ?? 0, stdout: "", stderr: "" };
    }
    if (args[0] === "run") {
      if (options.writeArtifact ?? true) {
        const runIdArg = args.find((a) => a.startsWith("SMOKE_RUN_ID="));
        const runId = (runIdArg ?? "SMOKE_RUN_ID=").split("=")[1] ?? "";
        const body =
          options.artifactBody === undefined
            ? JSON.stringify({
                schema_version: 1,
                status: "success",
                run_id: runId,
                session_token_present: true,
              })
            : options.artifactBody;
        if (body !== "") {
          await mkdir(`${options.workspace}/.pipeline-smoke/${runId}`, { recursive: true });
          await Bun.write(artifactPath(options.workspace, runId), `${body}\n`);
        }
      }
      if (options.blockRun === true) {
        return await new Promise<{ code: number }>((resolve) => {
          releaseRun = (code: number) => resolve({ code });
        });
      }
      return { code: options.runCode ?? 0, stdout: "", stderr: "" };
    }
    return { code: 1, stdout: "", stderr: `unexpected cli call: ${args[0]}` };
  };

  return {
    calls,
    runner,
    fireRunExit: (code: number) => {
      if (releaseRun !== null) {
        releaseRun(code);
        releaseRun = null;
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
  overrides: Partial<SmokeDeps> = {},
): SmokeDeps {
  return {
    cli,
    fetchAuth: async () => ({
      status: 200,
      body: { authority: "launcher", principal: "michael", launcher_id: LAUNCHER_ID },
    }),
    config: { socketPath: "/run/docker-helper/test.sock", credentialFile: dirs.credentialFile },
    stateDirPath: dirs.state,
    baseEnv: { HOME: "/home/opencode", XDG_CONFIG_HOME: "/uat-cred" },
    ...overrides,
  };
}

async function readText(path: string): Promise<string> {
  return await Bun.file(path).text();
}

test("1. success: worker runs, artifact verified, session cleaned up, exit 0", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: true });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(0);
    expect(outcome.ok).toBe(true);
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);
    expect(outcome.status).toBe("success");

    const createCall = calls.find((c) => c.args[0] === "session" && c.args[1] === "create")!;
    expect(createCall.args[createCall.args.indexOf("--workspace") + 1] ?? "").toBe(dirs.workspace);
    expect(createCall.env.XDG_CONFIG_HOME).toBe("/uat-cred");
    expect(createCall.env.DOCKER_HELPER_SESSION_TOKEN).toBeUndefined();

    const deleteCalls = calls.filter((c) => c.args[0] === "session" && c.args[1] === "delete");
    expect(deleteCalls.length).toBe(1);
    expect(deleteCalls[0]?.args[deleteCalls[0].args.indexOf("--id") + 1] ?? "").toBe(CHILD_SESSION_ID);
    expect(deleteCalls[0]?.env.XDG_CONFIG_HOME).toBe("/uat-cred");
    expect(deleteCalls[0]?.env.DOCKER_HELPER_SESSION_TOKEN).toBeUndefined();

    const stateFiles = await readdir(dirs.state);
    expect(stateFiles.length).toBe(1);
    const state = JSON.parse(await readText(join(dirs.state, stateFiles[0] ?? "")));
    expect(state.session_id).toBe(CHILD_SESSION_ID);
    expect(state.status).toBe("success");
    expect(JSON.stringify(state)).not.toContain(CHILD_TOKEN);
    expect(JSON.stringify(state)).not.toContain(LAUNCHER_TOKEN);

    const artifact = JSON.parse(
      await readText(artifactPath(dirs.workspace, outcome.runId)),
    );
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
    expect(calls.filter((c) => c.args[1] === "delete").length).toBe(1);
  });
});

test("3a. missing artifact: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: false });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("artifact not readable");
    expect(calls.filter((c) => c.args[1] === "delete").length).toBe(1);
  });
});

test("3b. wrong artifact schema: run fails, session deleted", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({
      workspace: dirs.workspace,
      artifactBody: JSON.stringify({ schema_version: 2, status: "success", run_id: "other" }),
    });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(calls.filter((c) => c.args[1] === "delete").length).toBe(1);
  });
});

test("4. cleanup failure: overall result can never be success", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: true, deleteCode: 1 });
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

test("5. launcher credential never reaches worker args/env; child token does", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: true });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner),
    );
    expect(outcome.exitCode).toBe(0);

    const runCall = calls.find((c) => c.args[0] === "run")!;
    expect(runCall.env.DOCKER_HELPER_SESSION_TOKEN).toBe(CHILD_TOKEN);

    const envPairs = runCall.args.filter((a) => a.includes("=") && /^[A-Z_]+=/u.test(a));
    expect(envPairs).toContain(`DOCKER_HELPER_SESSION_TOKEN=${CHILD_TOKEN}`);
    expect(envPairs.some((a) => a.startsWith("SMOKE_RUN_ID="))).toBe(true);

    for (const call of calls) {
      const serialized = JSON.stringify(call);
      expect(serialized.includes(LAUNCHER_TOKEN)).toBe(false);
      expect(Object.keys(call.env)).not.toContain("DOCKER_HELPER_CREDENTIAL_TOKEN");
    }

    const spec = smokeWorkerSpec("run-x", CHILD_TOKEN, "alpine:3.22");
    expect(Object.keys(spec.containerEnv).sort()).toEqual([
      "DOCKER_HELPER_SESSION_TOKEN",
      "SMOKE_RUN_ID",
    ]);
  });
});

test("6. signal after session creation triggers cleanup; SIGTERM exits 143", async () => {
  await withTempDirs(async (dirs) => {
    let signalHandler: ((signal: "SIGINT" | "SIGTERM") => void) | null = null;
    const { calls, runner, fireRunExit } = fakeCli({ workspace: dirs.workspace, writeArtifact: false, blockRun: true });
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
    expect(
      calls.some((c) => c.args[0] === "session" && c.args[1] === "create"),
    ).toBe(true);

    signalHandler!("SIGTERM");
    await Bun.sleep(10);
    expect(calls.some((c) => c.args[1] === "delete")).toBe(true);

    fireRunExit(143);

    const outcome = await pending;
    expect(outcome.sessionId).toBe(CHILD_SESSION_ID);
    expect(outcome.exitCode).toBe(143);
    expect(outcome.ok).toBe(false);
    expect(outcome.status).not.toBe("success");
  });
});

test("extra: endpoint unreachable fails before any session is created", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: true });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner, {
        fetchAuth: async () => {
          throw new Error("connect: connection refused");
        },
      }),
    );

    expect(outcome.exitCode).toBe(1);
    expect(calls.filter((c) => c.args[1] === "create").length).toBe(0);
    expect(calls.filter((c) => c.args[1] === "delete").length).toBe(0);
  });
});

test("extra: principal authority credential is rejected", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: true });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22" },
      makeDeps(dirs, runner, {
        fetchAuth: async () => ({ status: 200, body: { authority: "principal", principal: "michael" } }),
      }),
    );

    expect(outcome.exitCode).toBe(1);
    expect(calls.filter((c) => c.args[1] === "create").length).toBe(0);
  });
});

test("extra: wrong launcher id is rejected before creating a session", async () => {
  await withTempDirs(async (dirs) => {
    const { calls, runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: true });
    const outcome = await runSmoke(
      { workspace: dirs.workspace, workerImage: "alpine:3.22", launcherId: "dhl_expected" },
      makeDeps(dirs, runner),
    );

    expect(outcome.exitCode).toBe(1);
    expect(outcome.detail).toContain("dhl_expected");
    expect(calls.filter((c) => c.args[1] === "create").length).toBe(0);
    expect(calls.filter((c) => c.args[1] === "delete").length).toBe(0);
  });
});

test("extra: matching launcher id passes the guard", async () => {
  await withTempDirs(async (dirs) => {
    const { runner } = fakeCli({ workspace: dirs.workspace, writeArtifact: true });
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

test("extra: worker spec and seam helpers", () => {
  const spec = smokeWorkerSpec("run-1", CHILD_TOKEN, "alpine:3.22");
  const args = runArgs(spec, "/run/docker-helper/test.sock");
  expect(args[0]).toBe("run");
  expect(args).toContain("--image");
  expect(args).toContain("alpine:3.22");
  expect(args).toContain("--mount");
  expect(args).toContain(".:/workspace");
  expect(args).toContain(`DOCKER_HELPER_SESSION_TOKEN=${CHILD_TOKEN}`);
  expect(args).toContain("SMOKE_RUN_ID=run-1");
  expect(args[args.length - 4]).toBe("/bin/sh");
  expect(args[args.length - 1]).toContain("result.json");

  expect(resolveSocketPath(undefined)).toBe("/run/docker-helper/docker-helper.sock");
  expect(resolveSocketPath("/tmp/custom.sock")).toBe("/tmp/custom.sock");
  expect(credentialPath({ HOME: "/home/opencode" })).toBe(
    "/home/opencode/.config/docker-helper/credential.token",
  );
  expect(credentialPath({ XDG_CONFIG_HOME: "/xdg", HOME: "/home/opencode" })).toBe(
    "/xdg/docker-helper/credential.token",
  );
});

test("extra: state dir resolution", async () => {
  const { stateDir } = await import("../src/state.ts");
  expect(stateDir({ HOME: "/home/opencode" })).toBe("/home/opencode/.local/state/orchestrator");
  expect(stateDir({ XDG_STATE_HOME: "/xdg-state", HOME: "/home/opencode" })).toBe(
    "/xdg-state/orchestrator",
  );
});
