import { expect, test } from "bun:test";
import { SubprocessCliRunner, type CliRunOptions } from "../src/docker_helper.ts";
import { childSessionEnv } from "../src/lifecycle.ts";
import { chmod, mkdtemp, open, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const BEARER_ENV = childSessionEnv("dht_runner_test_token");

/**
 * Installs a fake `docker-helper` executable for the duration of the test.
 * Bun resolves the executable through the PATH in the CHILD environment, so
 * the tests pass the shim directory as the first PATH entry in the env given
 * to `run()` (the runner forwards the caller's env verbatim; production keeps
 * a bearer-only env and relies on the parent process PATH). System paths stay
 * appended so the shim can still use ordinary commands.
 */
async function withShim(
  scriptBody: (dir: string) => string,
  fn: (dir: string) => Promise<void>,
): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), "dh-shim-"));
  const shim = join(dir, "docker-helper");
  await writeFile(shim, scriptBody(dir));
  await chmod(shim, 0o755);
  try {
    await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

function shimEnv(dir: string, extra: Record<string, string> = {}): Record<string, string> {
  return { ...BEARER_ENV, PATH: `${dir}:${process.env.PATH ?? ""}`, ...extra };
}

/**
 * Creates a fifo pair for deterministic process barriers: the shim blocks on
 * `read < control` (the test opens the writer to confirm the process has
 * started), and a trap or script writes into `caught` where the test is the
 * reader. No sleep-based synchronization is used.
 */
async function makeFifo(dir: string, name: string): Promise<string> {
  const fifo = join(dir, name);
  Bun.spawnSync(["mkfifo", fifo]);
  return fifo;
}

test("runner timeout: long-running worker run is terminated at the deadline after confirmed start", async () => {
  await withShim(
    () => '#!/bin/sh\nread line < "$CONTROL_FIFO"\nexec sleep 30\n',
    async (dir) => {
      const controlFifo = await makeFifo(dir, "control");
      const runner = new SubprocessCliRunner();
      const started = Date.now();
      const pending = runner.run(["run", "--endpoint", "/sock", "--image", "x"], shimEnv(dir, { CONTROL_FIFO: controlFifo }), "capture", {
        signalOnAbort: true,
        timeoutSeconds: 1,
      });
      const control = await open(controlFifo, "w");
      await control.close();
      const result = await pending;
      const elapsed = Date.now() - started;
      expect(result.timedOut).toBe(true);
      expect(result.code).toBe(143);
      expect(elapsed).toBeGreaterThanOrEqual(900);
      expect(elapsed).toBeLessThan(10000);
    },
  );
});

test("runner without timeout: fast process completes normally", async () => {
  await withShim(() => "#!/bin/sh\nexit 0\n", async (dir) => {
    const runner = new SubprocessCliRunner();
    const result = await runner.run(["pull", "--endpoint", "/s", "img"], shimEnv(dir), "capture");
    expect(result.code).toBe(0);
    expect(result.timedOut).toBe(false);
  });
});

test("timeout keeps already produced stdout of the terminated process", async () => {
  await withShim(
    (dir) =>
      `#!/bin/sh\nprintf 'worker output before timeout\\n'\nread line < "$CONTROL_FIFO"\nexec sleep 30 # ${dir}\n`,
    async (dir) => {
      const controlFifo = await makeFifo(dir, "control");
      const runner = new SubprocessCliRunner();
      const pending = runner.run(["run", "--endpoint", "/s"], shimEnv(dir, { CONTROL_FIFO: controlFifo }), "capture", {
        signalOnAbort: true,
        timeoutSeconds: 1,
      });
      const control = await open(controlFifo, "w");
      await control.close();
      const result = await pending;
      expect(result.timedOut).toBe(true);
      expect(result.stdout).toContain("worker output before timeout");
    },
  );
});

test("user signal wins over the pending timeout: no timeout classification, signal exit code", async () => {
  await withShim(
    () => '#!/bin/sh\nread line < "$CONTROL_FIFO"\nexec sleep 30\n',
    async (dir) => {
      const controlFifo = await makeFifo(dir, "control");
      const runner = new SubprocessCliRunner();
      const pending = runner.run(["run", "--endpoint", "/sock"], shimEnv(dir, { CONTROL_FIFO: controlFifo }), "capture", {
        signalOnAbort: true,
        timeoutSeconds: 30_000,
      });
      // the shim confirmed it started (it blocked on reading the control fifo)
      const control = await open(controlFifo, "w");
      await control.close();

      const accepted = runner.killActive("SIGINT");
      expect(accepted).toBe(true);
      const result = await pending;
      expect(result.timedOut).toBe(false);
      expect(result.code).toBe(130);
    },
  );
});

test("timeout wins: a late user signal cannot reclassify the termination", async () => {
  await withShim(
    () => [
      "#!/bin/sh",
      "sleep 30 >/dev/null 2>&1 &",
      "bg=$!",
      'trap \'printf t > "$CAUGHT_FIFO"; read line < "$RELEASE_FIFO"; kill "$bg" 2>/dev/null; exit 42\' TERM',
      "wait",
    ].join("\n"),
    async (dir) => {
      const caughtFifo = await makeFifo(dir, "caught");
      const releaseFifo = await makeFifo(dir, "release");
      const runner = new SubprocessCliRunner();
      const pending = runner.run(
        ["run", "--endpoint", "/sock"],
        shimEnv(dir, { CAUGHT_FIFO: caughtFifo, RELEASE_FIFO: releaseFifo }),
        "capture",
        { signalOnAbort: true, timeoutSeconds: 1 },
      );
      // wait until the timer's SIGTERM was actually caught by the running
      // shim (the trap's fifo write unblocks here); the shim stays alive
      // inside the trap until the test opens the release fifo
      const caught = await open(caughtFifo, "r");
      const trapMarker = (await caught.readFile()).toString("utf8");
      await caught.close();
      expect(trapMarker).toBe("t");

      // a late user signal must not reclassify the timeout and must not
      // reach the lifecycle as a user abort
      const accepted = runner.killActive("SIGINT");
      expect(accepted).toBe(false);

      const release = await open(releaseFifo, "w");
      await release.close();
      const result = await pending;
      expect(result.timedOut).toBe(true);
      expect(result.code).toBe(42);
    },
  );
});

test("timer is always cleared: a non-timeout run with a large deadline completes normally", async () => {
  await withShim(() => "#!/bin/sh\nexit 0\n", async (dir) => {
    const runner = new SubprocessCliRunner();
    const result = await runner.run(["run", "--x"], shimEnv(dir), "capture", {
      signalOnAbort: true,
      timeoutSeconds: 10_000,
    });
    expect(result.timedOut).toBe(false);
    expect(result.code).toBe(0);
  });
});

test("invalid timeout bounds never set a timer", async () => {
  await withShim(() => "#!/bin/sh\nexit 7\n", async (dir) => {
    const runner = new SubprocessCliRunner();
    for (const timeoutSeconds of [0, -5, 2147484]) {
      const result = await runner.run(["run"], shimEnv(dir), "capture", {
        signalOnAbort: true,
        timeoutSeconds,
      } satisfies CliRunOptions);
      expect(result.code).toBe(7);
      expect(result.timedOut).toBe(false);
    }
  });
});

test("runner spawns docker-helper verbatim: exact argv and env passthrough", async () => {
  await withShim(
    (dir) =>
      [
        "#!/bin/sh",
        `printf '%s\\n' "$@" > ${join(dir, "args.txt")}`,
        `env > ${join(dir, "env.txt")}`,
        "exit 0",
      ].join("\n"),
    async (dir) => {
      const runner = new SubprocessCliRunner();
      const result = await runner.run(
        ["run", "--endpoint", "/sock", "--image", "img", "--", "opencode"],
        shimEnv(dir),
        "capture",
        { signalOnAbort: true, timeoutSeconds: 3600 },
      );
      expect(result.code).toBe(0);
      const recorded = await readFile(join(dir, "args.txt"), "utf8");
      expect(recorded.split("\n").filter((line) => line !== "")).toEqual([
        "run",
        "--endpoint",
        "/sock",
        "--image",
        "img",
        "--",
        "opencode",
      ]);
      const envText = await readFile(join(dir, "env.txt"), "utf8");
      const envKeys = envText
        .split("\n")
        .filter((line) => line.startsWith("DOCKER_HELPER_") || line.startsWith("PATH="))
        .map((line) => line.slice(0, line.indexOf("=")))
        .sort();
      expect(envKeys).toEqual(["DOCKER_HELPER_SESSION_TOKEN", "PATH"]);
      expect(envText).toContain(`DOCKER_HELPER_SESSION_TOKEN=dht_runner_test_token`);
    },
  );
});

test("killActive without an active signalable worker still reports a recordable abort", () => {
  const runner = new SubprocessCliRunner();
  expect(runner.killActive("SIGINT")).toBe(true);
});
