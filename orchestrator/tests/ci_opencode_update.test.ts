/**
 * The automatic OpenCode V1 update channel.
 *
 * Upstream `ghcr.io/anomalyco/opencode:latest` is a mutable tag, and the
 * existing diff-based image pipeline only rebuilds when repository files
 * change - a changed upstream tag content alone never triggers anything.
 * The channel (this file pins its contract):
 *
 *   - `Dockerfile_base` pins the upstream tag with a tracked manifest
 *     digest; the digest owns the exact build content and is the only
 *     place the upstream identity lives (no separately stored version
 *     string - the version is read from the image itself);
 *   - `.github/workflows/opencode-updater.yml` re-resolves the upstream
 *     digest on a schedule and on demand, proves the content by running
 *     the image (`opencode --version`), and opens a minimal PR changing
 *     only the tracked digest when a new OpenCode 1.x appears;
 *   - `scripts/opencode-update-gate.sh` is the single owner of the
 *     fail-closed decisions: only a strict `1.x.y` is accepted, V2 (or
 *     any other major), unrecognized output, a failed run and a digest
 *     mismatch refuse before any commit, PR or image publication;
 *   - `scripts/build-images.sh` runs the same gate on every freshly
 *     built base image, so a manual Dockerfile change that smuggles in
 *     V2 content is blocked by the ordinary build pipeline itself.
 *
 * The tests drive the real scripts with a fake `docker`/`curl` on PATH
 * and pin the workflow structure; they never touch the network.
 */
import { expect, test } from "bun:test";
import { chmodSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const repoRoot = join(import.meta.dir, "..", "..");
const gateScript = join(repoRoot, "scripts", "opencode-update-gate.sh");
const buildImagesScript = join(repoRoot, "scripts", "build-images.sh");
const baseDockerfile = readFileSync(join(repoRoot, "Dockerfile_base"), "utf8");

const DIGEST_A = `sha256:${"a".repeat(64)}`;
const DIGEST_B = `sha256:${"b".repeat(64)}`;

interface RunResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

interface SpawnOptions {
  env?: Record<string, string>;
  fakeBin?: string;
  cwd?: string;
}

function spawn(script: string, args: string[], options: SpawnOptions = {}): RunResult {
  return spawnCmd(["bash", script, ...args], options);
}

function spawnCmd(cmd: string[], options: SpawnOptions = {}): RunResult {
  const env: Record<string, string> = {
    ...(process.env as unknown as Record<string, string>),
    ...options.env,
  };
  if (options.fakeBin !== undefined) {
    env.PATH = `${options.fakeBin}:${process.env.PATH ?? ""}`;
  }
  const proc = Bun.spawnSync(cmd, {
    cwd: options.cwd ?? repoRoot,
    env,
    stdout: "pipe",
    stderr: "pipe",
  });
  return {
    exitCode: proc.exitCode ?? -1,
    stdout: proc.stdout ? proc.stdout.toString() : "",
    stderr: proc.stderr ? proc.stderr.toString() : "",
  };
}

function runGate(args: string[], options: SpawnOptions = {}): RunResult {
  return spawn(gateScript, args, options);
}

const FAKE_DOCKER = `#!/usr/bin/env bash
printf 'docker %s\\n' "$*" >> "$FAKE_DOCKER_LOG"
case "$1" in
  pull)
    exit "\${FAKE_PULL_EXIT:-0}"
    ;;
  image)
    printf '%s@sha256:%s\\n' "ghcr.io/anomalyco/opencode" "$FAKE_REPO_DIGEST"
    exit 0
    ;;
  run)
    if [[ -n "\${FAKE_RUN_EXIT:-}" && "$FAKE_RUN_EXIT" != "0" ]]; then
      echo "simulated container failure" >&2
      exit "$FAKE_RUN_EXIT"
    fi
    printf '%s\\n' "$FAKE_RUN_VERSION"
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
`;

const FAKE_CURL = `#!/usr/bin/env bash
printf 'curl %s\\n' "$*" >> "$FAKE_CURL_LOG"
case "$*" in
  *"token?"*)
    if [[ -n "\${FAKE_TOKEN_FAIL:-}" ]]; then exit 1; fi
    printf '{"token":"fake-token"}\\n'
    exit 0
    ;;
  */manifests/*)
    if [[ -n "\${FAKE_MANIFEST_FAIL:-}" ]]; then exit 1; fi
    printf 'HTTP/2 200\\ndocker-content-digest: %s\\n\\n' "$FAKE_RESOLVED_DIGEST"
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
`;

let scratchCounter = 0;

interface FakeRegistry {
  bin: string;
  dockerLog: string;
  curlLog: string;
  env: Record<string, string>;
}

function makeFakeRegistry(params: {
  resolvedDigest: string;
  repoDigest?: string;
  runVersion?: string;
  runExit?: string;
  manifestFail?: boolean;
}): FakeRegistry {
  scratchCounter += 1;
  const dir = join(
    tmpdir(),
    "ci-opencode-update-fakes",
    `fake-${scratchCounter}-${process.pid}`,
  );
  mkdirSync(dir, { recursive: true });
  for (const [name, content] of [
    ["docker", FAKE_DOCKER],
    ["curl", FAKE_CURL],
  ] as const) {
    const path = join(dir, name);
    writeFileSync(path, content);
    chmodSync(path, 0o755);
  }
  const dockerLog = `${dir}/docker.log`;
  const curlLog = `${dir}/curl.log`;
  const env: Record<string, string> = {
    FAKE_DOCKER_LOG: dockerLog,
    FAKE_CURL_LOG: curlLog,
    FAKE_RESOLVED_DIGEST: params.resolvedDigest,
    FAKE_REPO_DIGEST: params.repoDigest ?? params.resolvedDigest.replace(/^sha256:/, ""),
    FAKE_RUN_VERSION: params.runVersion ?? "",
  };
  if (params.runExit !== undefined) env.FAKE_RUN_EXIT = params.runExit;
  if (params.manifestFail) env.FAKE_MANIFEST_FAIL = "1";
  return { bin: dir, dockerLog, curlLog, env };
}

function cleanupFakeRegistry(registry: FakeRegistry): void {
  rmSync(registry.bin, { recursive: true, force: true });
}

function dockerCalls(registry: FakeRegistry): string[] {
  try {
    return readFileSync(registry.dockerLog, "utf8")
      .split("\n")
      .filter((line) => line.length > 0)
      .map((line) => line.replace(/^docker /, ""));
  } catch {
    return [];
  }
}

function curlCalls(registry: FakeRegistry): string[] {
  try {
    return readFileSync(registry.curlLog, "utf8")
      .split("\n")
      .filter((line) => line.length > 0);
  } catch {
    return [];
  }
}

function parseGateOutput(stdout: string): Map<string, string> {
  const map = new Map<string, string>();
  for (const line of stdout.split("\n")) {
    const eq = line.indexOf("=");
    if (eq > 0) map.set(line.slice(0, eq), line.slice(eq + 1));
  }
  return map;
}

// ---------------------------------------------------------------------------
// Dockerfile_base: the tracked upstream digest pin.
// ---------------------------------------------------------------------------

test("the base image pins the upstream tag by a tracked manifest digest", () => {
  const fromLines = baseDockerfile
    .split("\n")
    .filter((line) => line.startsWith("FROM ghcr.io/anomalyco/opencode:"));
  expect(fromLines).toHaveLength(1);
  expect(fromLines[0]).toMatch(/^FROM ghcr\.io\/anomalyco\/opencode:latest@sha256:[0-9a-f]{64}$/);
});

test("the update channel stores no OpenCode version string", () => {
  // The version is read from the image itself; the digest is the only
  // tracked upstream identity. No hand-edited version line anywhere in
  // the channel.
  const banned = [/\b1\.19\.5\b/, /\b2\.2\.5\b/];
  for (const relative of [
    "Dockerfile_base",
    "scripts/opencode-update-gate.sh",
    ".github/workflows/opencode-updater.yml",
  ]) {
    for (const pattern of banned) {
      expect(pattern.test(readFileSync(join(repoRoot, relative), "utf8"))).toBe(false);
    }
  }
});

// ---------------------------------------------------------------------------
// The pure decision: decide <tracked> <resolved> <version|->.
// ---------------------------------------------------------------------------

test("decide: an unchanged resolved digest exits as unchanged without a version proof", () => {
  const result = runGate(["decide", DIGEST_A, DIGEST_A, "-"]);
  expect(result.exitCode).toBe(0);
  expect(parseGateOutput(result.stdout).get("decision")).toBe("unchanged");
  expect(result.stdout).not.toContain("new_digest");
});

test("decide: a changed digest with a proven 1.x.y is an update", () => {
  const result = runGate(["decide", DIGEST_A, DIGEST_B, "1.20.0"]);
  expect(result.exitCode).toBe(0);
  const output = parseGateOutput(result.stdout);
  expect(output.get("decision")).toBe("update");
  expect(output.get("new_digest")).toBe(DIGEST_B);
  expect(output.get("opencode_version")).toBe("1.20.0");
});

test("decide: every non-1.x major is refused", () => {
  for (const version of ["2.2.5", "3.0.0", "10.1.2"]) {
    const result = runGate(["decide", DIGEST_A, DIGEST_B, version]);
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("version_not_v1");
    expect(result.stdout).not.toContain("decision=update");
  }
});

test("decide: unrecognized version output is refused", () => {
  for (const version of ["", "opencode 1.19.5", "1.19", "1.19.5-beta", "some error text"]) {
    const result = runGate(["decide", DIGEST_A, DIGEST_B, version]);
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("version_unrecognized");
    expect(result.stdout).not.toContain("decision=update");
  }
});

test("decide: changed content without a proven version is refused", () => {
  const result = runGate(["decide", DIGEST_A, DIGEST_B, "-"]);
  expect(result.exitCode).toBe(1);
  expect(result.stderr).toContain("version_unavailable");
});

test("decide: non-digest arguments are refused", () => {
  for (const tracked of ["", "latest", `sha256:${"g".repeat(64)}`]) {
    const result = runGate(["decide", tracked, DIGEST_B, "1.19.5"]);
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("digest_invalid");
  }
});

// ---------------------------------------------------------------------------
// The build gate: check-version <image-ref> (no environment override -
// the run also proves the base image HOME contract).
// ---------------------------------------------------------------------------

test("check-version: a proven 1.x.y passes", () => {
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, runVersion: "1.19.5" });
  try {
    const result = runGate(["check-version", "ghcr.io/kivarun/opencode-docker/base:abc"], {
      fakeBin: registry.bin,
      env: registry.env,
    });
    expect(result.exitCode).toBe(0);
    expect(parseGateOutput(result.stdout).get("version")).toBe("1.19.5");
    const runCall = dockerCalls(registry).find((line) => line.startsWith("run "));
    expect(runCall).toBeDefined();
    expect(runCall).toContain("--entrypoint opencode");
    // No HOME override: the built base image must satisfy its own
    // HOME contract without environment help.
    expect(runCall).not.toContain("HOME=");
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("check-version: V2 content stops the build", () => {
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, runVersion: "2.2.5" });
  try {
    const result = runGate(["check-version", "some-image"], {
      fakeBin: registry.bin,
      env: registry.env,
    });
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("version_not_v1");
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("check-version: unrecognized output, empty output and failed runs refuse", () => {
  const cases: Array<{ params: { runVersion?: string; runExit?: string }; reason: string }> = [
    { params: { runVersion: "garbage" }, reason: "version_unrecognized" },
    { params: { runVersion: "" }, reason: "version_unrecognized" },
    { params: { runExit: "7" }, reason: "version_unavailable" },
  ];
  for (const item of cases) {
    const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, ...item.params });
    try {
      const result = runGate(["check-version", "some-image"], {
        fakeBin: registry.bin,
        env: registry.env,
      });
      expect(result.exitCode).toBe(1);
      expect(result.stderr).toContain(item.reason);
    } finally {
      cleanupFakeRegistry(registry);
    }
  }
});

test("check-version: a missing image reference is refused", () => {
  const result = runGate(["check-version", ""]);
  expect(result.exitCode).toBe(1);
  expect(result.stderr).toContain("invalid_options");
});

// ---------------------------------------------------------------------------
// The full updater flow: run (resolve -> compare -> probe -> decide).
// ---------------------------------------------------------------------------

test("run: an unchanged digest exits unchanged and never runs the image", () => {
  const pinned = /FROM ghcr\.io\/anomalyco\/opencode:latest@sha256:([0-9a-f]{64})/
    .exec(baseDockerfile)![1];
  const registry = makeFakeRegistry({ resolvedDigest: `sha256:${pinned}` });
  try {
    const result = runGate(["run"], { fakeBin: registry.bin, env: registry.env });
    expect(result.exitCode).toBe(0);
    const output = parseGateOutput(result.stdout);
    expect(output.get("decision")).toBe("unchanged");
    expect(output.get("tracked_digest")).toBe(`sha256:${pinned}`);
    expect(output.get("resolved_digest")).toBe(`sha256:${pinned}`);
    // The image is never pulled or run for an unchanged digest.
    expect(dockerCalls(registry)).toEqual([]);
    expect(curlCalls(registry)).toHaveLength(2);
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("run: a new digest with proven V1 content is an update", () => {
  const registry = makeFakeRegistry({
    resolvedDigest: DIGEST_B,
    runVersion: "1.20.0",
  });
  try {
    const result = runGate(["run"], { fakeBin: registry.bin, env: registry.env });
    expect(result.exitCode).toBe(0);
    const output = parseGateOutput(result.stdout);
    expect(output.get("decision")).toBe("update");
    expect(output.get("new_digest")).toBe(DIGEST_B);
    expect(output.get("opencode_version")).toBe("1.20.0");
    // The probe pulls the tag, proves the daemon resolved the registry
    // digest, then runs the image at the exact digest.
    const calls = dockerCalls(registry);
    expect(calls.some((line) => line.startsWith("pull ghcr.io/anomalyco/opencode:latest"))).toBe(true);
    expect(calls.some((line) => line.startsWith("image inspect"))).toBe(true);
    const runCall = calls.find((line) => line.startsWith("run "));
    expect(runCall).toContain(`ghcr.io/anomalyco/opencode@${DIGEST_B}`);
    expect(runCall).toContain("--env HOME=/tmp/opencode-version-probe");
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("run: V2 content refuses before any update decision", () => {
  const registry = makeFakeRegistry({
    resolvedDigest: DIGEST_B,
    runVersion: "2.2.5",
  });
  try {
    const result = runGate(["run"], { fakeBin: registry.bin, env: registry.env });
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("version_not_v1");
    expect(result.stdout).not.toContain("decision=update");
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("run: the pulled tag must resolve to the registry digest", () => {
  const registry = makeFakeRegistry({
    resolvedDigest: DIGEST_B,
    repoDigest: "c".repeat(64),
    runVersion: "1.20.0",
  });
  try {
    const result = runGate(["run"], { fakeBin: registry.bin, env: registry.env });
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("digest_mismatch");
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("run: a failed image run refuses", () => {
  const registry = makeFakeRegistry({
    resolvedDigest: DIGEST_B,
    runVersion: "1.20.0",
    runExit: "7",
  });
  try {
    const result = runGate(["run"], { fakeBin: registry.bin, env: registry.env });
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("version_unavailable");
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("run: an unresolvable upstream digest refuses", () => {
  const registry = makeFakeRegistry({
    resolvedDigest: DIGEST_B,
    manifestFail: true,
  });
  try {
    const result = runGate(["run"], { fakeBin: registry.bin, env: registry.env });
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("digest_unresolved");
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("run: the probe result reaches the GITHUB_OUTPUT file", () => {
  scratchCounter += 1;
  const outputFile = join(
    tmpdir(),
    "ci-opencode-update-fakes",
    `output-${scratchCounter}-${process.pid}.txt`,
  );
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_B, runVersion: "1.20.0" });
  try {
    const result = runGate(["run"], {
      fakeBin: registry.bin,
      env: { ...registry.env, GITHUB_OUTPUT: outputFile },
    });
    expect(result.exitCode).toBe(0);
    const content = readFileSync(outputFile, "utf8");
    expect(content).toContain("decision=update");
    expect(content).toContain(`new_digest=${DIGEST_B}`);
    expect(content).toContain("opencode_version=1.20.0");
  } finally {
    rmSync(outputFile, { force: true });
    cleanupFakeRegistry(registry);
  }
});

// ---------------------------------------------------------------------------
// The build pipeline integration: the gate runs on every base build.
// ---------------------------------------------------------------------------

function buildBaseEnv(registry: FakeRegistry, extra: Record<string, string> = {}): Record<string, string> {
  return {
    ...registry.env,
    EVENT_NAME: "push",
    COMMIT_SHA: "deadbeef00000000000000000000000000000000",
    BRANCH: "main",
    DEFAULT_BRANCH: "main",
    REGISTRY_PREFIX: "ghcr.io/kivarun/opencode-docker",
    ...extra,
  };
}

test("build base: the gate runs after the build and before any push", () => {
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, runVersion: "1.19.5" });
  try {
    const result = spawn(
      buildImagesScript,
      ["build", "base"],
      {
        fakeBin: registry.bin,
        env: buildBaseEnv(registry, { PUBLISH: "1" }),
      },
    );
    expect(result.exitCode).toBe(0);
    const calls = dockerCalls(registry);
    const buildIndex = calls.findIndex((line) => line.startsWith("build "));
    const gateIndex = calls.findIndex((line) => line.startsWith("run "));
    const pushIndex = calls.findIndex((line) => line.startsWith("push "));
    expect(buildIndex).toBeGreaterThanOrEqual(0);
    expect(gateIndex).toBeGreaterThan(buildIndex);
    expect(pushIndex).toBeGreaterThan(gateIndex);
    expect(calls.some((line) => line.includes("base:latest"))).toBe(true);
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("build base: V2 content from a manual Dockerfile edit is blocked", () => {
  // The gate is content-based: whatever the Dockerfile says, a built
  // base that does not prove OpenCode 1.x stops the ordinary build
  // pipeline before the image is saved or pushed.
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, runVersion: "2.2.5" });
  try {
    const result = spawn(
      buildImagesScript,
      ["build", "base"],
      {
        fakeBin: registry.bin,
        env: buildBaseEnv(registry, { PUBLISH: "1" }),
      },
    );
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain("version_not_v1");
    const calls = dockerCalls(registry);
    expect(calls.some((line) => line.startsWith("push "))).toBe(false);
    expect(calls.some((line) => line.includes(":latest"))).toBe(false);
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("build base (PR mode): the gate runs before the artifact is saved", () => {
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, runVersion: "1.19.5" });
  try {
    const saveTo = join(registry.bin, "base-image.tar.gz");
    const result = spawn(
      buildImagesScript,
      ["build", "base"],
      {
        fakeBin: registry.bin,
        env: buildBaseEnv(registry, { PUBLISH: "0", SAVE_BASE_TO: saveTo }),
      },
    );
    expect(result.exitCode).toBe(0);
    const calls = dockerCalls(registry);
    const gateIndex = calls.findIndex((line) => line.startsWith("run "));
    const saveIndex = calls.findIndex((line) => line.startsWith("save "));
    expect(gateIndex).toBeGreaterThanOrEqual(0);
    expect(saveIndex).toBeGreaterThan(gateIndex);
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("build base: unrecognized gate output also stops the build", () => {
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, runVersion: "" });
  try {
    const result = spawn(
      buildImagesScript,
      ["build", "base"],
      {
        fakeBin: registry.bin,
        env: buildBaseEnv(registry, { PUBLISH: "1" }),
      },
    );
    expect(result.exitCode).not.toBe(0);
    expect(result.stderr).toContain("version_unrecognized");
    expect(dockerCalls(registry).some((line) => line.startsWith("push "))).toBe(false);
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("build standalone: no version gate", () => {
  const registry = makeFakeRegistry({ resolvedDigest: DIGEST_A, runVersion: "1.19.5" });
  try {
    const result = spawn(
      buildImagesScript,
      ["build", "gdk"],
      {
        fakeBin: registry.bin,
        env: buildBaseEnv(registry, { PUBLISH: "0" }),
      },
    );
    expect(result.exitCode).toBe(0);
    expect(dockerCalls(registry).some((line) => line.startsWith("run "))).toBe(false);
  } finally {
    cleanupFakeRegistry(registry);
  }
});

test("plan: a changed upstream tag content without a repository diff selects nothing", () => {
  // The regression encoded from the red-before proof: the plan is
  // purely diff-based; upstream tag content is invisible to it, which
  // is why the updater workflow and the digest pin exist.
  const head = spawnCmd(["git", "rev-parse", "HEAD"]).stdout.trim();
  const result = spawn(
    buildImagesScript,
    ["plan"],
    {
      env: {
        EVENT_NAME: "pull_request",
        COMMIT_SHA: head,
        PR_BASE_SHA: head,
      },
    },
  );
  expect(result.exitCode).toBe(0);
  const output = parseGateOutput(result.stdout);
  expect(output.get("base_changed")).toBe("false");
  expect(output.get("standalone")).toBe("[]");
  expect(output.get("dependent")).toBe("[]");
});

// ---------------------------------------------------------------------------
// The updater workflow structure.
// ---------------------------------------------------------------------------

interface WorkflowStep {
  name?: string;
  run?: string;
  uses?: string;
  if?: string;
}

interface WorkflowJob {
  name?: string;
  permissions?: Record<string, string>;
  needs?: string[];
  if?: string;
  outputs?: Record<string, string>;
  steps?: WorkflowStep[];
}

interface UpdaterWorkflow {
  name: string;
  on: Record<string, unknown>;
  permissions: Record<string, string>;
  jobs: Record<string, WorkflowJob>;
}

const updaterWorkflow = Bun.YAML.parse(
  readFileSync(join(repoRoot, ".github", "workflows", "opencode-updater.yml"), "utf8"),
) as unknown as UpdaterWorkflow;

const updaterJobs = updaterWorkflow.jobs;
const probeJob = updaterJobs.probe;
const pullRequestJob = updaterJobs["pull-request"];
if (probeJob === undefined || pullRequestJob === undefined) {
  throw new Error("the updater workflow must declare the probe and pull-request jobs");
}

test("the updater runs on a schedule and on demand only", () => {
  const triggers = updaterWorkflow.on;
  expect(Object.keys(triggers).sort()).toEqual(["schedule", "workflow_dispatch"]);
  const schedule = triggers.schedule as Array<{ cron: string }> | undefined;
  expect(schedule?.[0]?.cron).toMatch(/^\d+ \d+ \* \* \*$/);
});

test("the updater jobs carry exactly the permissions they need", () => {
  expect(updaterWorkflow.permissions).toEqual({ contents: "read" });
  expect(probeJob.permissions).toEqual({ contents: "read" });
  expect(pullRequestJob.permissions).toEqual({
    contents: "write",
    "pull-requests": "write",
  });
});

test("the probe job drives the gate and exports its decision", () => {
  expect(probeJob.outputs).toEqual({
    decision: "${{ steps.gate.outputs.decision }}",
    "new-digest": "${{ steps.gate.outputs.new_digest }}",
    "opencode-version": "${{ steps.gate.outputs.opencode_version }}",
  });
  const gateStep = probeJob.steps?.find((step) =>
    step.run?.includes("opencode-update-gate.sh run"),
  );
  expect(gateStep).toBeDefined();
  // The probe job never builds or pushes images.
  for (const step of probeJob.steps ?? []) {
    expect(step.run ?? "").not.toMatch(/docker|build-images/);
  }
});

test("the pull-request job opens a minimal digest-only PR", () => {
  expect(pullRequestJob.needs).toEqual(["probe"]);
  expect(pullRequestJob.if).toContain("needs.probe.outputs.decision == 'update'");
  const runs = (pullRequestJob.steps ?? [])
    .map((step) => step.run ?? "")
    .filter((run) => run.length > 0);
  expect(runs.length).toBeGreaterThanOrEqual(3);
  const updateStep = runs.find((run) => run.includes("sed -i -E"));
  expect(updateStep).toBeDefined();
  // The change is verified as exactly one file and checked for
  // whitespace errors before the commit.
  expect(updateStep).toContain("git diff --quiet -- Dockerfile_base");
  expect(updateStep).toContain("git status --porcelain");
  expect(updateStep).toContain("git diff --check");
  // The job never builds or pushes images.
  for (const run of runs) {
    expect(run).not.toMatch(/docker |build-images/);
  }
  expect(runs.some((run) => run.includes("gh pr create"))).toBe(true);
});
