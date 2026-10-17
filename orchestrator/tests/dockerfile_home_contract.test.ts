/**
 * The base-image HOME contract: the runtime home of the unprivileged
 * worker/orchestrator user is pinned by our own base image instead of
 * being derived from the upstream image config or the container runtime.
 *
 * The upstream `ghcr.io/anomalyco/opencode` image sets no HOME and no
 * passwd entry for its uid, so the container runtime derives HOME=/ and
 * OpenCode fails with EACCES on /.local. Our Dockerfile_base creates the
 * passwd user and pins `ENV HOME=/home/opencode` (the passwd home), so
 * the contract holds regardless of upstream drift. These tests pin the
 * Dockerfile contract and prove that no parallel HOME fix exists in the
 * runner, the worker adapter, the profiles or the orchestrator image.
 */
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const repoRoot = join(import.meta.dir, "..", "..");
const baseDockerfile = readFileSync(join(repoRoot, "Dockerfile_base"), "utf8");
const orchestratorDockerfile = readFileSync(join(repoRoot, "Dockerfile_orchestrator"), "utf8");

const sourceOf = (relative: string): string =>
  readFileSync(join(import.meta.dir, "..", "src", relative), "utf8");

test("the base image pins the normalized HOME exactly once", () => {
  const lines = baseDockerfile.split("\n").filter((line) => line.startsWith("ENV HOME"));
  expect(lines).toEqual(["ENV HOME=/home/opencode"]);
});

test("the base image keeps the passwd user and the OpenCode home directories", () => {
  expect(baseDockerfile).toContain("addgroup -g 1000 opencode");
  expect(baseDockerfile).toContain("adduser -D -u 1000 -G opencode opencode");
  // the OpenCode config/cache/data locations live under the pinned home
  expect(baseDockerfile).toContain("/home/opencode/.config/opencode");
  expect(baseDockerfile).toContain("/home/opencode/.local/share/opencode");
  expect(baseDockerfile).toContain("/home/opencode/.cache");
  // the runtime user is the final USER directive: no root runtime
  const userLines = baseDockerfile.split("\n").filter((line) => line.startsWith("USER "));
  expect(userLines.at(-1)).toBe("USER opencode");
  // no blanket-permission workaround
  expect(baseDockerfile).not.toContain("chmod 777");
  expect(baseDockerfile).not.toContain("chmod -R 777");
});

test("the orchestrator image inherits the contract without its own HOME override", () => {
  expect(orchestratorDockerfile).not.toContain("ENV HOME");
  expect(orchestratorDockerfile).not.toContain("HOME=");
  expect(orchestratorDockerfile).toContain('ENTRYPOINT ["bun", "/opt/orchestrator/src/main.ts"]');
  const userLines = orchestratorDockerfile.split("\n").filter((line) => line.startsWith("USER "));
  expect(userLines.at(-1)).toBe("USER opencode");
});

test("the worker environment builders never override HOME", () => {
  // the resolved worker environment is built in the docker runtime
  // adapter; HOME must never appear there or in the worker transport
  for (const source of [
    sourceOf("pipeline_v2_docker_runtime.ts"),
    sourceOf("worker.ts"),
    sourceOf("worker_operation.ts"),
  ]) {
    expect(source).not.toContain("HOME");
  }
  // no hardcoded home override anywhere in the orchestrator sources
  const banned: string[] = [];
  for (const entry of ["HOME=/home/opencode", 'HOME="/home', "HOME='/home"]) {
    for (const file of ["pipeline_v2_runner.ts", "main.ts", "profile.ts"]) {
      if (sourceOf(file).includes(entry)) banned.push(file);
    }
  }
  expect(banned).toEqual([]);
});

test("profiles still cannot declare HOME as a source or destination", () => {
  // the profile env validator keeps HOME in the operator-path ban list
  const profile = sourceOf("profile.ts");
  expect(profile).toContain('"HOME"');
  expect(profile).toContain("XDG_CONFIG_HOME");
});
