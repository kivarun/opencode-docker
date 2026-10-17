import { expect, test } from "bun:test";
import { resolvePipelineV2StateRootProjection } from "../src/pipeline_v2_state_root.ts";

type Env = Record<string, string | undefined>;

function expectError(env: Env, message: string): void {
  expect(() => resolvePipelineV2StateRootProjection(env)).toThrow(message);
}

test("host mode: no daemon variable means daemon root equals local root", () => {
  const projection = resolvePipelineV2StateRootProjection({
    ORCHESTRATOR_STATE_ROOT: "/srv/orchestrator-state",
  });
  expect(projection).toEqual({ localRoot: "/srv/orchestrator-state", daemonRoot: "/srv/orchestrator-state" });
});

test("both variables set: daemon root is the separate daemon path", () => {
  const projection = resolvePipelineV2StateRootProjection({
    ORCHESTRATOR_STATE_ROOT: "/srv/orchestrator-state",
    ORCHESTRATOR_DAEMON_STATE_ROOT: "/srv/daemon-state",
  });
  expect(projection).toEqual({ localRoot: "/srv/orchestrator-state", daemonRoot: "/srv/daemon-state" });
});

test("XDG_STATE_HOME fallback appends /orchestrator", () => {
  const projection = resolvePipelineV2StateRootProjection({ XDG_STATE_HOME: "/xdg/state" });
  expect(projection).toEqual({ localRoot: "/xdg/state/orchestrator", daemonRoot: "/xdg/state/orchestrator" });
});

test("HOME fallback builds ~/.local/state/orchestrator", () => {
  const projection = resolvePipelineV2StateRootProjection({ HOME: "/home/op" });
  expect(projection).toEqual({ localRoot: "/home/op/.local/state/orchestrator", daemonRoot: "/home/op/.local/state/orchestrator" });
});

test("explicit ORCHESTRATOR_STATE_ROOT wins over the XDG/HOME fallbacks", () => {
  const projection = resolvePipelineV2StateRootProjection({
    ORCHESTRATOR_STATE_ROOT: "/explicit",
    XDG_STATE_HOME: "/xdg/state",
    HOME: "/home/op",
  });
  expect(projection.localRoot).toBe("/explicit");
});

test("XDG_STATE_HOME wins over HOME", () => {
  const projection = resolvePipelineV2StateRootProjection({ XDG_STATE_HOME: "/xdg/state", HOME: "/home/op" });
  expect(projection.localRoot).toBe("/xdg/state/orchestrator");
});

test("no source at all is a CLI configuration error", () => {
  expectError(
    {},
    "cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)",
  );
});

const UNCLEAN_FORMS: Array<[string, string]> = [
  ["", "non-empty absolute path"],
  ["relative/state", "absolute path"],
  [" /spaced/state", "leading or trailing whitespace"],
  ["/spaced/state ", "leading or trailing whitespace"],
  ["/state//run", "clean absolute path"],
  ["/state/run/", "clean absolute path"],
  ["/state/../other", "clean absolute path"],
  ["/state/./run", "clean absolute path"],
  ["/..", "clean absolute path"],
  ["/.", "clean absolute path"],
  ["/state\0run", "NUL character"],
];

test("unclean forms are rejected through every env source", () => {
  for (const [value, expected] of UNCLEAN_FORMS) {
    expectError({ ORCHESTRATOR_STATE_ROOT: value }, expected);
    expectError({ ORCHESTRATOR_STATE_ROOT: "/ok", ORCHESTRATOR_DAEMON_STATE_ROOT: value }, expected);
    expectError({ XDG_STATE_HOME: value }, expected);
    expectError({ HOME: value }, expected);
  }
});

test("the root / and ordinary clean paths are accepted", () => {
  expect(resolvePipelineV2StateRootProjection({ ORCHESTRATOR_STATE_ROOT: "/" })).toEqual({
    localRoot: "/",
    daemonRoot: "/",
  });
  expect(
    resolvePipelineV2StateRootProjection({ ORCHESTRATOR_STATE_ROOT: "/state/run", ORCHESTRATOR_DAEMON_STATE_ROOT: "/" }),
  ).toEqual({ localRoot: "/state/run", daemonRoot: "/" });
  expect(resolvePipelineV2StateRootProjection({ XDG_STATE_HOME: "/xdg/state" })).toEqual({
    localRoot: "/xdg/state/orchestrator",
    daemonRoot: "/xdg/state/orchestrator",
  });
  expect(resolvePipelineV2StateRootProjection({ HOME: "/" })).toEqual({
    localRoot: "/.local/state/orchestrator",
    daemonRoot: "/.local/state/orchestrator",
  });
  expect(resolvePipelineV2StateRootProjection({ HOME: "/home/op" })).toEqual({
    localRoot: "/home/op/.local/state/orchestrator",
    daemonRoot: "/home/op/.local/state/orchestrator",
  });
});

test("the resolver creates no filesystem objects", () => {
  const fs = require("node:fs");
  const probe = `/tmp/probe/state-root-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  expect(fs.existsSync(probe)).toBe(false);
  resolvePipelineV2StateRootProjection({ ORCHESTRATOR_STATE_ROOT: probe });
  expect(fs.existsSync(probe)).toBe(false);
});

test("errors never echo the offending environment values", () => {
  for (const env of [
    { ORCHESTRATOR_STATE_ROOT: "/leaky//value" },
    { ORCHESTRATOR_DAEMON_STATE_ROOT: "/leaky/dæmon value" },
    { XDG_STATE_HOME: "/leaky/xdg value" },
    { HOME: "/leaky/home value" },
  ] as Env[]) {
    try {
      resolvePipelineV2StateRootProjection(env);
      throw new Error("expected the resolver to reject");
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      expect(message).not.toContain("leaky");
      expect(message).not.toContain("dæmon");
    }
  }
});
