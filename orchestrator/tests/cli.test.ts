import { expect, test } from "bun:test";
import { DEFAULT_PIPELINE_ROOT, parseCommand, usage } from "../src/cli_args.ts";

test("agent-smoke defaults to the bundled default pipeline root", () => {
  const parsed = parseCommand("agent-smoke", ["--config-root", "/abs/config"]);
  expect(parsed.kind).toBe("agent-smoke");
  if (parsed.kind !== "agent-smoke") {
    throw new Error("expected agent-smoke");
  }
  expect(parsed.pipelineRoot).toBe("/opt/orchestrator/pipelines/default");
  expect(parsed.configRoot).toBe("/abs/config");
  expect(parsed.workspace).toBe("/workspace");
  expect(parsed.launcherId).toBeUndefined();
});

test("explicit --pipeline-root and --workspace are accepted as absolute paths", () => {
  const parsed = parseCommand("agent-smoke", [
    "--config-root", "/cfg",
    "--pipeline-root", "/repo/pipelines/default",
    "--workspace", "/work",
    "--launcher-id", "dhl_x",
  ]);
  if (parsed.kind !== "agent-smoke") {
    throw new Error("expected agent-smoke");
  }
  expect(parsed.pipelineRoot).toBe("/repo/pipelines/default");
  expect(parsed.workspace).toBe("/work");
  expect(parsed.launcherId).toBe("dhl_x");
});

test("flag=value forms are accepted", () => {
  const parsed = parseCommand("agent-smoke", ["--config-root=/cfg", "--pipeline-root=/p"]);
  if (parsed.kind !== "agent-smoke") {
    throw new Error("expected agent-smoke");
  }
  expect(parsed.configRoot).toBe("/cfg");
  expect(parsed.pipelineRoot).toBe("/p");
});

test("relative paths are rejected", () => {
  expect(() => parseCommand("agent-smoke", ["--config-root", "rel"])).toThrow(/must be an absolute path/);
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--pipeline-root", "rel"])).toThrow(/must be an absolute path/);
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--workspace", "rel"])).toThrow(/must be an absolute path/);
});

test("the removed --profile and --task flags are rejected for agent-smoke", () => {
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--profile", "default"])).toThrow(
    /no longer accepts --profile; the execution profile is selected by the pipeline/,
  );
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--profile=default"])).toThrow(/--profile/);
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--task", "TASK.md"])).toThrow(
    /no longer accepts --task; the workspace input path is declared by the pipeline/,
  );
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--task=TASK.md"])).toThrow(/--task/);
});

test("--image stays rejected for agent-smoke", () => {
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--image", "x:1"])).toThrow(/does not accept --image/);
});

test("unknown flags are rejected", () => {
  expect(() => parseCommand("agent-smoke", ["--config-root", "/c", "--wat"])).toThrow(/unknown argument: --wat/);
  expect(() => parseCommand("smoke", ["--wat"])).toThrow(/unknown argument: --wat/);
});

test("smoke keeps its own contract", () => {
  const parsed = parseCommand("smoke", ["--image", "alpine:3.22", "--workspace", "/w"]);
  if (parsed.kind !== "smoke") {
    throw new Error("expected smoke");
  }
  expect(parsed.workerImage).toBe("alpine:3.22");
  expect(parsed.workspace).toBe("/w");
  expect(() => parseCommand("smoke", ["--profile", "default"])).toThrow(/unknown argument: --profile/);
  expect(() => parseCommand("smoke", ["--task", "TASK.md"])).toThrow(/unknown argument: --task/);
  expect(() => parseCommand("smoke", ["--pipeline-root", "/p"])).toThrow(/unknown argument: --pipeline-root/);
});

test("--config-root is required for agent-smoke", () => {
  expect(() => parseCommand("agent-smoke", [])).toThrow(/--config-root ABSOLUTE_PATH is required/);
});

test("usage documents the pipeline-driven agent-smoke contract", () => {
  const text = usage();
  expect(text).toContain("--pipeline-root PATH");
  expect(text).toContain("/opt/orchestrator/pipelines/default");
  expect(text).toContain("no --profile, --task, or --image flag");
  expect(text).toContain("one-step execution shape");
  expect(text).toContain("timeout_seconds");
  expect(text).not.toContain("--profile NAME");
  expect(text).not.toContain("--task PATH");
});
