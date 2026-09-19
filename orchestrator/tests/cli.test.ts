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
  expect(() => parseCommand("smoke", ["--config-root", "/c"])).toThrow(/unknown argument: --config-root/);
  expect(() => parseCommand("smoke", ["--config-root=/c"])).toThrow(/unknown argument: --config-root=/);
});

test("--config-root is required for agent-smoke", () => {
  expect(() => parseCommand("agent-smoke", [])).toThrow(/--config-root ABSOLUTE_PATH is required/);
});

test("usage documents the pipeline-driven agent-smoke contract", () => {
  const text = usage();
  expect(text).toContain("--pipeline-root PATH");
  expect(text).toContain("/opt/orchestrator/pipelines/default");
  expect(text).toContain("no --profile, --task, or --image flag");
  expect(text).toContain("sequential states, branching and cycles bounded by max_transitions");
  expect(text).not.toContain("one-step");
  expect(text).toContain("timeout_seconds");
  expect(text).not.toContain("--profile NAME");
  expect(text).not.toContain("--task PATH");
});

test("usage documents the production pipeline v2 run command", () => {
  const text = usage();
  expect(text).toContain("run flags (production pipeline v2)");
  expect(text).toContain("--input ID=PATH");
  expect(text).toContain("--json");
  expect(text).toContain("ORCHESTRATOR_STATE_ROOT");
  expect(text).toContain("ORCHESTRATOR_DAEMON_STATE_ROOT");
  expect(text).toContain("pipeline-runs/<run-id>/state.json");
  expect(text).toContain("pipeline-runs/<run-id>/outputs");
  expect(text).toContain("agent-smoke is the v1 diagnostic command");
  expect(text).toContain("run-owned directory");
});

const RUN_BASE = ["--pipeline-root", "/abs/pipeline", "--config-root", "/abs/config", "--project", "/abs/project"];

function expectRunError(argv: string[], message: string): void {
  expect(() => parseCommand("run", argv)).toThrow(message);
}

test("run parses the full command with both flag forms", () => {
  const withValues = parseCommand("run", [...RUN_BASE, "--launcher-id", "dhl_x", "--json"]);
  expect(withValues).toEqual({
    kind: "run",
    pipelineRoot: "/abs/pipeline",
    configRoot: "/abs/config",
    projectSourcePath: "/abs/project",
    inputBindings: [],
    launcherId: "dhl_x",
    json: true,
  });
  const withEquals = parseCommand("run", ["--pipeline-root=/abs/pipeline", "--config-root=/abs/config", "--project=/abs/project"]);
  expect(withEquals.kind).toBe("run");
  if (withEquals.kind !== "run") {
    throw new Error("expected run");
  }
  expect(withEquals.json).toBe(false);
  expect(withEquals.launcherId).toBeUndefined();
  expect(withEquals.inputBindings).toEqual([]);
});

test("run preserves input declaration order and splits at the first =", () => {
  const parsed = parseCommand("run", [
    ...RUN_BASE,
    "--input", "alpha=/abs/a.md",
    "--input=beta=/abs/b==c.md",
  ]);
  expect(parsed.kind).toBe("run");
  if (parsed.kind !== "run") {
    throw new Error("expected run");
  }
  expect(parsed.inputBindings).toEqual([
    { id: "alpha", path: "/abs/a.md" },
    { id: "beta", path: "/abs/b==c.md" },
  ]);
});

test("run accepts zero inputs and an empty state", () => {
  const parsed = parseCommand("run", ["--pipeline-root=/p", "--config-root=/c", "--project=/pr"]);
  expect(parsed.kind).toBe("run");
});

test("run rejects missing required flags", () => {
  expectRunError(["--config-root=/c", "--project=/pr"], "--pipeline-root ABSOLUTE_PATH is required for run");
  expectRunError(["--pipeline-root=/p", "--project=/pr"], "--config-root ABSOLUTE_PATH is required for run");
  expectRunError(["--pipeline-root=/p", "--config-root=/c"], "--project ABSOLUTE_PATH is required for run");
});

test("run rejects duplicate singleton flags", () => {
  expectRunError([...RUN_BASE, "--config-root=/other"], "--config-root may be given at most once");
  expectRunError([...RUN_BASE, "--pipeline-root=/other"], "--pipeline-root may be given at most once");
  expectRunError([...RUN_BASE, "--project=/other"], "--project may be given at most once");
  expectRunError([...RUN_BASE, "--json", "--json"], "--json may be given at most once");
  expectRunError([...RUN_BASE, "--launcher-id=dhl_a", "--launcher-id=dhl_b"], "--launcher-id may be given at most once");
});

test("run rejects unsafe, empty and duplicated input ids", () => {
  expectRunError([...RUN_BASE, "--input", "bad/id=/abs/a"], "--input id must be a safe identifier");
  expectRunError([...RUN_BASE, "--input", "x!y=/abs/a"], "--input id must be a safe identifier");
  expectRunError([...RUN_BASE, "--input", "a".repeat(129) + "=/abs/a"], "--input id must be a safe identifier");
  expectRunError([...RUN_BASE, "--input", "has space=/abs/a"], "--input id must be a safe identifier");
  expectRunError([...RUN_BASE, "--input", "a/b=/abs/a"], "--input id must be a safe identifier");
  expectRunError([...RUN_BASE, "--input", "/abs/no-id"], "--input requires SAFE_ID=ABSOLUTE_PATH");
  expectRunError([...RUN_BASE, "--input", "in.json"], "--input requires SAFE_ID=ABSOLUTE_PATH");
  expectRunError([...RUN_BASE, "--input", "=/abs/a"], "--input requires SAFE_ID=ABSOLUTE_PATH");
  expectRunError([...RUN_BASE, "--input", "a=/abs/a", "--input", "a=/abs/b"], "--input a is bound more than once");
  expectRunError([...RUN_BASE, "--input", "a=relative/x"], "--input a must be bound to an absolute path");
  expectRunError([...RUN_BASE, "--input", "a="], "--input requires SAFE_ID=ABSOLUTE_PATH (the path part is empty)");
});

test("run rejects v1, container, output-path and state-root flags", () => {
  expectRunError([...RUN_BASE, "--workspace", "/w"], "run does not accept --workspace");
  expectRunError([...RUN_BASE, "--image", "img"], "run does not accept --image");
  expectRunError([...RUN_BASE, "--profile", "p"], "run does not accept --profile");
  expectRunError([...RUN_BASE, "--task", "t"], "run does not accept --task");
  expectRunError([...RUN_BASE, "--state-root", "/s"], "run does not accept --state-root");
  expectRunError([...RUN_BASE, "--daemon-state-root", "/s"], "run does not accept --daemon-state-root");
  expectRunError([...RUN_BASE, "--outputs-dir=/x"], "unknown argument: --outputs-dir=/x");
});

test("run rejects unknown flags and relative required paths", () => {
  expectRunError([...RUN_BASE, "--unknown"], "unknown argument: --unknown");
  expectRunError(["--pipeline-root", "relative", "--config-root=/c", "--project=/pr"], "--pipeline-root must be an absolute path");
  expectRunError(["--pipeline-root=/p", "--config-root", "relative", "--project=/pr"], "--config-root must be an absolute path");
  expectRunError(["--pipeline-root=/p", "--config-root=/c", "--project", "relative"], "--project must be an absolute path");
});

// --- resume parsing -----------------------------------------------------------

test("resume parses the exact grammar with both flag forms", () => {
  const parsed = parseCommand("resume", [
    "--run-id", "run-1.2_abc",
    "--config-root", "/abs/config",
    "--launcher-id", "dhl_l1",
    "--json",
  ]);
  if (parsed.kind !== "resume") {
    throw new Error("expected resume");
  }
  expect(parsed.runId).toBe("run-1.2_abc");
  expect(parsed.configRoot).toBe("/abs/config");
  expect(parsed.launcherId).toBe("dhl_l1");
  expect(parsed.json).toBe(true);

  const minimal = parseCommand("resume", ["--run-id=rid_x", "--config-root=/cfg"]);
  if (minimal.kind !== "resume") {
    throw new Error("expected resume");
  }
  expect(minimal.runId).toBe("rid_x");
  expect(minimal.configRoot).toBe("/cfg");
  expect(minimal.launcherId).toBeUndefined();
  expect(minimal.json).toBe(false);
});

test("resume rejects missing, duplicate, unsafe and value-less required flags", () => {
  expect(() => parseCommand("resume", [])).toThrow(/--run-id SAFE_ID is required/);
  expect(() => parseCommand("resume", ["--run-id", "rid"])).toThrow(/--config-root ABSOLUTE_PATH is required/);
  expect(() => parseCommand("resume", ["--config-root", "/c"])).toThrow(/--run-id SAFE_ID is required/);
  expect(() => parseCommand("resume", ["--run-id", "a", "--run-id", "b", "--config-root", "/c"])).toThrow(
    /--run-id may be given at most once/,
  );
  expect(() => parseCommand("resume", ["--run-id", "rid", "--config-root", "/c", "--config-root=/d"])).toThrow(
    /--config-root may be given at most once/,
  );
  expect(() => parseCommand("resume", ["--run-id", "a/b", "--config-root", "/c"])).toThrow(
    /--run-id must be a safe identifier/,
  );
  expect(() => parseCommand("resume", ["--run-id", "rid", "--config-root", "rel"])).toThrow(
    /--config-root must be an absolute path/,
  );
  expect(() => parseCommand("resume", ["--run-id"])).toThrow(/--run-id requires a value/);
  expect(() => parseCommand("resume", ["--run-id=", "--config-root", "/c"])).toThrow(/--run-id requires a value/);
  expect(() => parseCommand("resume", ["--run-id", "rid", "--config-root", "/c", "--launcher-id", "x"])).toThrow(
    /--launcher-id must be a launcher ID/,
  );
  expect(() => parseCommand("resume", ["--run-id", "rid", "--config-root", "/c", "--json", "--json"])).toThrow(
    /--json may be given at most once/,
  );
  expect(() => parseCommand("resume", ["--run-id", "rid", "--config-root", "/c", "--json=x"])).toThrow(
    /--json does not take a value/,
  );
});

test("resume rejects every fresh-run flag, unknown flags and positional arguments", () => {
  const base = ["--run-id", "rid", "--config-root", "/c"];
  expect(() => parseCommand("resume", [...base, "--pipeline-root", "/p"])).toThrow(
    /resume does not accept --pipeline-root; the pipeline comes only from the durable state/,
  );
  expect(() => parseCommand("resume", [...base, "--project", "/p"])).toThrow(
    /resume does not accept --project; the run-owned project copy already exists/,
  );
  expect(() => parseCommand("resume", [...base, "--input", "a=/p"])).toThrow(
    /resume does not accept --input; the run-owned input snapshot already exists/,
  );
  expect(() => parseCommand("resume", [...base, "--workspace", "/w"])).toThrow(
    /resume does not accept --workspace; the run-owned project copy is mounted from the run root/,
  );
  expect(() => parseCommand("resume", [...base, "--image", "x:1"])).toThrow(
    /resume does not accept --image; the worker image comes only from the selected profile/,
  );
  expect(() => parseCommand("resume", [...base, "--profile", "coder"])).toThrow(
    /resume does not accept --profile; the execution profiles are selected/,
  );
  expect(() => parseCommand("resume", [...base, "--task", "TASK.md"])).toThrow(/resume does not accept --task/);
  expect(() => parseCommand("resume", [...base, "--state-root", "/s"])).toThrow(
    /resume does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT/,
  );
  expect(() => parseCommand("resume", [...base, "--daemon-state-root", "/s"])).toThrow(
    /resume does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT/,
  );
  expect(() => parseCommand("resume", [...base, "--flag"])).toThrow(/unknown argument: --flag/);
  expect(() => parseCommand("resume", [...base, "positional"])).toThrow(/unknown argument: positional/);
});

test("usage documents the production pipeline v2 resume command", () => {
  const text = usage();
  expect(text).toContain("'orchestrator resume'");
  expect(text).toContain("--run-id SAFE_ID");
  expect(text).toContain("resume flags (production pipeline v2 continuation):");
  expect(text).toContain("resume continues only a clean active run");
  expect(text).toContain("the pipeline comes");
});

test("smoke and agent-smoke parsing is unchanged", () => {
  expect(parseCommand("smoke", []).kind).toBe("smoke");
  expect(parseCommand("agent-smoke", ["--config-root=/c"]).kind).toBe("agent-smoke");
  expect(() => parseCommand("smoke", ["--project=/p"])).toThrow("unknown argument: --project=/p");
  expect(() => parseCommand("agent-smoke", ["--config-root=/c", "--json"])).toThrow("unknown argument: --json");
});
