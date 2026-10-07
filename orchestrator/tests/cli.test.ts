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
  expect(text).toContain("handled by dedicated commands");
  expect(text).toContain("'continue_stage' intervention runs via 'orchestrator continue-stage'");
  expect(text).toContain("'revise_task' intervention via 'orchestrator revise-task'");
  expect(text).not.toContain("no dedicated CLI path");
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

// --- respond parsing -----------------------------------------------------------

test("respond parses the exact grammar with both flag forms", () => {
  const parsed = parseCommand("respond", [
    "--run-id", "run-1.2_abc",
    "--wait-index", "3",
    "--action", "continue_stage",
    "--json",
  ]);
  if (parsed.kind !== "respond") {
    throw new Error("expected respond");
  }
  expect(parsed.runId).toBe("run-1.2_abc");
  expect(parsed.waitIndex).toBe(3);
  expect(parsed.actionId).toBe("continue_stage");
  expect(parsed.json).toBe(true);

  const minimal = parseCommand("respond", ["--run-id=rid_x", "--wait-index=1", "--action=a.b_c"]);
  if (minimal.kind !== "respond") {
    throw new Error("expected respond");
  }
  expect(minimal.runId).toBe("rid_x");
  expect(minimal.waitIndex).toBe(1);
  expect(minimal.actionId).toBe("a.b_c");
  expect(minimal.json).toBe(false);
});

test("respond wait-index boundaries: 1 and MAX_SAFE_INTEGER accepted; every other form rejected", () => {
  const accepted = (value: string): number => {
    const parsed = parseCommand("respond", ["--run-id", "rid", "--wait-index", value, "--action", "a"]);
    if (parsed.kind !== "respond") {
      throw new Error("expected respond");
    }
    return parsed.waitIndex;
  };
  expect(accepted("1")).toBe(1);
  expect(accepted(String(Number.MAX_SAFE_INTEGER))).toBe(Number.MAX_SAFE_INTEGER);
  expect(accepted("9007199254740991")).toBe(9007199254740991);

  const rejected = (value: string): void => {
    expect(() => parseCommand("respond", ["--run-id", "rid", "--wait-index", value, "--action", "a"])).toThrow();
  };
  rejected("0");
  rejected("-1");
  rejected("+1");
  rejected("01");
  rejected("1.5");
  rejected("1e3");
  rejected(" 1");
  rejected("1 ");
  rejected("");
  rejected("abc");
  rejected("9007199254740992"); // 2^53: the first unsafe integer
  rejected("99999999999999999999"); // overflow
});

test("respond rejects missing, duplicate and unsafe required values", () => {
  expect(() => parseCommand("respond", [])).toThrow(/--run-id SAFE_ID is required/);
  expect(() => parseCommand("respond", ["--run-id", "rid"])).toThrow(/--wait-index POSITIVE_INTEGER is required/);
  expect(() => parseCommand("respond", ["--run-id", "rid", "--wait-index", "1"])).toThrow(
    /--action SAFE_ID is required/,
  );
  expect(() => parseCommand("respond", ["--wait-index", "1", "--action", "a"])).toThrow(
    /--run-id SAFE_ID is required/,
  );
  expect(() => parseCommand("respond", ["--run-id", "a", "--run-id", "b", "--wait-index", "1", "--action", "a"])).toThrow(
    /--run-id may be given at most once/,
  );
  expect(() => parseCommand("respond", ["--run-id", "rid", "--wait-index", "1", "--wait-index", "2", "--action", "a"])).toThrow(
    /--wait-index may be given at most once/,
  );
  expect(() => parseCommand("respond", ["--run-id", "rid", "--wait-index", "1", "--action", "a", "--action", "b"])).toThrow(
    /--action may be given at most once/,
  );
  expect(() => parseCommand("respond", ["--run-id", "bad/id", "--wait-index", "1", "--action", "a"])).toThrow(
    /--run-id must be a safe identifier/,
  );
  expect(() => parseCommand("respond", ["--run-id", "rid", "--wait-index", "1", "--action", "bad id"])).toThrow(
    /--action must be a safe identifier/,
  );
  expect(() => parseCommand("respond", ["--run-id", "rid", "--wait-index", "1", "--action", "a", "--json", "--json"])).toThrow(
    /--json may be given at most once/,
  );
  expect(() => parseCommand("respond", ["--run-id", "rid", "--wait-index", "1", "--action", "a", "--json=x"])).toThrow(
    /--json does not take a value/,
  );
});

test("respond rejects fresh-run and resume flags, unknown flags and positional arguments", () => {
  const base = ["--run-id", "rid", "--wait-index", "1", "--action", "a"];
  expect(() => parseCommand("respond", [...base, "--config-root", "/c"])).toThrow(
    /respond does not accept --config-root/,
  );
  expect(() => parseCommand("respond", [...base, "--launcher-id", "dhl_x"])).toThrow(
    /respond does not accept --launcher-id; the response command uses no Launcher credential/,
  );
  expect(() => parseCommand("respond", [...base, "--pipeline-root", "/p"])).toThrow(
    /respond does not accept --pipeline-root/,
  );
  expect(() => parseCommand("respond", [...base, "--project", "/p"])).toThrow(/respond does not accept --project/);
  expect(() => parseCommand("respond", [...base, "--input", "a=/p"])).toThrow(/respond does not accept --input/);
  expect(() => parseCommand("respond", [...base, "--workspace", "/w"])).toThrow(
    /respond does not accept --workspace; no worker is launched by respond/,
  );
  expect(() => parseCommand("respond", [...base, "--image", "x:1"])).toThrow(/respond does not accept --image/);
  expect(() => parseCommand("respond", [...base, "--profile", "coder"])).toThrow(
    /respond does not accept --profile/,
  );
  expect(() => parseCommand("respond", [...base, "--task", "TASK.md"])).toThrow(/respond does not accept --task/);
  expect(() => parseCommand("respond", [...base, "--state-root", "/s"])).toThrow(
    /respond does not accept --state-root/,
  );
  expect(() => parseCommand("respond", [...base, "--daemon-state-root", "/s"])).toThrow(
    /respond does not accept --daemon-state-root/,
  );
  expect(() => parseCommand("respond", [...base, "--target", "ship"])).toThrow(/unknown argument: --target/);
  expect(() => parseCommand("respond", [...base, "--request-digest", "a"])).toThrow(/unknown argument/);
  expect(() => parseCommand("respond", [...base, "--response-digest", "a"])).toThrow(/unknown argument/);
  expect(() => parseCommand("respond", [...base, "positional"])).toThrow(/unknown argument: positional/);
});

test("usage documents the production pipeline v2 respond command", () => {
  const text = usage();
  expect(text).toContain("'orchestrator respond'");
  expect(text).toContain("respond flags (production pipeline v2 wait response):");
  expect(text).toContain("--wait-index N");
  expect(text).toContain("the pipeline is NOT continued");
});

test("usage documents the production pipeline v2 resume command", () => {
  const text = usage();
  expect(text).toContain("'orchestrator resume'");
  expect(text).toContain("--run-id SAFE_ID");
  expect(text).toContain("resume flags (production pipeline v2 continuation):");
  expect(text).toContain("resume continues only a clean active run");
  expect(text).toContain("the pipeline comes");
});

// --- continue-stage parser (red-before) -------------------------------------

test("continue-stage parses the exact grammar with both flag forms", () => {
  const base = ["--run-id", "run-1", "--wait-index", "2", "--additional-iterations", "3", "--config-root", "/abs/config"];
  const parsed = parseCommand("continue-stage", [...base, "--launcher-id", "dhl_l1", "--json"]);
  expect(parsed).toEqual({
    kind: "continue-stage",
    runId: "run-1",
    waitIndex: 2,
    additionalIterations: 3,
    configRoot: "/abs/config",
    launcherId: "dhl_l1",
    json: true,
  });
  const inline = parseCommand("continue-stage", [
    "--run-id=run-2",
    "--wait-index=4",
    "--additional-iterations=1",
    "--config-root=/abs/config2",
  ]);
  expect(inline).toEqual({
    kind: "continue-stage",
    runId: "run-2",
    waitIndex: 4,
    additionalIterations: 1,
    configRoot: "/abs/config2",
    launcherId: undefined,
    json: false,
  });
});

test("continue-stage accepts MAX_SAFE_INTEGER for both numeric flags", () => {
  const parsed = parseCommand("continue-stage", [
    "--run-id", "run-1",
    "--wait-index", "9007199254740991",
    "--additional-iterations", "9007199254740991",
    "--config-root", "/abs/config",
  ]);
  expect(parsed.kind).toBe("continue-stage");
  if (parsed.kind !== "continue-stage") {
    throw new Error("expected continue-stage");
  }
  expect(parsed.waitIndex).toBe(9007199254740991);
  expect(parsed.additionalIterations).toBe(9007199254740991);
});

test("continue-stage rejects the full invalid-number matrix for both numeric flags", () => {
  const invalid = [
    "",
    "0",
    "-1",
    "+1",
    "01",
    "1.5",
    "1e2",
    " 1",
    "1 ",
    "1_000",
    "0x1",
    "abc",
    "١٢٣",
    "9007199254740993",
  ];
  for (const flag of ["--wait-index", "--additional-iterations"]) {
    for (const value of invalid) {
      let message = "";
      try {
        parseCommand("continue-stage", [
          "--run-id", "run-1",
          "--wait-index", flag === "--wait-index" ? value : "1",
          "--additional-iterations", flag === "--additional-iterations" ? value : "1",
          "--config-root", "/abs/config",
        ]);
      } catch (cause) {
        message = cause instanceof Error ? cause.message : String(cause);
      }
      expect(message).toContain(`${flag}`);
    }
    // overflow gets its own message
    let message = "";
    try {
      parseCommand("continue-stage", [
        "--run-id", "run-1",
        "--wait-index", flag === "--wait-index" ? "9007199254740993" : "1",
        "--additional-iterations", flag === "--additional-iterations" ? "9007199254740993" : "1",
        "--config-root", "/abs/config",
      ]);
    } catch (cause) {
      message = cause instanceof Error ? cause.message : String(cause);
    }
    expect(message).toBe(`${flag} exceeds the safe integer range`);
  }
});

test("continue-stage rejects missing, duplicate and value-less required flags", () => {
  for (const argv of [
    [],
    ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2"],
    ["--run-id", "run-1", "--additional-iterations", "2", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--config-root", "/abs/config"],
    ["--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--run-id", "run-2", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--wait-index", "2", "--additional-iterations", "2", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--additional-iterations", "3", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config", "--config-root", "/other"],
    ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config", "--launcher-id", "dhl_a", "--launcher-id", "dhl_b"],
    ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config", "--json", "--json"],
    ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config", "--json=true"],
  ]) {
    expect(() => parseCommand("continue-stage", argv)).toThrow();
  }
  expect(() =>
    parseCommand("continue-stage", ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--config-root"]),
  ).toThrow("--config-root requires a value");
  expect(() => parseCommand("continue-stage", ["--run-id", "run-1", "--wait-index"])).toThrow(
    "--wait-index requires a value",
  );
  expect(() => parseCommand("continue-stage", ["--additional-iterations"])).toThrow(
    "--additional-iterations requires a value",
  );
  expect(() => parseCommand("continue-stage", ["--run-id="])).toThrow("--run-id requires a value");
});

test("continue-stage rejects unsafe run ids, relative config roots and invalid launcher ids", () => {
  expect(() =>
    parseCommand("continue-stage", ["--run-id", "../escape", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config"]),
  ).toThrow("--run-id must be a safe identifier");
  expect(() =>
    parseCommand("continue-stage", ["--run-id", "", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config"]),
  ).toThrow();
  expect(() =>
    parseCommand("continue-stage", ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "relative/path"]),
  ).toThrow("--config-root must be an absolute path");
  expect(() =>
    parseCommand("continue-stage", ["--run-id", "run-1", "--wait-index", "1", "--additional-iterations", "2", "--config-root", "/abs/config", "--launcher-id", "admin_l1"]),
  ).toThrow("--launcher-id must be a launcher ID (dhl_...)");
});

test("continue-stage rejects respond, fresh-run, state-root and internal-policy flags", () => {
  for (const argv of [
    ["--action", "continue_stage"],
    ["--action=continue_stage"],
    ["--pipeline-root", "/abs/pipeline"],
    ["--project", "/abs/project"],
    ["--input", "spec=/abs/spec.md"],
    ["--workspace", "/w"],
    ["--image", "img:1"],
    ["--profile", "coder"],
    ["--task", "/abs/task.md"],
    ["--state-root", "/state/root"],
    ["--daemon-state-root", "/daemon/root"],
    ["--stage-id", "stage-1"],
    ["--plan-digest", "abc"],
    ["--initial-budget", "2"],
    ["--intent", "{}"],
    ["--compiled-plan", "{}"],
  ]) {
    expect(() =>
      parseCommand("continue-stage", [
        "--run-id", "run-1",
        "--wait-index", "1",
        "--additional-iterations", "2",
        "--config-root", "/abs/config",
        ...argv,
      ]),
    ).toThrow();
  }
  expect(() =>
    parseCommand("continue-stage", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--additional-iterations", "2",
      "--config-root", "/abs/config",
      "--action", "revise_task",
    ]),
  ).toThrow("continue-stage does not accept --action; the intervention action is fixed as continue_stage by the runner entrypoint");
  expect(() =>
    parseCommand("continue-stage", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--additional-iterations", "2",
      "--config-root", "/abs/config",
      "--pipeline-root", "/abs/pipeline",
    ]),
  ).toThrow("continue-stage does not accept --pipeline-root; the pipeline comes only from the durable state");
});

test("continue-stage rejects unknown flags and positional arguments", () => {
  expect(() =>
    parseCommand("continue-stage", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--additional-iterations", "2",
      "--config-root", "/abs/config",
      "--unknown",
    ]),
  ).toThrow("unknown argument: --unknown");
  expect(() =>
    parseCommand("continue-stage", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--additional-iterations", "2",
      "--config-root", "/abs/config",
      "positional",
    ]),
  ).toThrow("unknown argument: positional");
});

test("usage documents the production pipeline v2 continue-stage command", () => {
  const text = usage();
  expect(text).toContain("continue-stage flags (production pipeline v2 stage intervention)");
  expect(text).toContain("--additional-iterations");
  expect(text).toContain("no default");
  expect(text).toContain("fixed as 'continue_stage'");
  expect(text).toContain("(the command accepts no");
  expect(text).toContain("--action flag) and never accepts");
  expect(text).toContain("orchestrator continue-stage");
  expect(text).toContain("performs the continue_stage stage intervention");
});

// --- revise-task parser ------------------------------------------------------

test("revise-task parses the exact grammar with both flag forms", () => {
  const parsed = parseCommand("revise-task", [
    "--run-id", "run-1",
    "--wait-index", "2",
    "--task-id", "task-a",
    "--task-file", "/abs/body.md",
    "--config-root", "/abs/config",
    "--launcher-id", "dhl_l1",
    "--json",
  ]);
  expect(parsed).toEqual({
    kind: "revise-task",
    runId: "run-1",
    waitIndex: 2,
    taskId: "task-a",
    taskFile: "/abs/body.md",
    configRoot: "/abs/config",
    launcherId: "dhl_l1",
    json: true,
  });
  const inline = parseCommand("revise-task", [
    "--run-id=run-2",
    "--wait-index=4",
    "--task-id=task-b",
    "--task-file=/abs/body2.md",
    "--config-root=/abs/config2",
  ]);
  expect(inline).toEqual({
    kind: "revise-task",
    runId: "run-2",
    waitIndex: 4,
    taskId: "task-b",
    taskFile: "/abs/body2.md",
    configRoot: "/abs/config2",
    launcherId: undefined,
    json: false,
  });
  // '=' inside the task-file path is allowed by the shared value parser
  const equals = parseCommand("revise-task", [
    "--run-id", "run-3",
    "--wait-index", "1",
    "--task-id", "task-c",
    "--task-file", "/abs/weird=name.md",
    "--config-root", "/abs/config",
  ]);
  if (equals.kind !== "revise-task") {
    throw new Error("expected revise-task");
  }
  expect(equals.taskFile).toBe("/abs/weird=name.md");
});

test("revise-task accepts MAX_SAFE_INTEGER for --wait-index", () => {
  const parsed = parseCommand("revise-task", [
    "--run-id", "run-1",
    "--wait-index", "9007199254740991",
    "--task-id", "task-a",
    "--task-file", "/abs/body.md",
    "--config-root", "/abs/config",
  ]);
  if (parsed.kind !== "revise-task") {
    throw new Error("expected revise-task");
  }
  expect(parsed.waitIndex).toBe(9007199254740991);
});

test("revise-task rejects the full invalid-number matrix for --wait-index", () => {
  const invalid = [
    "",
    "0",
    "-1",
    "+1",
    "01",
    "1.5",
    "1e2",
    " 1",
    "1 ",
    "1_000",
    "0x1",
    "abc",
    "١٢٣",
    "9007199254740993",
  ];
  for (const value of invalid) {
    expect(() =>
      parseCommand("revise-task", [
        "--run-id", "run-1",
        "--wait-index", value,
        "--task-id", "task-a",
        "--task-file", "/abs/body.md",
        "--config-root", "/abs/config",
      ]),
    ).toThrow();
  }
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root"]),
  ).toThrow("--config-root requires a value");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index"]),
  ).toThrow("--wait-index requires a value");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1"]),
  ).toThrow("--wait-index POSITIVE_INTEGER is required for revise-task");
  expect(() =>
    parseCommand("revise-task", ["--task-id=task-a", "--wait-index=1", "--config-root=/c"]),
  ).toThrow("--run-id SAFE_ID is required for revise-task");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-file", "/abs/body.md", "--config-root", "/abs/config"]),
  ).toThrow("--task-id SAFE_ID is required for revise-task");
  expect(() =>
    parseCommand("revise-task", ["--task-file="]),
  ).toThrow("--task-file requires a value");
});

test("revise-task rejects missing, duplicate and value-less required flags", () => {
  for (const argv of [
    [],
    ["--run-id", "run-1", "--task-id", "task-a", "--task-file", "/abs/body.md"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-file", "/abs/body.md", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--config-root", "/abs/config"],
    ["--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--run-id", "run-2", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--wait-index", "2", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-id", "task-b", "--task-file", "/abs/body.md", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--task-file", "/abs/other.md", "--config-root", "/abs/config"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config", "--config-root", "/other"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config", "--launcher-id", "dhl_a", "--launcher-id", "dhl_b"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config", "--json", "--json"],
    ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config", "--json=true"],
  ]) {
    expect(() => parseCommand("revise-task", argv)).toThrow();
  }
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config", "--task-id"]),
  ).toThrow("--task-id requires a value");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--config-root", "/abs/config"]),
  ).toThrow("--task-file ABSOLUTE_PATH is required for revise-task");
});

test("revise-task rejects unsafe ids, relative or empty paths and invalid launcher ids", () => {
  expect(() =>
    parseCommand("revise-task", ["--run-id", "../escape", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config"]),
  ).toThrow("--run-id must be a safe identifier");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config"]),
  ).toThrow();
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "../escape", "--task-file", "/abs/body.md", "--config-root", "/abs/config"]),
  ).toThrow("--task-id must be a safe identifier");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "", "--task-file", "/abs/body.md", "--config-root", "/abs/config"]),
  ).toThrow();
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "relative/body.md", "--config-root", "/abs/config"]),
  ).toThrow("--task-file must be an absolute path");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "", "--config-root", "/abs/config"]),
  ).toThrow("--task-file requires a value");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "relative/config"]),
  ).toThrow("--config-root must be an absolute path");
  expect(() =>
    parseCommand("revise-task", ["--run-id", "run-1", "--wait-index", "1", "--task-id", "task-a", "--task-file", "/abs/body.md", "--config-root", "/abs/config", "--launcher-id", "admin_l1"]),
  ).toThrow("--launcher-id must be a launcher ID (dhl_...)");
});

test("revise-task rejects respond, fresh-run, continue-stage, state-root, body and internal-policy flags", () => {
  for (const argv of [
    ["--action", "revise_task"],
    ["--action=revise_task"],
    ["--task-body", "inline body"],
    ["--task-body=inline body"],
    ["--additional-iterations", "2"],
    ["--pipeline-root", "/abs/pipeline"],
    ["--project", "/abs/project"],
    ["--input", "spec=/abs/spec.md"],
    ["--workspace", "/w"],
    ["--image", "img:1"],
    ["--profile", "coder"],
    ["--task", "/abs/task.md"],
    ["--state-root", "/state/root"],
    ["--daemon-state-root", "/daemon/root"],
    ["--stage-id", "stage-1"],
    ["--plan-digest", "abc"],
    ["--initial-budget", "2"],
    ["--intent", "{}"],
    ["--candidate-task-revision", "{}"],
    ["--compiled-plan", "{}"],
    ["--generation-index", "1"],
    ["--iteration-index", "1"],
  ]) {
    expect(() =>
      parseCommand("revise-task", [
        "--run-id", "run-1",
        "--wait-index", "1",
        "--task-id", "task-a",
        "--task-file", "/abs/body.md",
        "--config-root", "/abs/config",
        ...argv,
      ]),
    ).toThrow();
  }
  expect(() =>
    parseCommand("revise-task", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--task-id", "task-a",
      "--task-file", "/abs/body.md",
      "--config-root", "/abs/config",
      "--action", "continue_stage",
    ]),
  ).toThrow("revise-task does not accept --action; the intervention action is fixed as revise_task by the runner entrypoint");
  expect(() =>
    parseCommand("revise-task", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--task-id", "task-a",
      "--task-file", "/abs/body.md",
      "--config-root", "/abs/config",
      "--task-body", "inline",
    ]),
  ).toThrow("revise-task does not accept --task-body; the revised task body is never passed inline or through argv - use --task-file");
  expect(() =>
    parseCommand("revise-task", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--task-id", "task-a",
      "--task-file", "/abs/body.md",
      "--config-root", "/abs/config",
      "--pipeline-root", "/abs/pipeline",
    ]),
  ).toThrow("revise-task does not accept --pipeline-root; the pipeline comes only from the durable state");
});

test("revise-task rejects unknown flags and positional arguments", () => {
  expect(() =>
    parseCommand("revise-task", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--task-id", "task-a",
      "--task-file", "/abs/body.md",
      "--config-root", "/abs/config",
      "--unknown",
    ]),
  ).toThrow("unknown argument: --unknown");
  expect(() =>
    parseCommand("revise-task", [
      "--run-id", "run-1",
      "--wait-index", "1",
      "--task-id", "task-a",
      "--task-file", "/abs/body.md",
      "--config-root", "/abs/config",
      "positional",
    ]),
  ).toThrow("unknown argument: positional");
});

test("usage documents the production pipeline v2 revise-task command and the file-only task body", () => {
  const text = usage();
  expect(text).toContain("revise-task  production pipeline v2 task revision intervention");
  expect(text).toContain("revise-task flags (production pipeline v2 task revision intervention)");
  expect(text).toContain("--task-id SAFE_ID");
  expect(text).toContain("--task-file PATH");
  expect(text).toContain("The body is never passed");
  expect(text).toContain("inline or through argv (there is no --task-body flag)");
  expect(text).toContain("an empty file is a CLI contract error");
  expect(text).toContain("without trimming, normalizing line endings or adding a");
  expect(text).toContain("trailing newline");
  expect(text).toContain("fixed as 'revise_task'");
  expect(text).toContain("orchestrator revise-task");
  expect(text).toContain("performs the revise_task task revision intervention");
  // the stale claim is removed; the generic respond stays fail-closed
  expect(text).not.toContain("no dedicated CLI path");
  expect(text).toContain("the 'revise_task' intervention via 'orchestrator revise-task'");
  expect(text).toContain("generic respond fail-closes on them");
});

test("smoke and agent-smoke parsing is unchanged", () => {
  expect(parseCommand("smoke", []).kind).toBe("smoke");
  expect(parseCommand("agent-smoke", ["--config-root=/c"]).kind).toBe("agent-smoke");
  expect(() => parseCommand("smoke", ["--project=/p"])).toThrow("unknown argument: --project=/p");
  expect(() => parseCommand("agent-smoke", ["--config-root=/c", "--json"])).toThrow("unknown argument: --json");
});

// --- resume-plan (production pipeline v2 planning-run-plan continuation) -----

test("resume-plan parses the exact grammar with both flag forms", () => {
  const minimal = parseCommand("resume-plan", [
    "--run-id", "planning-run",
    "--stage-id", "stage-2",
    "--initial-budget", "2",
    "--config-root", "/cfg",
  ]);
  expect(minimal).toEqual({
    kind: "resume-plan",
    runId: "planning-run",
    stageId: "stage-2",
    initialBudget: 2,
    configRoot: "/cfg",
    launcherId: undefined,
    json: false,
  });
  const full = parseCommand("resume-plan", [
    "--run-id=planning-run",
    "--stage-id=stage-2",
    "--initial-budget=2",
    "--config-root=/cfg",
    "--launcher-id=dhl_x",
    "--json",
  ]);
  expect(full).toEqual({
    kind: "resume-plan",
    runId: "planning-run",
    stageId: "stage-2",
    initialBudget: 2,
    configRoot: "/cfg",
    launcherId: "dhl_x",
    json: true,
  });
  // both forms may be mixed per flag
  const mixed = parseCommand("resume-plan", [
    "--run-id", "planning-run",
    "--stage-id=stage-2",
    "--initial-budget", "2",
    "--config-root=/cfg",
  ]);
  expect(mixed).toMatchObject({ runId: "planning-run", stageId: "stage-2", initialBudget: 2, configRoot: "/cfg" });
});

test("resume-plan accepts MAX_SAFE_INTEGER for --initial-budget", () => {
  const parsed = parseCommand("resume-plan", [
    "--run-id", "r",
    "--stage-id", "s",
    "--initial-budget", String(Number.MAX_SAFE_INTEGER),
    "--config-root", "/cfg",
  ]) as Extract<Awaited<ReturnType<typeof parseCommand>>, { kind: "resume-plan" }>;
  expect(parsed.initialBudget).toBe(Number.MAX_SAFE_INTEGER);
});

test("resume-plan rejects the full invalid-number matrix for --initial-budget", () => {
  for (const value of ["0", "-2", "+2", "02", "1.5", "1e2", " 2", "2 ", String(Number.MAX_SAFE_INTEGER + 1)]) {
    expect(() =>
      parseCommand("resume-plan", [
        "--run-id", "r",
        "--stage-id", "s",
        "--initial-budget", value,
        "--config-root", "/cfg",
      ]),
    ).toThrow(/--initial-budget/);
  }
});

test("resume-plan rejects missing, duplicate and value-less required flags", () => {
  const required = ["--run-id", "--stage-id", "--initial-budget", "--config-root"];
  // every missing required flag
  for (const missing of required) {
    const argv = [
      ["--run-id", "r"],
      ["--stage-id", "s"],
      ["--initial-budget", "2"],
      ["--config-root", "/cfg"],
    ]
      .filter((pair) => pair[0] !== missing)
      .flat();
    expect(() => parseCommand("resume-plan", argv)).toThrow(new RegExp(missing.replace(/-/g, "\\-")));
  }
  // duplicates reject instead of silently last-wins
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "a", "--run-id", "b", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/cfg"]),
  ).toThrow("--run-id may be given at most once");
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "a", "--stage-id", "b", "--initial-budget", "2", "--config-root", "/cfg"]),
  ).toThrow("--stage-id may be given at most once");
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--initial-budget", "3", "--config-root", "/cfg"]),
  ).toThrow("--initial-budget may be given at most once");
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/a", "--config-root", "/b"]),
  ).toThrow("--config-root may be given at most once");
  // a value-less required flag at the end
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root"]),
  ).toThrow("--config-root requires a value");
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget"]),
  ).toThrow("--initial-budget requires a value");
});

test("resume-plan rejects unsafe run/stage ids, relative or empty config roots and invalid launcher ids", () => {
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "../escape", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/cfg"]),
  ).toThrow(/--run-id must be a safe identifier/);
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "../escape", "--initial-budget", "2", "--config-root", "/cfg"]),
  ).toThrow(/--stage-id must be a safe identifier/);
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "..", "--initial-budget", "2", "--config-root", "/cfg"]),
  ).toThrow(/--stage-id must be a safe identifier/);
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", "relative/config"]),
  ).toThrow("--config-root must be an absolute path");
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", ""]),
  ).toThrow();
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/cfg", "--launcher-id", "not-dhl"]),
  ).toThrow("--launcher-id must be a launcher ID (dhl_...)");
});

test("resume-plan rejects --json with a value and duplicate --json", () => {
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/cfg", "--json=true"]),
  ).toThrow("--json does not take a value");
  expect(() =>
    parseCommand("resume-plan", ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/cfg", "--json", "--json"]),
  ).toThrow("--json may be given at most once");
});

test("resume-plan rejects respond, fresh-run, state-root and internal-policy flags", () => {
  const base = ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/cfg"];
  for (const [flag, value] of [
    ["--action", "continue_stage"],
    ["--wait-index", "1"],
    ["--additional-iterations", "2"],
    ["--task-id", "task-a"],
    ["--task-file", "/tmp/task.md"],
    ["--task-body", "BODY"],
    ["--pipeline-root", "/pipelines"],
    ["--project", "/project"],
    ["--input", "task=/inputs/task.md"],
    ["--workspace", "/workspace"],
    ["--image", "ghcr.io/example/worker:1"],
    ["--profile", "coder"],
    ["--task", "x"],
    ["--state-root", "/state"],
    ["--daemon-state-root", "/daemon"],
    ["--plan-digest", "a".repeat(64)],
    ["--intent", "never"],
    ["--generation-index", "1"],
    ["--iteration-index", "1"],
    ["--transition-index", "0"],
    ["--compiled-stage", "stage-2"],
  ] as ReadonlyArray<readonly [string, string]>) {
    expect(() =>
      parseCommand("resume-plan", [...base, flag, value]),
      `${flag} must be rejected`,
    ).toThrow();
    expect(() =>
      parseCommand("resume-plan", [...base, `${flag}=${value}`]),
      `${flag}= form must be rejected`,
    ).toThrow();
  }
});

test("resume-plan rejects unknown flags and positional arguments", () => {
  const base = ["--run-id", "r", "--stage-id", "s", "--initial-budget", "2", "--config-root", "/cfg"];
  expect(() => parseCommand("resume-plan", [...base, "--stage"])).toThrow("unknown argument: --stage");
  expect(() => parseCommand("resume-plan", [...base, "--stage-idx=1"])).toThrow("unknown argument: --stage-idx=1");
  expect(() => parseCommand("resume-plan", [...base, "stray"])).toThrow("unknown argument: stray");
});

test("usage documents the production pipeline v2 resume-plan command", () => {
  const text = usage();
  expect(text).toContain("resume-plan  production pipeline v2 planning continuation: accept the settled");
  expect(text).toContain("resume-plan flags (production pipeline v2 planning continuation)");
  expect(text).toContain("--stage-id SAFE_ID");
  expect(text).toContain("--initial-budget N");
  expect(text).toContain("operator policy of this command");
  expect(text).toContain("automatic planning loop)");
  expect(text).toContain("serves the initial plan-ready boundary of a fresh run and");
  expect(text).toContain("It serves both planning boundaries: the initial plan-ready");
  expect(text).toContain("boundary of a fresh run (plan revision -> first generation and iteration");
  expect(text).toContain("after a revise cycle (task revisions -> plan revision -> old generation");
  expect(text).toContain("planning loop and no automatic stage/budget selection");
  expect(text).toContain("a respond action and accepts no --action flag");
  expect(text).toContain("resume-plan' accepts the settled planning output");
  expect(text).toContain("the stage id and the initial budget are explicit operator");
});
