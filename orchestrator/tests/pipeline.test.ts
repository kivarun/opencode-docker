import { statSync } from "node:fs";
import { mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  PIPELINE_SCHEMA_VERSION,
  PipelineError,
  loadPipeline,
  parsePipelineSpec,
} from "../src/pipeline.ts";

const DEFAULT_BUNDLE = join(import.meta.dir, "..", "..", "pipelines", "default");

const hasDefaultBundle = (() => {
  try {
    return statSync(DEFAULT_BUNDLE).isDirectory();
  } catch {
    return false;
  }
})();

const INPUTS_YAML = `inputs:
  - id: task
    path: TASK.md
    protected: true
`;

const AGENT_STATE_YAML = `  - id: execute
    type: agent
    profile: default
    prompt: prompts/execute.md
    inputs:
      - task
    result_schema: schemas/agent-result.schema.json
    timeout_seconds: 3600
    max_attempts: 1
    transitions:
      - outcome: completed
        to: completed
`;

const TERMINAL_STATE_YAML = `  - id: completed
    type: terminal
    result: success
`;

function pipelineYaml(
  states: string = `${AGENT_STATE_YAML}${TERMINAL_STATE_YAML}`,
  inputs: string = INPUTS_YAML,
  entry: string = "execute",
  maxTransitions: number | string = 1,
): string {  return [
    "schema_version: 1",
    `entry_state: ${entry}`,
    `max_transitions: ${maxTransitions}`,
    "",
    inputs.trimEnd(),
    "",
    "states:",
    states.trimEnd(),
    "",
  ].join("\n");
}

function parseOk(yaml: string): void {
  const spec = parsePipelineSpec(yaml);
  expect(spec.schema_version).toBe(PIPELINE_SCHEMA_VERSION);
}

test("pipeline schema version constant is 1", () => {
  expect(PIPELINE_SCHEMA_VERSION).toBe(1);
});

test.skipIf(!hasDefaultBundle)(
  "1. loads the real default bundle with resolved prompt and schema content",
  async () => {
  const resolved = await loadPipeline(DEFAULT_BUNDLE);

  expect(resolved.bundleRoot).toBe(await realpath(DEFAULT_BUNDLE));
  expect(resolved.entry_state).toBe("execute");
  expect(resolved.max_transitions).toBe(1);
  expect(resolved.inputs).toEqual([{ id: "task", path: "TASK.md", protected: true }]);

  expect(resolved.states.length).toBe(2);
  const execute = resolved.states[0];
  expect(execute?.type).toBe("agent");
  if (execute?.type !== "agent") {
    throw new Error("expected agent state");
  }
  expect(execute.id).toBe("execute");
  expect(execute.profile).toBe("default");
  expect(execute.inputs).toEqual(["task"]);
  expect(execute.timeout_seconds).toBe(3600);
  expect(execute.max_attempts).toBe(1);
  expect(execute.transitions).toEqual([{ outcome: "completed", to: "completed" }]);
  expect(execute.promptPath.endsWith("prompts/execute.md")).toBe(true);
  expect(execute.promptContent).toContain("implementation agent");
  expect(execute.resultSchemaPath.endsWith("schemas/agent-result.schema.json")).toBe(true);
  expect(execute.resultSchema.type).toBe("object");

  const terminal = resolved.states[1];
  expect(terminal).toEqual({ id: "completed", type: "terminal", result: "success" });
  },
);

test("2. malformed YAML is rejected", () => {
  expect(() => parsePipelineSpec("schema_version: [unclosed")).toThrow(/not valid YAML/);
  expect(() => parsePipelineSpec("[]")).toThrow(/not a YAML mapping/);
});

test("3. unknown and missing fields are rejected at every level", () => {
  expect(() =>
    parsePipelineSpec(pipelineYaml().replace("max_transitions: 1", "max_transitions: 1\nextra: x")),
  ).toThrow(/pipeline.*unknown field "extra"/);
  expect(() =>
    parsePipelineSpec(pipelineYaml().replace("entry_state: execute\n", "")),
  ).toThrow(/missing required field "entry_state"/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml().replace("    protected: true", "    protected: true\n    extra: 1"),
    ),
  ).toThrow(/pipeline input 0.*unknown field "extra"/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml().replace("    timeout_seconds: 3600\n", ""),
    ),
  ).toThrow(/agent state "execute".*missing required field "timeout_seconds"/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml().replace("    result: success", "    result: success\n    profile: p"),
    ),
  ).toThrow(/terminal state "completed".*unknown field "profile"/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml().replace("      - outcome: completed\n        to: completed", "      - outcome: completed\n        to: completed\n        extra: 1"),
    ),
  ).toThrow(/transition 0 of state "execute".*unknown field "extra"/);
});

test("4. unsupported schema versions are rejected", () => {
  expect(() => parsePipelineSpec(pipelineYaml().replace("schema_version: 1", "schema_version: 2"))).toThrow(
    /schema_version 2, expected 1/,
  );
  expect(() => parsePipelineSpec(pipelineYaml().replace("schema_version: 1", "schema_version: 0"))).toThrow(
    /schema_version 0, expected 1/,
  );
  expect(() => parsePipelineSpec(pipelineYaml().replace("schema_version: 1", 'schema_version: "1"'))).toThrow(
    /schema_version "1", expected 1/,
  );
});

test("5. duplicate input, state and outcome identifiers are rejected", () => {
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(
        `${AGENT_STATE_YAML}${TERMINAL_STATE_YAML}`,
        `${INPUTS_YAML}  - id: task\n    path: other.md\n    protected: false\n`,
      ),
    ),
  ).toThrow(/input "task" more than once/);
  expect(() =>
    parsePipelineSpec(pipelineYaml(`${AGENT_STATE_YAML}${AGENT_STATE_YAML}${TERMINAL_STATE_YAML}`)),
  ).toThrow(/state "execute" more than once/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(
        `${AGENT_STATE_YAML.replace(
          "      - outcome: completed\n        to: completed",
          "      - outcome: completed\n        to: completed\n      - outcome: completed\n        to: completed",
        )}${TERMINAL_STATE_YAML}`,
      ),
    ),
  ).toThrow(/outcome "completed" more than once/);
  // distinct outcomes may target the same state
  parseOk(
    pipelineYaml(
      `${AGENT_STATE_YAML.replace(
        "      - outcome: completed\n        to: completed",
        "      - outcome: completed\n        to: completed\n      - outcome: failed\n        to: completed",
      )}${TERMINAL_STATE_YAML}`,
    ),
  );
});

test("6. unknown entry_state is rejected", () => {
  expect(() => parsePipelineSpec(pipelineYaml(`${AGENT_STATE_YAML}${TERMINAL_STATE_YAML}`, INPUTS_YAML, "missing"))).toThrow(
    /entry_state "missing" does not name a declared state/,
  );
});

test("7. unknown input references and unknown transition targets are rejected", () => {
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(`${AGENT_STATE_YAML.replace("      - task\n", "      - missing\n")}${TERMINAL_STATE_YAML}`),
    ),
  ).toThrow(/references undeclared input "missing"/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(`${AGENT_STATE_YAML.replace("        to: completed", "        to: nowhere")}${TERMINAL_STATE_YAML}`),
    ),
  ).toThrow(/targets unknown state "nowhere"/);
});

test("8. unreachable state is rejected", () => {
  const unreachableTerminal = `${TERMINAL_STATE_YAML}  - id: orphan\n    type: terminal\n    result: failed\n`;
  expect(() => parsePipelineSpec(pipelineYaml(`${AGENT_STATE_YAML}${unreachableTerminal}`))).toThrow(
    /state "orphan" is not reachable from entry_state/,
  );
});

test("9. agent without a path to a terminal state is rejected", () => {
  const doomedFragment = (id: string, to: string) => `  - id: ${id}
    type: agent
    profile: default
    prompt: prompts/execute.md
    inputs:
      - task
    result_schema: schemas/agent-result.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: loop
        to: ${to}
`;
  const states =
    AGENT_STATE_YAML.replace(
      "      - outcome: completed\n        to: completed",
      "      - outcome: completed\n        to: completed\n      - outcome: started\n        to: helper",
    ) +
    doomedFragment("helper", "helper2") +
    doomedFragment("helper2", "helper") +
    TERMINAL_STATE_YAML;
  expect(() => parsePipelineSpec(pipelineYaml(states))).toThrow(
    /agent state "helper" has no path to a terminal state/,
  );
});

test("10. a pipeline without any terminal state is rejected", () => {
  expect(() => parsePipelineSpec(pipelineYaml(AGENT_STATE_YAML))).toThrow(
    /must declare at least one terminal state/,
  );
});

test("11. zero, negative, fractional and unsafe bounds are rejected", () => {
  for (const bad of [0, -1, -3600, 1.5, Number.MAX_SAFE_INTEGER + 1, "3600", true]) {
    expect(() =>
      parsePipelineSpec(pipelineYaml().replace("timeout_seconds: 3600", `timeout_seconds: ${JSON.stringify(bad)}`)),
    ).toThrow(/timeout_seconds must be a positive safe integer/);
    expect(() =>
      parsePipelineSpec(pipelineYaml().replace("max_attempts: 1", `max_attempts: ${JSON.stringify(bad)}`)),
    ).toThrow(/max_attempts must be a positive safe integer/);
    expect(() =>
      parsePipelineSpec(
        pipelineYaml(`${AGENT_STATE_YAML}${TERMINAL_STATE_YAML}`, INPUTS_YAML, "execute", JSON.stringify(bad)),
      ),
    ).toThrow(/max_transitions must be a positive safe integer/);
  }
});

test("12. agent and terminal conditional fields are mutually rejected", () => {
  expect(() =>
    parsePipelineSpec(pipelineYaml(TERMINAL_STATE_YAML.replace("    result: success", "    result: success\n    prompt: prompts/execute.md"))),
  ).toThrow(/terminal state "completed".*unknown field "prompt"/);
  expect(() =>
    parsePipelineSpec(pipelineYaml(AGENT_STATE_YAML.replace("    profile: default", "    profile: default\n    result: success"))),
  ).toThrow(/agent state "execute".*unknown field "result"/);
});

test("13. unsupported state types are rejected", () => {
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(`  - id: shell_step\n    type: shell\n    command: echo hi\n${TERMINAL_STATE_YAML}`),
    ),
  ).toThrow(/unsupported type "shell"/);
});

test("14. states, inputs and transitions given as mappings are rejected", () => {
  const noStates = [
    "schema_version: 1",
    "entry_state: execute",
    "max_transitions: 1",
    "",
    INPUTS_YAML.trimEnd(),
    "",
    "states: {}",
    "",
  ].join("\n");
  expect(() => parsePipelineSpec(noStates)).toThrow(/pipeline states must be a list/);
  const noInputs = pipelineYaml().replace("inputs:\n", "inputs: {}\n").replace("  - id: task\n    path: TASK.md\n    protected: true\n", "");
  expect(() => parsePipelineSpec(noInputs)).toThrow(/pipeline inputs must be a list/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(
        AGENT_STATE_YAML.replace(
          "    transitions:\n      - outcome: completed\n        to: completed\n",
          "    transitions: {}\n",
        ) + TERMINAL_STATE_YAML,
      ),
    ),
  ).toThrow(/transitions must be a list/);
});

test("15. workspace input paths must be clean and relative", () => {
  for (const [bad, pattern] of [
    ["/etc/passwd", /workspace-relative path/],
    ["~/TASK.md", /workspace-relative path/],
    ["a/./b", /clean workspace-relative path/],
    ["a/../b", /clean workspace-relative path/],
    ["a//b", /clean workspace-relative path/],
    ["/", /workspace-relative path/],
    ["~", /workspace-relative path/],
  ] as Array<[string, RegExp]>) {
    expect(() =>
      parsePipelineSpec(
        pipelineYaml().replace("    path: TASK.md", `    path: ${JSON.stringify(bad)}`),
      ),
    ).toThrow(pattern);
  }
});

test("16. agent and terminal state ids must be safe", () => {
  expect(() =>
    parsePipelineSpec(pipelineYaml(AGENT_STATE_YAML.replace("  - id: execute", '  - id: "../escape"'))),
  ).toThrow(/not a safe identifier/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(
        AGENT_STATE_YAML.replace("        to: completed", "        to: ../escape"),
      ),
    ),
  ).toThrow(/not a safe identifier/);
});

test("17. pipeline capability fields are rejected as unknown", () => {
  expect(() =>
    parsePipelineSpec(pipelineYaml().replace("entry_state: execute", "entry_state: execute\nimage: ghcr.io/example/agent:latest")),
  ).toThrow(/unknown field "image"/);
  expect(() =>
    parsePipelineSpec(pipelineYaml().replace("entry_state: execute", "entry_state: execute\nenv:\n  LLM_KEY:\n    from_env: LLM_KEY\n    required: true")),
  ).toThrow(/unknown field "env"/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(AGENT_STATE_YAML.replace("    profile: default", "    profile: default\n    mounts:\n      - .:/workspace")),
    ),
  ).toThrow(/unknown field "mounts"/);
  expect(() =>
    parsePipelineSpec(
      pipelineYaml(AGENT_STATE_YAML.replace("    profile: default", "    profile: default\n    command:\n      - sh\n      - -c")),
    ),
  ).toThrow(/unknown field "command"/);
});

test("18. loadPipeline requires an absolute existing bundle root", async () => {
  await expect(loadPipeline("relative/bundle")).rejects.toThrow(/must be an absolute path/);
  await expect(loadPipeline(join(tmpdir(), "pipeline-missing-bundle"))).rejects.toThrow(
    /cannot be canonicalized/,
  );
});

test("19. only pipeline.yaml is supported, not pipeline.yml or JSON", async () => {
  await withBundle(
    { "pipeline.yml": pipelineYaml(), "pipeline.json": "{}", "prompts/execute.md": "x", "schemas/agent-result.schema.json": "{}" },
    async (bundle) => {
      await expect(loadPipeline(bundle)).rejects.toThrow(/no readable pipeline\.yaml/);
    },
  );
});

test("20. bundle prompt and result_schema files must exist as regular files inside the bundle", async () => {
  await withBundle(bundleFiles(pipelineYaml().replace("prompts/execute.md", "prompts/missing.md")), async (bundle) => {
    await expect(loadPipeline(bundle)).rejects.toThrow(/prompt .* is not accessible/);
  });
  await withBundle(bundleFiles(pipelineYaml().replace("prompts/execute.md", "prompts")), async (bundle) => {
    await expect(loadPipeline(bundle)).rejects.toThrow(/prompt .* is not a regular file/);
  });
  await withBundle(
    bundleFiles(pipelineYaml().replace("schemas/agent-result.schema.json", "schemas/missing.json")),
    async (bundle) => {
      await expect(loadPipeline(bundle)).rejects.toThrow(/result_schema .* is not accessible/);
    },
  );
  await withBundle(bundleFiles(pipelineYaml().replace("prompts/execute.md", "/etc/hostname")), async (bundle) => {
    await expect(loadPipeline(bundle)).rejects.toThrow(/must be a bundle-relative path/);
  });
  await withBundle(bundleFiles(pipelineYaml().replace("prompts/execute.md", "prompts/../execute.md")), async (bundle) => {
    await expect(loadPipeline(bundle)).rejects.toThrow(/must be a clean bundle-relative path/);
  });
});

test("21. prompt symlink escape is rejected, in-bundle symlink is accepted", async () => {
  const outside = await mkdtemp(join(tmpdir(), "pipeline-outside-"));
  try {
    await writeFile(join(outside, "secret.md"), "outside content\n");
    await withBundle(bundleFiles(pipelineYaml().replace("prompts/execute.md", "prompts/escape.md")), async (bundle) => {
      await symlink(join(outside, "secret.md"), join(bundle, "prompts", "escape.md"));
      await expect(loadPipeline(bundle)).rejects.toThrow(/resolves outside the pipeline bundle/);
    });

    await withBundle(bundleFiles(pipelineYaml().replace("prompts/execute.md", "prompts/alias.md")), async (bundle) => {
      await symlink(join(bundle, "prompts", "execute.md"), join(bundle, "prompts", "alias.md"));
      const resolved = await loadPipeline(bundle);
      const agent = resolved.states[0];
      if (agent?.type !== "agent") {
        throw new Error("expected agent state");
      }
      expect(agent.promptContent).toContain("Execute the task");
      expect(agent.promptPath.startsWith(await realpath(bundle))).toBe(true);
    });
  } finally {
    await rm(outside, { recursive: true, force: true });
  }
});

test("22. empty prompt and non-object result schema are rejected", async () => {
  await withBundle(
    { ...bundleFiles(pipelineYaml()), "prompts/execute.md": "   \n" },
    async (bundle) => {
      await expect(loadPipeline(bundle)).rejects.toThrow(/prompt .* is empty/);
    },
  );
  await withBundle(
    { ...bundleFiles(pipelineYaml()), "schemas/agent-result.schema.json": "{ not json" },
    async (bundle) => {
      await expect(loadPipeline(bundle)).rejects.toThrow(/result schema .* is not valid JSON/);
    },
  );
  await withBundle(
    { ...bundleFiles(pipelineYaml()), "schemas/agent-result.schema.json": "[]" },
    async (bundle) => {
      await expect(loadPipeline(bundle)).rejects.toThrow(/result schema .* is not a JSON object/);
    },
  );
});

async function withBundle(
  files: Record<string, string>,
  fn: (bundleRoot: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-bundle-"));
  const bundle = join(root, "bundle");
  await mkdir(bundle, { recursive: true });
  for (const [name, content] of Object.entries(files)) {
    const path = join(bundle, name);
    await mkdir(join(path, ".."), { recursive: true });
    await writeFile(path, content);
  }
  try {
    await fn(bundle);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function bundleFiles(pipelineText: string): Record<string, string> {
  return {
    "pipeline.yaml": pipelineText,
    "prompts/execute.md": "Execute the task as instructed.\n",
    "schemas/agent-result.schema.json": JSON.stringify({ type: "object" }),
  };
}
