import { cp, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";
import { PipelineError } from "../src/pipeline.ts";
import {
  pipelineV2ExecutionDigest,
  pipelineV2ExecutionSnapshot,
  pipelineV2ExecutionSnapshotJson,
  pipelineV2RunPipelineIdentity,
} from "../src/pipeline_v2_digest.ts";
import {
  reducePipelineV2RunCommand,
  validatePipelineV2RunState,
} from "../src/pipeline_v2_state.ts";

const CONFIG_SCHEMA = {
  type: "object",
  required: ["stage"],
  properties: { stage: { type: "string" } },
};

const FACTS_SCHEMA = {
  type: "object",
  required: ["revision"],
  properties: { revision: { type: "integer" } },
};

const DIGEST_PIPELINE_YAML = `schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true
  - id: config
    type: json
    protected: true
    schema: schemas/config.schema.json

outputs:
  - id: final_report
    required: true
    source:
      state_output:
        state: architect
        output: report
  - id: facts_digest
    required: false
    source:
      state_output:
        state: architect
        output: facts

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: task
        source:
          pipeline_input: task
      - id: cfg
        source:
          pipeline_input: config
    outputs:
      - id: implementation
        type: file
    timeout_seconds: 1800
    max_attempts: 1
    transitions:
      - outcome: completed
        to: gate

  - id: gate
    type: decision
    model: decisions/gate.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: config
    transitions:
      - outcome: d1
        to: architect
      - outcome: uncovered
        to: rejected
      - outcome: inconsistent_facts
        to: rejected
      - outcome: invalid_facts
        to: rejected

  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs:
      - id: task
        source:
          pipeline_input: task
      - id: implementation
        source:
          state_output:
            state: coder
            output: implementation
      - id: facts
        source:
          state_output:
            state: architect
            output: facts
    outputs:
      - id: facts
        type: json
        schema: schemas/facts.schema.json
      - id: report
        type: file
    timeout_seconds: 1800
    max_attempts: 2
    transitions:
      - outcome: completed
        to: done
      - outcome: needs_changes
        to: coder

  - id: done
    type: terminal
    result: success
  - id: rejected
    type: terminal
    result: failed
`;

// Same logical pipeline as DIGEST_PIPELINE_YAML, with reordered mapping
// keys, deeper indentation and blank-line noise; the compiled semantics
// are identical, so the digest must not move.
const DIGEST_PIPELINE_YAML_REFORMATTED = `

schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
    - protected: true
      type: file
      id: task
    - type: json
      protected: true
      schema: schemas/config.schema.json
      id: config

outputs:
    - id: final_report
      required: true
      source:
          state_output:
              state: architect
              output: report
    - id: facts_digest
      required: false
      source:
          state_output:
              output: facts
              state: architect

states:
    - type: agent
      profile: coder
      prompt: prompts/coder.md
      id: coder
      inputs:
          - id: task
            source:
                pipeline_input: task
          - source:
                pipeline_input: config
            id: cfg
      outputs:
          - id: implementation
            type: file
      max_attempts: 1
      timeout_seconds: 1800
      transitions:
          - to: gate
            outcome: completed

    - id: gate
      type: decision
      model: decisions/gate.yaml
      transitions:
          - to: architect
            outcome: d1
          - to: rejected
            outcome: uncovered
          - to: rejected
            outcome: inconsistent_facts
          - to: rejected
            outcome: invalid_facts
      inputs:
          - source:
                pipeline_input: config
            id: facts

    - id: architect
      type: agent
      profile: architect
      prompt: prompts/architect.md
      inputs:
          - id: task
            source:
                pipeline_input: task
          - source:
                state_output:
                    output: implementation
                    state: coder
            id: implementation
          - id: facts
            source:
                state_output:
                    state: architect
                    output: facts
      outputs:
          - type: json
            id: facts
            schema: schemas/facts.schema.json
          - type: file
            id: report
      timeout_seconds: 1800
      max_attempts: 2
      transitions:
          - to: done
            outcome: completed
          - to: coder
            outcome: needs_changes

    - result: success
      type: terminal
      id: done
    - type: terminal
      id: rejected
      result: failed

`;


const GATE_MODEL_YAML = `schema_version: 1
facts:
  - id: f1
decisions:
  - id: d1
relations: []
constraints: []
rules:
  - id: r1
    when:
      fact: f1
      equals: true
    decision: d1
`;

const GATE_MODEL_YAML_REFORMATTED = `rules:
    - decision: d1
      when:
          equals: true
          fact: f1
      id: r1
constraints: []
relations: []
decisions:
    - id: d1
facts:
    - id: f1
schema_version: 1
`;

async function withTemp(fn: (root: string) => Promise<void>): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-digest-"));
  try {
    await fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function withTempResult<T>(fn: (root: string) => Promise<T>): Promise<T> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-digest-"));
  try {
    return await fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function at<T>(items: readonly T[], index: number): T {
  const item = items[index];
  if (item === undefined) {
    throw new Error(`missing item at index ${index}`);
  }
  return item;
}

async function writeDigestBundle(bundle: string): Promise<void> {
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), DIGEST_PIPELINE_YAML);
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(bundle, "prompts", "architect.md"), "review the implementation\n");
  await writeFile(join(bundle, "schemas", "config.schema.json"), JSON.stringify(CONFIG_SCHEMA));
  await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  await writeFile(join(bundle, "decisions", "gate.yaml"), GATE_MODEL_YAML);
}

interface DigestBundle {
  bundle: string;
  json: string;
  digest: string;
}

async function loadDigestBundle(bundle: string): Promise<DigestBundle> {
  const pipeline = await loadPipelineV2(bundle);
  return {
    bundle,
    json: pipelineV2ExecutionSnapshotJson(pipeline),
    digest: pipelineV2ExecutionDigest(pipeline),
  };
}

async function digestAfterMutation(
  mutate: (bundle: string) => Promise<void>,
): Promise<{ before: DigestBundle; after: DigestBundle }> {
  return await withTempResult(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const before = await loadDigestBundle(bundle);
    await mutate(bundle);
    const after = await loadDigestBundle(bundle);
    return { before, after };
  });
}

async function replaceInPipelineYaml(
  bundle: string,
  from: string,
  to: string,
): Promise<void> {
  const path = join(bundle, "pipeline.yaml");
  const raw = await readFile(path, "utf8");
  if (!raw.includes(from)) {
    throw new Error(`pipeline.yaml does not contain ${JSON.stringify(from)}`);
  }
  await writeFile(path, raw.replace(from, to));
}

function sortedKeys(value: object): string[] {
  return Object.keys(value).sort();
}

function assertFullyFrozen(value: unknown, path: string): void {
  if (value === null || typeof value !== "object") {
    return;
  }
  if (!Object.isFrozen(value)) {
    throw new Error(`not frozen: ${path}`);
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertFullyFrozen(item, `${path}[${index}]`));
    return;
  }
  for (const key of Object.keys(value)) {
    assertFullyFrozen((value as Record<string, unknown>)[key], `${path}.${key}`);
  }
}

test("identical bundles in different absolute directories produce identical snapshot JSON, digest and relocating identity", async () => {
  await withTemp(async (root) => {
    const bundleA = join(root, "location-a", "bundle");
    await writeDigestBundle(bundleA);
    const bundleB = join(root, "location-b", "elsewhere");
    await cp(bundleA, bundleB, { recursive: true });
    const pipelineA = await loadPipelineV2(bundleA);
    const pipelineB = await loadPipelineV2(bundleB);
    expect(pipelineV2ExecutionSnapshotJson(pipelineA)).toBe(
      pipelineV2ExecutionSnapshotJson(pipelineB),
    );
    expect(pipelineV2ExecutionDigest(pipelineA)).toBe(pipelineV2ExecutionDigest(pipelineB));
    expect(pipelineV2ExecutionSnapshot(pipelineA)).toEqual(
      pipelineV2ExecutionSnapshot(pipelineB),
    );
    const identityA = pipelineV2RunPipelineIdentity(pipelineA);
    const identityB = pipelineV2RunPipelineIdentity(pipelineB);
    expect(identityA.bundle_root).toBe(await realpath(bundleA));
    expect(identityB.bundle_root).toBe(await realpath(bundleB));
    expect(identityA.bundle_root).not.toBe(identityB.bundle_root);
    const relocated = { ...identityA, bundle_root: null } as Record<string, unknown>;
    expect(relocated).toEqual({ ...identityB, bundle_root: null });
  });
});

test("repeated calls on one trusted snapshot are fully deterministic", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const pipeline = await loadPipelineV2(bundle);
    const json1 = pipelineV2ExecutionSnapshotJson(pipeline);
    const json2 = pipelineV2ExecutionSnapshotJson(pipeline);
    const json3 = pipelineV2ExecutionSnapshotJson(pipeline);
    expect(json1).toBe(json2);
    expect(json2).toBe(json3);
    const digest1 = pipelineV2ExecutionDigest(pipeline);
    const digest2 = pipelineV2ExecutionDigest(pipeline);
    expect(digest1).toBe(digest2);
    expect(pipelineV2ExecutionSnapshot(pipeline)).toEqual(
      pipelineV2ExecutionSnapshot(pipeline),
    );
    expect(pipelineV2RunPipelineIdentity(pipeline)).toEqual(
      pipelineV2RunPipelineIdentity(pipeline),
    );
  });
});

test("snapshot and all nested structures are deep-frozen", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const pipeline = await loadPipelineV2(bundle);
    const snapshot = pipelineV2ExecutionSnapshot(pipeline);
    assertFullyFrozen(snapshot, "snapshot");
  });
});

test("hand-built, cloned, spread and proxied pipelines are rejected before any field is read", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const trusted = await loadPipelineV2(bundle);
    const forged: readonly (readonly [string, ResolvedPipelineV2])[] = [
      ["hand-built", JSON.parse(JSON.stringify(trusted)) as ResolvedPipelineV2],
      ["spread", { ...trusted }],
      ["structuredClone", structuredClone(trusted)],
      ["empty object", {} as ResolvedPipelineV2],
    ];
    for (const [name, candidate] of forged) {
      expect(() => pipelineV2ExecutionSnapshot(candidate)).toThrow(PipelineError);
      expect(() => pipelineV2ExecutionSnapshotJson(candidate)).toThrow(PipelineError);
      expect(() => pipelineV2ExecutionDigest(candidate)).toThrow(PipelineError);
      expect(() => pipelineV2RunPipelineIdentity(candidate)).toThrow(PipelineError);
      void name;
    }
    let gets = 0;
    const proxy = new Proxy(trusted, {
      get(target, key, receiver) {
        gets += 1;
        return Reflect.get(target, key, receiver);
      },
    });
    expect(() => pipelineV2ExecutionSnapshot(proxy)).toThrow(PipelineError);
    expect(gets).toBe(0);
    for (const [, candidate] of forged) {
      expect(() => pipelineV2ExecutionSnapshot(candidate)).toThrow(
        "requires the deep-frozen snapshot object returned by loadPipelineV2",
      );
    }
  });
});

test("each public function rejects forged input under its own stable name", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const trusted = await loadPipelineV2(bundle);
    const forged = { ...trusted };
    expect(() => pipelineV2ExecutionSnapshot(forged)).toThrow(
      "pipelineV2ExecutionSnapshot requires the deep-frozen snapshot object returned by loadPipelineV2",
    );
    expect(() => pipelineV2ExecutionSnapshotJson(forged)).toThrow(
      "pipelineV2ExecutionSnapshotJson requires the deep-frozen snapshot object returned by loadPipelineV2",
    );
    expect(() => pipelineV2ExecutionDigest(forged)).toThrow(
      "pipelineV2ExecutionDigest requires the deep-frozen snapshot object returned by loadPipelineV2",
    );
    expect(() => pipelineV2RunPipelineIdentity(forged)).toThrow(
      "pipelineV2RunPipelineIdentity requires the deep-frozen snapshot object returned by loadPipelineV2",
    );
    expect(trusted.schema_version).toBe(2);
  });
});

test("snapshot describes the compiled v2 semantics with exact shapes and original transition indexes", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const pipeline = await loadPipelineV2(bundle);
    const snapshot = pipelineV2ExecutionSnapshot(pipeline) as Record<string, unknown>;
    expect(sortedKeys(snapshot)).toEqual(
      ["entry_state", "inputs", "max_transitions", "outputs", "schema_version", "states"],
    );
    expect(snapshot.schema_version).toBe(2);
    expect(snapshot.entry_state).toBe("coder");
    expect(snapshot.max_transitions).toBe(20);
    expect(snapshot.inputs).toEqual([
      { id: "task", type: "file", protected: true },
      {
        id: "config",
        type: "json",
        protected: true,
        schema_path: "schemas/config.schema.json",
        schema_value: CONFIG_SCHEMA,
      },
    ]);
    expect(snapshot.outputs).toEqual([
      {
        id: "final_report",
        required: true,
        source: { state_output: { state: "architect", output: "report" } },
        type: "file",
      },
      {
        id: "facts_digest",
        required: false,
        source: { state_output: { state: "architect", output: "facts" } },
        type: "json",
        schema_value: FACTS_SCHEMA,
      },
    ]);
    const states = snapshot.states as Record<string, unknown>[];
    expect(states.map((state) => state.id)).toEqual([
      "coder",
      "gate",
      "architect",
      "done",
      "rejected",
    ]);
    const coder = at(states, 0);
    expect(sortedKeys(coder)).toEqual([
      "id",
      "inputs",
      "max_attempts",
      "outputs",
      "profile",
      "prompt",
      "prompt_content",
      "timeout_seconds",
      "transitions",
      "type",
    ]);
    expect(coder.profile).toBe("coder");
    expect(coder.prompt).toBe("prompts/coder.md");
    expect(coder.prompt_content).toBe("implement the task\n");
    expect(coder.inputs).toEqual([
      { id: "task", source: { pipeline_input: "task" }, type: "file" },
      {
        id: "cfg",
        source: { pipeline_input: "config" },
        type: "json",
        schema_value: CONFIG_SCHEMA,
      },
    ]);
    expect(coder.outputs).toEqual([{ id: "implementation", type: "file" }]);
    expect(coder.timeout_seconds).toBe(1800);
    expect(coder.max_attempts).toBe(1);
    expect(coder.transitions).toEqual([{ index: 0, outcome: "completed", to: "gate" }]);

    const gate = at(states, 1);
    expect(sortedKeys(gate)).toEqual([
      "decision_model",
      "id",
      "inputs",
      "model",
      "transitions",
      "type",
    ]);
    expect(gate.model).toBe("decisions/gate.yaml");
    expect(gate.decision_model).toEqual({
      fact_ids: ["f1"],
      decision_ids: ["d1"],
      relations: [],
      constraints: [],
      rules: [
        {
          id: "r1",
          when: { kind: "fact", fact_index: 0, expected: true },
          decision_index: 0,
        },
      ],
    });
    expect(gate.inputs).toEqual([
      {
        id: "facts",
        source: { pipeline_input: "config" },
        type: "json",
        schema_value: CONFIG_SCHEMA,
      },
    ]);
    expect(gate.transitions).toEqual([
      { index: 0, outcome: "d1", to: "architect" },
      { index: 1, outcome: "uncovered", to: "rejected" },
      { index: 2, outcome: "inconsistent_facts", to: "rejected" },
      { index: 3, outcome: "invalid_facts", to: "rejected" },
    ]);

    const architect = at(states, 2);
    expect(architect.outputs).toEqual([
      {
        id: "facts",
        type: "json",
        schema_path: "schemas/facts.schema.json",
        schema_value: FACTS_SCHEMA,
      },
      { id: "report", type: "file" },
    ]);
    expect(architect.transitions).toEqual([
      { index: 0, outcome: "completed", to: "done" },
      { index: 1, outcome: "needs_changes", to: "coder" },
    ]);
    expect(architect.max_attempts).toBe(2);

    expect(at(states, 3)).toEqual({ id: "done", type: "terminal", result: "success" });
    expect(at(states, 4)).toEqual({ id: "rejected", type: "terminal", result: "failed" });
  });
});

test("identity has the exact durable form of state schema v3", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const pipeline = await loadPipelineV2(bundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    expect(sortedKeys(identity)).toEqual([
      "bundle_root",
      "entry_state",
      "execution_snapshot_sha256",
      "max_transitions",
      "schema_version",
    ]);
    expect(identity).toEqual({
      schema_version: 2,
      bundle_root: await realpath(bundle),
      execution_snapshot_sha256: pipelineV2ExecutionDigest(pipeline),
      entry_state: "coder",
      max_transitions: 20,
    });
    expect(identity.execution_snapshot_sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(identity.bundle_root).toMatch(/^\//);
  });
});

test("create_run accepts the built identity and the state survives the loader round-trip", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const pipeline = await loadPipelineV2(bundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const inputs = pipeline.inputs.map((input) => ({
      id: input.id,
      type: input.type,
      protected: input.protected,
      digest: new Bun.CryptoHasher("sha256").update(input.id).digest("hex"),
    }));
    const state = reducePipelineV2RunCommand(
      null,
      { kind: "create_run", runId: "digest-run", pipeline: identity, inputs },
      new Date(0),
    );
    expect(state.revision).toBe(1);
    expect(state.status).toBe("active");
    expect(state.pipeline).toEqual(identity);
    expect(state.cursor).toEqual({ current_state: "coder", transition_count: 0 });
    const roundTrip = validatePipelineV2RunState(JSON.parse(JSON.stringify(state)));
    expect(roundTrip).toEqual(state);
  });
});

test("the canonical snapshot contains no absolute bundle or host paths and no runtime-only fields", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const pipeline = await loadPipelineV2(bundle);
    const json = pipelineV2ExecutionSnapshotJson(pipeline);
    const canonicalBundle = await realpath(bundle);
    const forbidden = [
      canonicalBundle,
      root,
      join(canonicalBundle, "prompts", "coder.md"),
      join(canonicalBundle, "prompts", "architect.md"),
      join(canonicalBundle, "schemas", "config.schema.json"),
      join(canonicalBundle, "schemas", "facts.schema.json"),
      join(canonicalBundle, "decisions", "gate.yaml"),
      join(canonicalBundle, "pipeline.yaml"),
      "/workspace",
      "/run",
      "/opt/orchestrator",
    ];
    for (const path of forbidden) {
      expect(json).not.toContain(path);
    }
    const required = [
      "prompts/coder.md",
      "prompts/architect.md",
      "schemas/config.schema.json",
      "schemas/facts.schema.json",
      "decisions/gate.yaml",
      "implement the task",
      "review the implementation",
    ];
    for (const needle of required) {
      expect(json).toContain(needle);
    }
  });
});

test("the snapshot contains no credential, env, session or runtime canaries", async () => {
  await withTemp(async (root) => {
    const bundle = join(root, "bundle");
    await writeDigestBundle(bundle);
    const pipeline = await loadPipelineV2(bundle);
    const json = pipelineV2ExecutionSnapshotJson(pipeline);
    const identityJson = JSON.stringify(pipelineV2RunPipelineIdentity(pipeline));
    const canaries = [
      "dhcr_",
      "dht_",
      "bearer",
      "Bearer",
      "OPENCODE_CONFIG_CONTENT",
      "openCode",
      "http+unix",
      "docker.sock",
      "credential",
      "password",
      "api_key",
      "session_id",
      "session-",
      "run_id",
      "digest-run",
      "stdout",
      "stderr",
      "started_at",
      "updated_at",
      "activation",
      "launcher",
      "profile_env",
      "DOCKER_HELPER",
    ];
    for (const canary of canaries) {
      expect(json).not.toContain(canary);
      expect(identityJson).not.toContain(canary);
    }
  });
});

test("changing prompt content changes the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    await writeFile(join(bundle, "prompts", "coder.md"), "implement the task thoroughly\n");
  });
  expect(before.json).not.toBe(after.json);
  expect(before.digest).not.toBe(after.digest);
});

test("changing a profile name changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(bundle, "profile: coder\n", "profile: coder_primary\n"),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing an agent timeout changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(bundle, "timeout_seconds: 1800\n    max_attempts: 1", "timeout_seconds: 1799\n    max_attempts: 1"),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing max_attempts changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(bundle, "max_attempts: 2", "max_attempts: 3"),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing an agent input port source changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(
      bundle,
      "      - id: cfg\n        source:\n          pipeline_input: config",
      "      - id: cfg\n        source:\n          pipeline_input: task",
    ),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing an agent output port type changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(
      bundle,
      "      - id: report\n        type: file",
      "      - id: report\n        type: directory",
    ),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing a run output source changes the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    await replaceInPipelineYaml(
      bundle,
      "      state_output:\n        state: architect\n        output: report",
      "      state_output:\n        state: coder\n        output: implementation",
    );
  });
  expect(before.digest).not.toBe(after.digest);
});

test("changing a run input protected flag changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(
      bundle,
      "  - id: task\n    type: file\n    protected: true",
      "  - id: task\n    type: file\n    protected: false",
    ),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing a run output required flag changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(bundle, "  - id: final_report\n    required: true", "  - id: final_report\n    required: false"),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing a JSON schema changes the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    await writeFile(
      join(bundle, "schemas", "facts.schema.json"),
      JSON.stringify({
        type: "object",
        required: ["revision"],
        properties: { revision: { type: "integer" }, note: { type: "string" } },
      }),
    );
  });
  expect(before.digest).not.toBe(after.digest);
});

test("changing a decision model rule changes the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    const path = join(bundle, "decisions", "gate.yaml");
    const raw = await readFile(path, "utf8");
    if (!raw.includes("equals: true")) {
      throw new Error("gate.yaml does not contain the rule");
    }
    await writeFile(path, raw.replace("equals: true", "equals: false"));
  });
  expect(before.digest).not.toBe(after.digest);
});

test("adding a decision model constraint changes the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    const path = join(bundle, "decisions", "gate.yaml");
    const raw = await readFile(path, "utf8");
    if (!raw.includes("constraints: []")) {
      throw new Error("gate.yaml has unexpected constraints");
    }
    await writeFile(
      path,
      raw.replace(
        "constraints: []",
        "constraints:\n  - id: hc1\n    when:\n      fact: f1\n      equals: true\n    forbid:\n      - d1",
      ),
    );
  });
  expect(before.digest).not.toBe(after.digest);
});

test("changing a transition target changes the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    await replaceInPipelineYaml(
      bundle,
      "      - outcome: needs_changes\n        to: coder",
      "      - outcome: needs_changes\n        to: gate",
    );
  });
  expect(before.digest).not.toBe(after.digest);
});

test("changing a transition outcome changes the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    await replaceInPipelineYaml(
      bundle,
      "      - outcome: needs_changes\n        to: coder",
      "      - outcome: review_requested\n        to: coder",
    );
  });
  expect(before.digest).not.toBe(after.digest);
});

test("changing transition declaration order changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(
      bundle,
      "      - outcome: completed\n        to: done\n      - outcome: needs_changes\n        to: coder",
      "      - outcome: needs_changes\n        to: coder\n      - outcome: completed\n        to: done",
    ),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing the entry state changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(bundle, "entry_state: coder\n", "entry_state: gate\n"),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing the transition budget changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(bundle, "max_transitions: 20\n", "max_transitions: 21\n"),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing a terminal result changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(
      bundle,
      "  - id: rejected\n    type: terminal\n    result: failed",
      "  - id: rejected\n    type: terminal\n    result: success",
    ),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing run input declaration order changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(
      bundle,
      "inputs:\n  - id: task\n    type: file\n    protected: true\n  - id: config\n    type: json\n    protected: true\n    schema: schemas/config.schema.json",
      "inputs:\n  - id: config\n    type: json\n    protected: true\n    schema: schemas/config.schema.json\n  - id: task\n    type: file\n    protected: true",
    ),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("changing state declaration order changes the digest", async () => {
  const { before, after } = await digestAfterMutation((bundle) =>
    replaceInPipelineYaml(
      bundle,
      "  - id: done\n    type: terminal\n    result: success\n  - id: rejected\n    type: terminal\n    result: failed",
      "  - id: rejected\n    type: terminal\n    result: failed\n  - id: done\n    type: terminal\n    result: success",
    ),
  );
  expect(before.digest).not.toBe(after.digest);
});

test("YAML whitespace and mapping key order do not change the digest", async () => {
  const { before, after } = await digestAfterMutation(async (bundle) => {
    await writeFile(join(bundle, "pipeline.yaml"), DIGEST_PIPELINE_YAML_REFORMATTED);
    await writeFile(join(bundle, "decisions", "gate.yaml"), GATE_MODEL_YAML_REFORMATTED);
  });
  expect(after.json).toBe(before.json);
  expect(after.digest).toBe(before.digest);
});
