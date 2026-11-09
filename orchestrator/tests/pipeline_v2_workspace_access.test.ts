import { mkdir, mkdtemp, readFile, rm, stat, writeFile, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  loadPipelineV2,
  planActivationLayout,
  PIPELINE_V2_WORKSPACE_ACCESS_VALUES,
  type ResolvedPipelineV2,
} from "../src/pipeline_v2.ts";
import { PipelineError } from "../src/pipeline.ts";
import type { ActivationLayoutPlan } from "../src/pipeline_v2.ts";
import {
  pipelineV2ExecutionDigest,
  pipelineV2ExecutionSnapshotJson,
  pipelineV2RunPipelineIdentity,
} from "../src/pipeline_v2_digest.ts";
import {
  compiledWorkspaceAccessFor,
  PipelineV2OrchestrationError,
} from "../src/pipeline_v2_orchestration.ts";
import {
  prepareActivationData,
  acceptActivationOutputs,
  snapshotRunInputs,
  type PreparedActivationData,
  type RunInputsSnapshot,
} from "../src/pipeline_v2_runtime.ts";
import { reducePipelineV2RunCommand, type PipelineV2RunState } from "../src/pipeline_v2_state.ts";
import { restorePipelineV2RuntimeContext, PipelineV2RuntimeContextRestoreError } from "../src/pipeline_v2_resume_context.ts";
import { pipelineV2ProjectProvenanceBinding, preparePipelineV2ProjectProvenance } from "../src/pipeline_v2_project_provenance.ts";

const FEEDBACK_SCHEMA = {
  type: "object",
  required: ["verdict"],
  properties: { verdict: { type: "string" } },
};

const DECISION_MODEL = `
schema_version: 1
facts:
  - id: ok
decisions:
  - id: go
relations: []
constraints: []
rules:
  - id: r1
    when: {fact: ok, equals: true}
    decision: go
`;

function stageRoleYaml(access: string | null): string {
  const field = access === null ? "" : `\n      workspace_access: ${access}`;
  return `
schema_version: 2
entry_state: dev
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev
  execution_roles:
    - state_id: dev
      role: stage
      stage_template: development${field}

states:
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs:
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: feedback
        type: json
        schema: schemas/feedback.schema.json
    timeout_seconds: 30
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;
}

async function writeBundle(root: string, yaml: string, model: string | null = null): Promise<string> {
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), yaml);
  await writeFile(join(bundle, "prompts/dev.md"), "work\n");
  await writeFile(join(bundle, "schemas/feedback.schema.json"), JSON.stringify(FEEDBACK_SCHEMA));
  if (model !== null) {
    await writeFile(join(bundle, "decisions/check.yaml"), model);
  }
  return bundle;
}

async function loadFrom(root: string, yaml: string, model: string | null = null): Promise<ResolvedPipelineV2> {
  const bundle = await writeBundle(root, yaml, model);
  return loadPipelineV2(bundle);
}

// --- A. Parser/compiler ------------------------------------------------------

test("A1. a stage agent without workspace_access compiles and resolves effective read_write", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml(null));
    expect(compiledWorkspaceAccessFor(pipeline, "dev")).toBe("read_write");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A2. a stage agent with workspace_access read_write resolves read_write", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml("read_write"));
    expect(compiledWorkspaceAccessFor(pipeline, "dev")).toBe("read_write");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A3. a stage agent with workspace_access read_only resolves read_only", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml("read_only"));
    expect(compiledWorkspaceAccessFor(pipeline, "dev")).toBe("read_only");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A4. an unknown workspace_access value is rejected and never echoed", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    for (const hostile of ["READ_ONLY", "read-only", "writeable", 42, "null", true]) {
      let message = "";
      try {
        await loadFrom(root, stageRoleYaml(hostile as string));
      } catch (cause) {
        message = (cause as Error).message;
      }
      expect(message).toContain("workspace_access");
      expect(message).toContain("read_write");
      expect(message).not.toContain("READ_ONLY");
      expect(message).not.toContain("read-only");
      expect(message).not.toContain("writeable");
      expect(message).not.toContain("42");
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A5. a planning entry carrying workspace_access is rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const yaml = `
schema_version: 2
entry_state: dev
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: dev
  execution_roles:
    - state_id: dev
      role: planning
      plan_output: feedback
      stage_wait:
        reason: review_wait
        actions:
          - continue_stage
      workspace_access: read_only

states:
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs:
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: feedback
        type: json
        schema: schemas/feedback.schema.json
    timeout_seconds: 30
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;
    let message = "";
    try {
      await loadFrom(root, yaml);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain('unknown field "workspace_access"');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A6. a control entry carrying workspace_access is rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const yaml = `
schema_version: 2
entry_state: dev
max_transitions: 20

inputs: []

outputs: []

orchestration:
  stage_templates: []
  execution_roles:
    - state_id: dev
      role: planning
      plan_output: facts
      stage_wait:
        reason: review_wait
        actions:
          - continue_stage
    - state_id: check
      role: control
      workspace_access: read_only

states:
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs: []
    outputs:
      - id: facts
        type: json
        schema: schemas/feedback.schema.json
    timeout_seconds: 30
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check

  - id: check
    type: decision
    model: decisions/check.yaml
    inputs:
      - id: facts
        source:
          state_output:
            state: dev
            output: facts
    transitions:
      - outcome: go
        to: done
      - outcome: uncovered
        to: done
      - outcome: inconsistent_facts
        to: done
      - outcome: invalid_facts
        to: done

  - id: done
    type: terminal
    result: success
`;
    let message = "";
    try {
      await loadFrom(root, yaml, DECISION_MODEL);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain('unknown field "workspace_access"');
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A7. a stage role bound to a decision state carrying workspace_access is rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const yaml = `
schema_version: 2
entry_state: dev
max_transitions: 20

inputs: []

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: check
  execution_roles:
    - state_id: dev
      role: planning
      plan_output: facts
      stage_wait:
        reason: review_wait
        actions:
          - continue_stage
    - state_id: check
      role: stage
      stage_template: development
      workspace_access: read_only

states:
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs: []
    outputs:
      - id: facts
        type: json
        schema: schemas/feedback.schema.json
    timeout_seconds: 30
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check

  - id: check
    type: decision
    model: decisions/check.yaml
    inputs:
      - id: facts
        source:
          state_output:
            state: dev
            output: facts
    transitions:
      - outcome: go
        to: done
      - outcome: uncovered
        to: done
      - outcome: inconsistent_facts
        to: done
      - outcome: invalid_facts
        to: done

  - id: done
    type: terminal
    result: success
`;
    let message = "";
    try {
      await loadFrom(root, yaml, DECISION_MODEL);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain("workspace_access");
    expect(message).toContain("decision");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A8. a read-only stage agent with declared JSON outputs compiles and the resolved role carries the policy", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml("read_only"));
    const role = pipeline.orchestration?.execution_roles.find((entry) => entry.state_id === "dev");
    expect(role).toBeDefined();
    expect(role?.role).toBe("stage");
    if (role !== undefined && role.role === "stage") {
      expect(role.stage_template).toBe("development");
      expect(role.workspace_access).toBe("read_only");
    }
    const state = pipeline.states.find((entry) => entry.id === "dev");
    expect(state?.type).toBe("agent");
    expect(state !== undefined && "outputs" in state ? state.outputs.map((port) => port.id) : []).toEqual([
      "feedback",
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A9. extra and non-scalar policy shapes fail closed", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    // an unrelated policy field on a stage entry is an unknown field
    const extraYaml = stageRoleYaml("read_only").replace(
      "      workspace_access: read_only",
      "      workspace_access: read_only\n      plan_ready:\n        stage_position: 1\n        initial_budget: 1",
    );
    let message = "";
    try {
      await loadFrom(root, extraYaml);
    } catch (cause) {
      message = (cause as Error).message;
    }
    expect(message).toContain('unknown field "plan_ready"');

    // a non-scalar workspace_access value is rejected without echo
    for (const hostile of [{ access: "read_only" }, { field: true }]) {
      let hostileMessage = "";
      try {
        await loadFrom(root, stageRoleYaml(hostile as unknown as string));
      } catch (cause) {
        hostileMessage = (cause as Error).message;
      }
      expect(hostileMessage).toContain("workspace_access must be one of");
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("A10. resolved orchestration objects are deep-frozen", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml("read_only"));
    const role = pipeline.orchestration?.execution_roles.find((entry) => entry.state_id === "dev");
    expect(Object.isFrozen(pipeline.orchestration)).toBe(true);
    expect(Object.isFrozen(role)).toBe(true);
    expect(() => {
      (role as unknown as Record<string, unknown>)["workspace_access"] = "read_write";
    }).toThrow();
    expect(() => {
      (pipeline.orchestration as unknown as Record<string, unknown>)["execution_roles"] = [];
    }).toThrow();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

// --- resolver battery --------------------------------------------------------

test("R1. the resolver resolves planning to the established read-write and rejects non-agent states and unknown states", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    // One valid mixed bundle: dev is a planning agent, check is a stage
    // role bound to a decision state (no policy field), done is terminal.
    const mixedYaml = `
schema_version: 2
entry_state: dev
max_transitions: 20

inputs: []

outputs: []

orchestration:
  stage_templates:
    - id: development
      entry_state: check
  execution_roles:
    - state_id: dev
      role: planning
      plan_output: facts
      stage_wait:
        reason: review_wait
        actions:
          - continue_stage
    - state_id: check
      role: stage
      stage_template: development

states:
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs: []
    outputs:
      - id: facts
        type: json
        schema: schemas/feedback.schema.json
    timeout_seconds: 30
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check

  - id: check
    type: decision
    model: decisions/check.yaml
    inputs:
      - id: facts
        source:
          state_output:
            state: dev
            output: facts
    transitions:
      - outcome: go
        to: done
      - outcome: uncovered
        to: done
      - outcome: inconsistent_facts
        to: done
      - outcome: invalid_facts
        to: done

  - id: done
    type: terminal
    result: success
`;
    const mixed = await loadFrom(root, mixedYaml, DECISION_MODEL);

    let planning = "";
    try {
      planning = compiledWorkspaceAccessFor(mixed, "dev");
    } catch (cause) {
      planning = `unexpected error: ${(cause as Error).message}`;
    }
    expect(planning).toBe("read_write");

    let decision = "";
    try {
      compiledWorkspaceAccessFor(mixed, "check");
    } catch (cause) {
      decision = (cause as Error).message;
    }
    expect(decision).toContain("not a declared agent state");

    let unknown = "";
    const pipeline = await loadFrom(root, stageRoleYaml("read_only"));
    try {
      compiledWorkspaceAccessFor(pipeline, "nope");
    } catch (cause) {
      unknown = (cause as Error).message;
    }
    expect(unknown).toContain("not declared by the pipeline");

    let terminal = "";
    try {
      compiledWorkspaceAccessFor(pipeline, "done");
    } catch (cause) {
      terminal = (cause as Error).message;
    }
    expect(terminal).toContain("terminal states carry no execution role");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("R2. the resolver resolves orchestration-less pipelines to the established read-write and rejects forged snapshots", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const yaml = stageRoleYaml("read_only").replace(
      `orchestration:
  stage_templates:
    - id: development
      entry_state: dev
  execution_roles:
    - state_id: dev
      role: stage
      stage_template: development
      workspace_access: read_only

`,
      "",
    );
    const pipeline = await loadFrom(root, yaml);
    expect(pipeline.orchestration).toBeUndefined();
    // No orchestration section: no stage policy can exist, so every agent
    // state keeps the established read-write workspace (a structural fact,
    // not a guess).
    expect(compiledWorkspaceAccessFor(pipeline, "dev")).toBe("read_write");

    const trusted = await loadFrom(root, stageRoleYaml("read_only"));
    let forged = "";
    try {
      compiledWorkspaceAccessFor(structuredClone(trusted) as unknown as ResolvedPipelineV2, "dev");
    } catch (cause) {
      forged = (cause as Error).message;
    }
    expect(forged.length > 0).toBe(true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("R3. repeated resolver calls are structurally identical and never mutate the pipeline", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml("read_only"));
    const first = compiledWorkspaceAccessFor(pipeline, "dev");
    const second = compiledWorkspaceAccessFor(pipeline, "dev");
    expect(first).toBe(second);
    expect(first).toBe("read_only");
    const digest = pipelineV2ExecutionDigest(pipeline);
    expect(pipelineV2ExecutionDigest(pipeline)).toBe(digest);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

// --- B. Digest and compatibility ---------------------------------------------

test("B1. a bundle without the field keeps the exact pre-policy snapshot shape (no key anywhere)", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml(null));
    const json = pipelineV2ExecutionSnapshotJson(pipeline);
    expect(json.includes("workspace_access")).toBe(false);
    const role = pipeline.orchestration?.execution_roles.find((entry) => entry.state_id === "dev");
    expect(Object.keys(role as object).sort()).toEqual(["role", "stage_template", "state_id"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("B2. declaring read_only moves the digest; read_write moves it differently; removal restores the baseline", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const baseline = await loadFrom(root, stageRoleYaml(null));
    const baselineDigest = pipelineV2ExecutionDigest(baseline);

    const ro = await loadFrom(root, stageRoleYaml("read_only"));
    const roDigest = pipelineV2ExecutionDigest(ro);
    expect(roDigest).not.toBe(baselineDigest);

    const rw = await loadFrom(root, stageRoleYaml("read_write"));
    const rwDigest = pipelineV2ExecutionDigest(rw);
    expect(rwDigest).not.toBe(baselineDigest);
    expect(rwDigest).not.toBe(roDigest);

    // removal restores the exact baseline digest
    const removed = await loadFrom(root, stageRoleYaml(null));
    expect(pipelineV2ExecutionDigest(removed)).toBe(baselineDigest);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("B3. bundle relocation stays digest-neutral with and without the policy", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const first = await loadFrom(root, stageRoleYaml("read_only"));
    const secondRoot = join(root, "elsewhere");
    await mkdir(secondRoot, { recursive: true });
    const bundle = await writeBundle(secondRoot, stageRoleYaml("read_only"));
    const second = await loadPipelineV2(bundle);
    expect(pipelineV2ExecutionDigest(second)).toBe(pipelineV2ExecutionDigest(first));
    expect(second.bundleRoot).not.toBe(first.bundleRoot);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("B4. a changed workspace access is a pipeline_mismatch refusal before any filesystem effect", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const baseline = await loadFrom(root, stageRoleYaml(null));
    const readOnly = await loadFrom(root, stageRoleYaml("read_only"));
    expect(pipelineV2RunPipelineIdentity(readOnly).execution_snapshot_sha256).not.toBe(
      pipelineV2RunPipelineIdentity(baseline).execution_snapshot_sha256,
    );

    // A durable run created from the baseline bundle...
    const state = reduceRunFor(baseline);
    expect(state).not.toBeNull();

    // ...refuses the read-only bundle before any filesystem access: the
    // run root does not exist and is never created.
    const missingRunRoot = join(root, "run", "run-1");
    let reason: string | null = null;
    try {
      await restorePipelineV2RuntimeContext(readOnly, state as PipelineV2RunState, missingRunRoot);
    } catch (cause) {
      if (cause instanceof PipelineV2RuntimeContextRestoreError) {
        reason = cause.reason;
      } else {
        throw cause;
      }
    }
    expect(reason).toBe("pipeline_mismatch");
    let exists = false;
    try {
      await stat(missingRunRoot);
      exists = true;
    } catch {
      exists = false;
    }
    expect(exists).toBe(false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

function reduceRunFor(pipeline: ResolvedPipelineV2): PipelineV2RunState | null {
  const identity = pipelineV2RunPipelineIdentity(pipeline);
  const commands: Parameters<typeof reducePipelineV2RunCommand>[1][] = [
    {
      kind: "create_run",
      runId: "run-1",
      pipeline: identity,
      inputs: [
        { id: "task", type: "file", protected: true, digest: "a".repeat(64) },
      ],
      projectProvenance: pipelineV2ProjectProvenanceBinding(preparePipelineV2ProjectProvenance([])),
      profileEpoch: { digest: "d".repeat(64), profile_count: 1 },
    },
  ];
  let current: PipelineV2RunState | null = null;
  for (const command of commands) {
    current = reducePipelineV2RunCommand(current, command, new Date(Date.UTC(2026, 0, 1, 0, 0, 0)));
  }
  return current;
}

// --- D. Output preservation (host-side, read-only reviewer fixture) ----------

async function reviewerFixture(root: string): Promise<{
  pipeline: ResolvedPipelineV2;
  snapshot: RunInputsSnapshot;
  prepared: PreparedActivationData;
  projectDir: string;
}> {
  const bundle = await writeBundle(root, stageRoleYaml("read_only"));
  const pipeline = await loadPipelineV2(bundle);
  const sources = join(root, "userdata");
  await mkdir(sources, { recursive: true });
  await writeFile(join(sources, "task.txt"), "the task\n");
  const runRoot = join(root, "run");
  await mkdir(join(runRoot, "project"), { recursive: true, mode: 0o700 });
  await writeFile(join(runRoot, "project", "README.md"), "project seed\n");
  const snapshot = await snapshotRunInputs(
    pipeline,
    [{ id: "task", path: join(sources, "task.txt") }],
    runRoot,
  );
  const prepared = await prepareActivationData(pipeline, snapshot, [], "dev", 1);
  return { pipeline, snapshot, prepared, projectDir: join(runRoot, "project") };
}

async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (rel: string): Promise<void> => {
    const abs = rel === "" ? root : join(root, rel);
    const info = await stat(abs);
    if (info.isDirectory()) {
      lines.push(`${rel}/ dir ${info.mode.toString(8)}`);
      for (const entry of [...(await readdir(abs))].sort()) {
        await walk(rel === "" ? entry : `${rel}/${entry}`);
      }
      return;
    }
    lines.push(`${rel} file ${info.mode.toString(8)} ${(await readFile(abs)).toString("base64")}`);
  };
  await walk("");
  return lines.join("\n");
}

test("D1. the read-only reviewer writes its declared JSON output and the project stays byte-identical", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const fixture = await reviewerFixture(root);
    const before = await fingerprint(fixture.projectDir);
    await writeFile(
      join(fixture.prepared.outputs_root, "feedback"),
      JSON.stringify({ verdict: "looks good" }),
    );
    const accepted = await acceptActivationOutputs(fixture.pipeline, fixture.prepared);
    expect(accepted.map((record) => `${record.state}/${record.output}`)).toEqual(["dev/feedback"]);
    expect(accepted[0]?.activation_index).toBe(1);
    expect(accepted[0]?.digest).toMatch(/^[0-9a-f]{64}$/);
    const after = await fingerprint(fixture.projectDir);
    expect(after).toBe(before);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("D2. the prepared activation mounts and execution document state the effective access truthfully", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const { pipeline, snapshot, prepared } = await reviewerFixture(root);
    expect(prepared.mounts[0]).toEqual({
      source: prepared.project_root,
      target: "/workspace",
      read_only: true,
    });
    expect(prepared.mounts[1]?.read_only).toBe(true);
    expect(prepared.mounts[2]?.read_only).toBe(false);
    const doc = await readFile(prepared.execution_document.host_path, "utf8");
    expect(doc).toContain("read-only; write only to your declared outputs");
    expect(doc.includes("read-write")).toBe(false);
    void pipeline;
    void snapshot;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("D3. the read-write baseline keeps the established mounts and execution document byte-for-byte", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const pipeline = await loadFrom(root, stageRoleYaml("read_write"));
    const sources = join(root, "userdata");
    await mkdir(sources, { recursive: true });
    await writeFile(join(sources, "task.txt"), "the task\n");
    const runRoot = join(root, "run");
    await mkdir(join(runRoot, "project"), { recursive: true, mode: 0o700 });
    await writeFile(join(runRoot, "project", "README.md"), "project seed\n");
    const snapshot = await snapshotRunInputs(
      pipeline,
      [{ id: "task", path: join(sources, "task.txt") }],
      runRoot,
    );
    const prepared = await prepareActivationData(pipeline, snapshot, [], "dev", 1);
    expect(prepared.mounts[0]).toEqual({
      source: prepared.project_root,
      target: "/workspace",
      read_only: false,
    });
    const doc = await readFile(prepared.execution_document.host_path, "utf8");
    expect(doc).toContain("mounted at `/workspace` read-write");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

// --- H. Ownership/source pins ------------------------------------------------

const RUNTIME_SOURCE = await readFile(new URL("../src/pipeline_v2_docker_runtime.ts", import.meta.url), "utf8");
const ORCHESTRATION_SOURCE = await readFile(new URL("../src/pipeline_v2_orchestration.ts", import.meta.url), "utf8");
const SCHEMA_SOURCE = await readFile(new URL("../src/pipeline_v2.ts", import.meta.url), "utf8");
const RUNTIME_DATA_SOURCE = await readFile(new URL("../src/pipeline_v2_runtime.ts", import.meta.url), "utf8");
const DEFAULT_BUNDLE = await readFile(
  new URL("../../pipelines/default-v2/pipeline.yaml", import.meta.url),
  "utf8",
);

test("H1. one vocabulary owner, one resolver implementation, one compatible re-export, one production call site", () => {
  expect((SCHEMA_SOURCE.match(/PIPELINE_V2_WORKSPACE_ACCESS_VALUES/g) ?? []).length).toBeGreaterThanOrEqual(1);
  expect((SCHEMA_SOURCE.match(/export const PIPELINE_V2_WORKSPACE_ACCESS_VALUES/g) ?? []).length).toBe(1);
  // the single implementation owner lives next to the planner that consumes it
  expect((SCHEMA_SOURCE.match(/export function compiledWorkspaceAccessFor/g) ?? []).length).toBe(1);
  // the orchestration module keeps the compatible re-export, not a copy
  expect((ORCHESTRATION_SOURCE.match(/export \{ compiledWorkspaceAccessFor/g) ?? []).length).toBe(1);
  expect((ORCHESTRATION_SOURCE.match(/export function compiledWorkspaceAccessFor/g) ?? []).length).toBe(0);
  expect((RUNTIME_SOURCE.match(/compiledWorkspaceAccessFor\(/g) ?? []).length).toBe(1);
  expect((RUNTIME_DATA_SOURCE.match(/compiledWorkspaceAccessFor\(/g) ?? []).length).toBe(1);
});

test("H2. the policy is resolved only from compiled metadata, never from env/CLI/profile/state", () => {
  // every `workspace_access` mention in the runtime adapter is documentation
  for (const line of RUNTIME_SOURCE.split("\n").filter((candidate) => candidate.includes("workspace_access"))) {
    const trimmed = line.trim();
    expect(
      trimmed.startsWith("*") || trimmed.startsWith("//") || trimmed.startsWith("/*"),
    ).toBe(true);
  }
  expect(RUNTIME_SOURCE.includes("compiledWorkspaceAccessFor")).toBe(true);
  expect(RUNTIME_DATA_SOURCE.includes("compiledWorkspaceAccessFor")).toBe(true);
  expect(SCHEMA_SOURCE.match(/workspace_access/g)?.length).toBeGreaterThanOrEqual(1);
  // the single default site: exactly one `workspace_access ?? "read_write"`
  expect((SCHEMA_SOURCE.match(/workspace_access \?\? "read_write"/g) ?? []).length).toBe(1);
  expect((ORCHESTRATION_SOURCE.match(/workspace_access \?\? "read_write"/g) ?? []).length).toBe(0);
  expect((RUNTIME_SOURCE.match(/workspace_access \?\? "read_write"/g) ?? []).length).toBe(0);
  expect((RUNTIME_DATA_SOURCE.match(/workspace_access \?\? "read_write"/g) ?? []).length).toBe(0);
  // the default bundle does not declare the policy: its semantics are unchanged
  expect(DEFAULT_BUNDLE.includes("workspace_access")).toBe(false);
});

test("H3. the vocabulary is exactly the closed two-value set", () => {
  expect([...PIPELINE_V2_WORKSPACE_ACCESS_VALUES]).toEqual(["read_write", "read_only"]);
});

// --- L. Logical activation layout coherence ----------------------------------

async function planFor(
  root: string,
  yaml: string,
  stateId: string,
  model: string | null = null,
): Promise<ActivationLayoutPlan> {
  const pipeline = await loadFrom(root, yaml, model);
  return planActivationLayout(pipeline, stateId);
}

test("L1. a no-field stage agent plans the project read-write", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const plan = await planFor(root, stageRoleYaml(null), "dev");
    expect(plan.project).toEqual({ target: "/workspace", read_only: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("L2. an explicitly read-write stage agent plans the project read-write", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const plan = await planFor(root, stageRoleYaml("read_write"), "dev");
    expect(plan.project).toEqual({ target: "/workspace", read_only: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("L3. an explicitly read-only stage agent plans the project read-only", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const plan = await planFor(root, stageRoleYaml("read_only"), "dev");
    expect(plan.project).toEqual({ target: "/workspace", read_only: true });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("L4. a planning agent plans the project read-write", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const planningYaml = `
schema_version: 2
entry_state: dev
max_transitions: 20

inputs:
  - id: task
    type: file
    protected: true

outputs: []

orchestration:
  stage_templates: []
  execution_roles:
    - state_id: dev
      role: planning
      plan_output: feedback
      stage_wait:
        reason: review_wait
        actions:
          - continue_stage

states:
  - id: dev
    type: agent
    profile: coder
    prompt: prompts/dev.md
    inputs:
      - id: task
        source:
          pipeline_input: task
    outputs:
      - id: feedback
        type: json
        schema: schemas/feedback.schema.json
    timeout_seconds: 30
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;
    const plan = await planFor(root, planningYaml, "dev");
    expect(plan.project).toEqual({ target: "/workspace", read_only: false });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("L5. the layout and the effective policy agree for one trusted pipeline in every case", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    for (const yaml of [stageRoleYaml(null), stageRoleYaml("read_write"), stageRoleYaml("read_only")]) {
      const pipeline = await loadFrom(root, yaml);
      const plan = planActivationLayout(pipeline, "dev");
      expect(plan.project.read_only).toBe(compiledWorkspaceAccessFor(pipeline, "dev") === "read_only");
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("L6. forged pipelines are still rejected by the provenance gate before any layout is built", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const trusted = await loadFrom(root, stageRoleYaml("read_only"));
    let cloned = "";
    try {
      planActivationLayout(structuredClone(trusted) as unknown as ResolvedPipelineV2, "dev");
    } catch (cause) {
      cloned = (cause as Error).message;
    }
    expect(cloned.length > 0).toBe(true);
    let spread = "";
    try {
      planActivationLayout({ ...trusted } as unknown as ResolvedPipelineV2, "dev");
    } catch (cause) {
      spread = (cause as Error).message;
    }
    expect(spread.length > 0).toBe(true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("L8. the default-v2 bundle keeps the established read-write layout for every stage state", async () => {
  const { loadPipelineV2: loadDefault } = await import("../src/pipeline_v2.ts");
  const defaultPipeline = await loadDefault(
    new URL("../../pipelines/default-v2", import.meta.url).pathname,
  );
  for (const state of defaultPipeline.states) {
    if (state.type !== "agent") {
      continue;
    }
    const plan = planActivationLayout(defaultPipeline, state.id);
    expect(plan.project.read_only).toBe(false);
    expect(plan.project.target).toBe("/workspace");
  }
});

test("L9. a read-only stage layout keeps every declared output port writable", async () => {
  const root = await mkdtemp(join(tmpdir(), "ws-access-"));
  try {
    const plan = await planFor(root, stageRoleYaml("read_only"), "dev");
    expect(plan.project.read_only).toBe(true);
    expect(plan.output_ports.length).toBeGreaterThan(0);
    for (const port of plan.output_ports) {
      expect(port.read_only).toBe(false);
      expect(port.target.startsWith("/pipeline/outputs/")).toBe(true);
    }
    for (const port of plan.input_ports) {
      expect(port.read_only).toBe(true);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
