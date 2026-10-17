import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync, chmodSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as net from "node:net";

import {
  ControlPlaneProjectRegistryError,
  loadControlPlaneProjectRegistry,
  resolveControlPlaneRunCommand,
  type ControlPlaneRegistryFailureReason,
  type ControlPlaneRunRequest,
} from "../src/control_plane/project_registry.ts";
import { OrchestratorSubprocessClient } from "../src/control_plane/subprocess_client.ts";
import { routeOrchestratorCommand } from "../src/control_plane/routing.ts";

function registryDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    projects: [
      {
        id: "project-a",
        project_root: "/srv/projects/project-a",
        config_root: "/etc/orchestrator/project-a",
        pipeline_root: null,
        launcher_id: null,
        input_roots: [{ id: "tasks", root: "/var/lib/inputs/project-a/tasks" }],
      },
    ],
    ...overrides,
  };
}

function expectRegistryError(cause: unknown, reason: ControlPlaneRegistryFailureReason): void {
  expect(cause instanceof ControlPlaneProjectRegistryError).toBe(true);
  expect((cause as ControlPlaneProjectRegistryError).reason).toBe(reason);
}

async function expectLoadError(path: string, reason: ControlPlaneRegistryFailureReason): Promise<void> {
  try {
    await loadControlPlaneProjectRegistry(path);
    throw new Error(`expected load failure ${reason}`);
  } catch (cause) {
    expectRegistryError(cause, reason);
  }
}

async function expectResolveError(registry: Awaited<ReturnType<typeof loadControlPlaneProjectRegistry>>, request: unknown, reason: ControlPlaneRegistryFailureReason): Promise<void> {
  try {
    await resolveControlPlaneRunCommand(registry, request as ControlPlaneRunRequest);
    throw new Error(`expected resolve failure ${reason}`);
  } catch (cause) {
    expectRegistryError(cause, reason);
  }
}

interface TreeEntry {
  path: string;
  kind: string;
  size: number;
}

function fingerprint(root: string): TreeEntry[] {
  const entries: TreeEntry[] = [];
  const walk = (absolute: string, relative: string): void => {
    const info = statSync(absolute);
    const kind = info.isSymbolicLink() ? "symlink" : info.isDirectory() ? "directory" : info.isFile() ? "file" : "other";
    entries.push({ path: relative, kind, size: kind === "file" ? info.size : 0 });
    if (kind === "directory") {
      for (const child of readdirSorted(absolute)) {
        walk(join(absolute, child), relative === "" ? child : `${relative}/${child}`);
      }
    }
  };
  walk(root, "");
  return entries;
}

function readdirSorted(absolute: string): string[] {
  const { readdirSync } = require("node:fs") as typeof import("node:fs");
  return readdirSync(absolute).sort();
}

async function makeFifo(path: string): Promise<void> {
  const child = spawn("mkfifo", [path]);
  await new Promise<void>((resolve, reject) => {
    child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
  });
}

function makeSocket(path: string): Promise<void> {
  return new Promise<void>((resolve) => {
    const server = net.createServer();
    server.listen(path, () => {
      server.close(() => resolve());
    });
  });
}

describe("control plane project registry: document", () => {
  const root = mkdtempSync(join(tmpdir(), "cp-reg-doc-"));
  // Positive loads canonicalize operator roots, so the valid documents
  // point at real directories created here; shape failures fail before
  // canonicalization and may keep the nonexistent example spellings.
  const realProject = join(root, "real-project");
  const realConfig = join(root, "real-config");
  const realPipeline = join(root, "real-pipeline");
  const realInputRoot = join(root, "real-inputs");
  const realInputRootB = join(root, "real-inputs-b");

  function registryFile(name: string, document: unknown): string {
    const path = join(root, `${name}.json`);
    writeFileSync(path, JSON.stringify(document), { mode: 0o600 });
    return path;
  }

  beforeAll(() => {
    mkdirSync(realProject, { recursive: true });
    mkdirSync(realConfig, { recursive: true });
    mkdirSync(realPipeline, { recursive: true });
    mkdirSync(realInputRoot, { recursive: true });
    mkdirSync(realInputRootB, { recursive: true });
  });

  afterAll(() => {
    rmSync(root, { recursive: true, force: true });
  });

  test("a minimal valid registry loads with canonical declaration order", async () => {
    const registry = await loadControlPlaneProjectRegistry(registryFile("valid", registryDocument({
      projects: [{
        id: "project-a",
        project_root: realProject,
        config_root: realConfig,
        pipeline_root: null,
        launcher_id: null,
        input_roots: [{ id: "tasks", root: realInputRoot }],
      }],
    })));
    expect(registry.schemaVersion).toBe(1);
    expect(registry.projects).toHaveLength(1);
    expect(Object.isFrozen(registry)).toBe(true);
    expect(Object.isFrozen(registry.projects)).toBe(true);
    expect(Object.isFrozen(registry.projects[0])).toBe(true);
  });

  test("multiple projects and input roots keep declaration order", async () => {
    const document = registryDocument({
      projects: [
        {
          id: "zeta",
          project_root: realProject,
          config_root: realConfig,
          pipeline_root: realPipeline,
          launcher_id: "dhl_zeta",
          input_roots: [
            { id: "second", root: realInputRoot },
            { id: "first", root: realInputRootB },
          ],
        },
        {
          id: "alpha",
          project_root: realProject,
          config_root: realConfig,
          pipeline_root: null,
          launcher_id: null,
          input_roots: [],
        },
      ],
    });
    const registry = await loadControlPlaneProjectRegistry(registryFile("order", document));
    expect(registry.projects.map((project) => project.id)).toEqual(["zeta", "alpha"]);
    expect(registry.projects[0]?.inputRoots.map((inputRoot) => inputRoot.id)).toEqual(["second", "first"]);
    expect(registry.projects[0]?.pipelineRoot).toBe(realPipeline);
    expect(registry.projects[0]?.launcherId).toBe("dhl_zeta");
    expect(registry.projects[1]?.pipelineRoot).toBeNull();
    expect(registry.projects[1]?.launcherId).toBeNull();
  });

  test("unknown, missing and inherited keys are rejected at every level", async () => {
    await expectLoadError(registryFile("extra-root", { ...registryDocument(), extra: 1 }), "invalid_registry");
    await expectLoadError(registryFile("missing-root", { schema_version: 1 }), "invalid_registry");
    const protoDocument = JSON.parse('{"schema_version":1,"__proto__":{"hostile":true},"projects":[]}') as Record<string, unknown>;
    await expectLoadError(registryFile("proto-root", protoDocument), "invalid_registry");
    await expectLoadError(
      registryFile("extra-project", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: null, input_roots: [], extra: 1 }] })),
      "invalid_registry",
    );
    await expectLoadError(
      registryFile("missing-project", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, input_roots: [] }] })),
      "invalid_registry",
    );
    await expectLoadError(
      registryFile("extra-input-root", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: null, input_roots: [{ id: "r", root: "/v", extra: 1 }] }] })),
      "invalid_registry",
    );
    await expectLoadError(
      registryFile("missing-input-root", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: null, input_roots: [{ id: "r" }] }] })),
      "invalid_registry",
    );
  });

  test("primitive, array and null documents are rejected", async () => {
    await expectLoadError(registryFile("array-doc", [1]), "invalid_registry");
    await expectLoadError(registryFile("primitive-doc", 42), "invalid_registry");
    await expectLoadError(registryFile("null-doc", null), "invalid_registry");
    await expectLoadError(registryFile("project-array", registryDocument({ projects: "nope" })), "invalid_registry");
    await expectLoadError(registryFile("project-primitive", registryDocument({ projects: [7] })), "invalid_registry");
    await expectLoadError(registryFile("roots-primitive", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: null, input_roots: "x" }] })), "invalid_registry");
    await expectLoadError(registryFile("version-string", registryDocument({ schema_version: "1" })), "unsupported_registry_version");
  });

  test("duplicate project ids, input-root ids and canonical roots are rejected", async () => {
    const duplicateProject = registryDocument({
      projects: [
        { id: "dup", project_root: "/srv/a", config_root: "/etc/a", pipeline_root: null, launcher_id: null, input_roots: [] },
        { id: "dup", project_root: "/srv/b", config_root: "/etc/b", pipeline_root: null, launcher_id: null, input_roots: [] },
      ],
    });
    await expectLoadError(registryFile("dup-project", duplicateProject), "invalid_registry");
    const duplicateRootId = registryDocument({
      projects: [{ id: "p", project_root: "/srv/a", config_root: "/etc/a", pipeline_root: null, launcher_id: null, input_roots: [{ id: "r", root: "/v/one" }, { id: "r", root: "/v/two" }] }],
    });
    await expectLoadError(registryFile("dup-root-id", duplicateRootId), "invalid_registry");
  });

  test("unsafe ids and wrong scalar types are rejected", async () => {
    await expectLoadError(registryFile("bad-project-id", registryDocument({ projects: [{ id: "bad/id", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: null, input_roots: [] }] })), "invalid_registry");
    await expectLoadError(registryFile("bad-root-id", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: null, input_roots: [{ id: "..", root: "/v" }] }] })), "invalid_registry");
    await expectLoadError(registryFile("bad-launcher", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: "nope", input_roots: [] }] })), "invalid_registry");
    await expectLoadError(registryFile("bare-dhl", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, launcher_id: "dhl_", input_roots: [] }] })), "invalid_registry");
    await expectLoadError(registryFile("relative-project-root", registryDocument({ projects: [{ id: "p", project_root: "relative", config_root: "/c", pipeline_root: null, launcher_id: null, input_roots: [] }] })), "invalid_registry");
    await expectLoadError(registryFile("pipeline-number", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: 5, launcher_id: null, input_roots: [] }] })), "invalid_registry");
    await expectLoadError(registryFile("missing-pipeline-key", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", launcher_id: null, input_roots: [] }] })), "invalid_registry");
    await expectLoadError(registryFile("missing-launcher-key", registryDocument({ projects: [{ id: "p", project_root: "/p", config_root: "/c", pipeline_root: null, input_roots: [] }] })), "invalid_registry");
  });

  test("empty projects, malformed JSON and unsupported versions fail closed", async () => {
    await expectLoadError(registryFile("empty-projects", registryDocument({ projects: [] })), "invalid_registry");
    const malformed = join(root, "malformed.json");
    writeFileSync(malformed, '{"schema_version":1,"projects":[', { mode: 0o600 });
    await expectLoadError(malformed, "invalid_registry");
    await expectLoadError(registryFile("version-2", registryDocument({ schema_version: 2 })), "unsupported_registry_version");
  });

  test("the file is bounded and path shapes are typed", async () => {
    const oversized = join(root, "oversized.json");
    writeFileSync(oversized, `{"pad":"${"x".repeat(2 * 1024 * 1024)}"}`, { mode: 0o600 });
    await expectLoadError(oversized, "registry_too_large");
    await expectLoadError(join(root, "definitely-missing.json"), "registry_missing");
    await expectLoadError(root, "invalid_registry_path");
    const symlinked = join(root, "symlink.json");
    symlinkSync(join(root, "valid.json"), symlinked);
    await expectLoadError(symlinked, "invalid_registry_path");
    const fifo = join(root, "fifo.json");
    await makeFifo(fifo);
    await expectLoadError(fifo, "invalid_registry_path");
    await expectLoadError("relative.json", "invalid_registry_path");
    await expectLoadError("", "invalid_registry_path");
  });

  test("an unreadable registry file is a typed failure", async () => {
    const unreadable = join(root, "unreadable.json");
    writeFileSync(unreadable, JSON.stringify(registryDocument()), { mode: 0o600 });
    chmodSync(unreadable, 0o000);
    try {
      await expectLoadError(unreadable, "registry_unreadable");
    } finally {
      chmodSync(unreadable, 0o600);
    }
  });

  test("diagnostics stay content-free", async () => {
    const malformed = join(root, "canary.json");
    writeFileSync(malformed, "SECRET-CANARY-BODY", { mode: 0o600 });
    try {
      await loadControlPlaneProjectRegistry(malformed);
      throw new Error("expected failure");
    } catch (cause) {
      expect((cause as Error).message).not.toContain("SECRET-CANARY-BODY");
    }
    try {
      await loadControlPlaneProjectRegistry(join(root, "nosuch.json"));
      throw new Error("expected failure");
    } catch (cause) {
      expect((cause as Error).message).not.toContain("nosuch");
    }
  });
});

describe("control plane project registry: roots", () => {
  const root = mkdtempSync(join(tmpdir(), "cp-reg-roots-"));
  const realDir = join(root, "real-dir");
  const nestedReal = join(root, "nested", "real");
  let linkedDirTarget = "";

  function registryFile(name: string, document: unknown): string {
    const path = join(root, `${name}.json`);
    writeFileSync(path, JSON.stringify(document), { mode: 0o600 });
    return path;
  }

  afterAll(() => {
    try {
      rmSync(linkedDirTarget, { force: true });
    } catch {
      // best effort
    }
    rmSync(root, { recursive: true, force: true });
  });

  test("a missing project, config, pipeline or input root fails at load", async () => {
    mkdirSync(join(root, "exists"), { recursive: true });
    const base = { id: "p", config_root: join(root, "exists"), pipeline_root: null, launcher_id: null, input_roots: [] };
    await expectLoadError(registryFile("missing-project-root", registryDocument({ projects: [{ ...base, project_root: join(root, "nope") }] })), "invalid_registry");
    await expectLoadError(registryFile("missing-config-root", registryDocument({ projects: [{ ...base, project_root: join(root, "exists"), config_root: join(root, "nope") }] })), "invalid_registry");
    await expectLoadError(registryFile("missing-pipeline-root", registryDocument({ projects: [{ ...base, project_root: join(root, "exists"), pipeline_root: join(root, "nope") }] })), "invalid_registry");
    await expectLoadError(registryFile("missing-input-root", registryDocument({ projects: [{ ...base, project_root: join(root, "exists"), input_roots: [{ id: "r", root: join(root, "nope") }] }] })), "invalid_registry");
  });

  test("a file instead of a directory and a leaf symlink root fail at load", async () => {
    const filePath = join(root, "plain-file");
    writeFileSync(filePath, "x", { mode: 0o600 });
    const base = { id: "p", pipeline_root: null, launcher_id: null, input_roots: [] };
    await expectLoadError(registryFile("file-as-project", registryDocument({ projects: [{ ...base, project_root: filePath, config_root: filePath }] })), "invalid_registry");
    const linkPath = join(root, "link-dir");
    symlinkSync(realDir, linkPath);
    await expectLoadError(registryFile("leaf-symlink-root", registryDocument({ projects: [{ ...base, project_root: linkPath, config_root: linkPath }] })), "invalid_registry");
  });

  test("configured spelling with a symlinked ancestor canonicalizes", async () => {
    mkdirSync(join(root, "nested"), { recursive: true });
    mkdirSync(nestedReal, { recursive: true });
    const linkParent = join(root, "alias");
    symlinkSync(join(root, "nested"), linkParent);
    const document = registryDocument({
      projects: [{ id: "p", project_root: join(linkParent, "real"), config_root: join(linkParent, "real"), pipeline_root: null, launcher_id: null, input_roots: [] }],
    });
    const registry = await loadControlPlaneProjectRegistry(registryFile("canonicalized", document));
    const expected = await import("node:fs/promises").then((fs) => fs.realpath(nestedReal));
    expect(registry.projects[0]?.projectRoot).toBe(expected);
    expect(registry.projects[0]?.projectRoot.includes("alias")).toBe(false);
  });

  test("duplicate canonical input roots within one project are rejected", async () => {
    mkdirSync(join(root, "shared"), { recursive: true });
    const linkA = join(root, "alias-a");
    symlinkSync(join(root, "shared"), linkA);
    const document = registryDocument({
      projects: [{
        id: "p",
        project_root: join(root, "shared"),
        config_root: join(root, "shared"),
        pipeline_root: null,
        launcher_id: null,
        input_roots: [
          { id: "one", root: join(root, "shared") },
          { id: "two", root: linkA },
        ],
      }],
    });
    await expectLoadError(registryFile("dup-canonical", document), "invalid_registry");
    void linkedDirTarget;
  });

  test("registry errors never echo operator paths", async () => {
    const secretDir = join(root, "secret-root-name");
    mkdirSync(secretDir, { recursive: true });
    const document = registryDocument({
      projects: [{ id: "p", project_root: secretDir, config_root: join(root, "missing-config"), pipeline_root: null, launcher_id: null, input_roots: [] }],
    });
    try {
      await loadControlPlaneProjectRegistry(registryFile("no-echo", document));
      throw new Error("expected failure");
    } catch (cause) {
      expect((cause as Error).message).not.toContain("secret-root-name");
      expect((cause as Error).message).not.toContain(root);
    }
  });
});

describe("control plane project registry: requests and composition", () => {
  const root = mkdtempSync(join(tmpdir(), "cp-reg-req-"));
  const projectDir = join(root, "project");
  const configDir = join(root, "config");
  const pipelineDir = join(root, "pipeline");
  const inputRoot = join(root, "inputs", "tasks");
  let registry: Awaited<ReturnType<typeof loadControlPlaneProjectRegistry>>;

  function request(overrides: Record<string, unknown> = {}): ControlPlaneRunRequest {
    return {
      runId: "cp-req-1",
      projectId: "project-a",
      inputs: [{ id: "task", rootId: "tasks", ref: "task-file" }],
      ...overrides,
    } as ControlPlaneRunRequest;
  }

  afterAll(() => {
    rmSync(root, { recursive: true, force: true });
  });

  test("setup: a registry over real directories", async () => {
    mkdirSync(projectDir, { recursive: true });
    mkdirSync(configDir, { recursive: true });
    mkdirSync(pipelineDir, { recursive: true });
    mkdirSync(inputRoot, { recursive: true });
    writeFileSync(join(inputRoot, "task-file"), "TASK-BODY\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "second-file"), "SECOND\n", { mode: 0o600 });
    const document = registryDocument({
      projects: [{
        id: "project-a",
        project_root: projectDir,
        config_root: configDir,
        pipeline_root: pipelineDir,
        launcher_id: "dhl_smoke",
        input_roots: [{ id: "tasks", root: inputRoot }],
      }],
    });
    writeFileSync(join(root, "registry.json"), JSON.stringify(document), { mode: 0o600 });
    registry = await loadControlPlaneProjectRegistry(join(root, "registry.json"));
  });

  test("the composition returns the exact existing command and routing config", async () => {
    const resolved = await resolveControlPlaneRunCommand(registry, request());
    expect(resolved.command).toEqual({
      kind: "run",
      runId: "cp-req-1",
      projectPath: projectDir,
      inputs: [{ id: "task", path: join(inputRoot, "task-file") }],
      launcherId: "dhl_smoke",
    });
    expect(resolved.routingConfig).toEqual({ configRoot: configDir, pipelineRoot: pipelineDir });
  });

  test("the composed command routes through the real router with the exact argv", async () => {
    const resolved = await resolveControlPlaneRunCommand(registry, request());
    const routed = routeOrchestratorCommand(resolved.command, resolved.routingConfig);
    expect(routed.argv).toEqual([
      "run",
      "--run-id",
      "cp-req-1",
      "--pipeline-root",
      pipelineDir,
      "--config-root",
      configDir,
      "--project",
      projectDir,
      "--input",
      `task=${join(inputRoot, "task-file")}`,
      "--launcher-id",
      "dhl_smoke",
      "--json",
    ]);
  });

  test("a null pipeline root keeps the bundled default-v2 path", async () => {
    const document = registryDocument({
      projects: [{ id: "solo", project_root: projectDir, config_root: configDir, pipeline_root: null, launcher_id: null, input_roots: [{ id: "tasks", root: inputRoot }] }],
    });
    writeFileSync(join(root, "registry-null.json"), JSON.stringify(document), { mode: 0o600 });
    const nullRegistry = await loadControlPlaneProjectRegistry(join(root, "registry-null.json"));
    const resolved = await resolveControlPlaneRunCommand(nullRegistry, {
      runId: "cp-req-2",
      projectId: "solo",
      inputs: [{ id: "task", rootId: "tasks", ref: "task-file" }],
    });
    const routed = routeOrchestratorCommand(resolved.command, resolved.routingConfig);
    expect(routed.argv).not.toContain("--pipeline-root");
    expect(routed.argv).not.toContain("--launcher-id");
    expect(resolved.routingConfig.pipelineRoot).toBeUndefined();
  });

  test("two inputs resolve in declaration order to canonical files", async () => {
    const resolved = await resolveControlPlaneRunCommand(registry, request({
      inputs: [
        { id: "task", rootId: "tasks", ref: "second-file" },
        { id: "spec", rootId: "tasks", ref: "task-file" },
      ],
    }));
    expect((resolved.command as { inputs: readonly { id: string; path: string }[] }).inputs.map((input) => input.id)).toEqual(["task", "spec"]);
    expect((resolved.command as { inputs: readonly { id: string; path: string }[] }).inputs[0]?.path).toBe(join(inputRoot, "second-file"));
  });

  test("request failures are typed before any spawn", async () => {
    await expectResolveError(registry, request({ projectId: "unknown" }), "project_not_found");
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "elsewhere", ref: "task-file" }] }), "input_root_not_found");
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "task-file" }, { id: "task", rootId: "tasks", ref: "second-file" }] }), "invalid_request");
    await expectResolveError(registry, request({ inputs: [] }), "invalid_request");
    await expectResolveError(registry, request({ runId: "bad/id" }), "invalid_request");
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "sub/dir" }] }), "invalid_request");
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: ".." }] }), "invalid_request");
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "back\\slash" }] }), "invalid_request");
    await expectResolveError(registry, { ...request(), extra: "hostile" }, "invalid_request");
    await expectResolveError(registry, null, "invalid_request");
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "missing-file" }] }), "input_not_found");
    symlinkSync(join(inputRoot, "task-file"), join(inputRoot, "link-file"));
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "link-file" }] }), "input_invalid");
    mkdirSync(join(inputRoot, "adir"), { recursive: true });
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "adir" }] }), "input_invalid");
    rmSync(join(inputRoot, "adir"), { recursive: true, force: true });
    await makeFifo(join(inputRoot, "afifo"));
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "afifo" }] }), "input_invalid");
    const server = net.createServer();
    server.listen(join(inputRoot, "asocket"));
    await new Promise<void>((resolve) => server.on("listening", () => resolve()));
    try {
      await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "asocket" }] }), "input_invalid");
    } finally {
      server.close();
      await new Promise<void>((resolve) => server.on("close", () => resolve()));
    }
    rmSync(join(inputRoot, "link-file"), { force: true });
    rmSync(join(inputRoot, "afifo"), { force: true });
    rmSync(join(inputRoot, "asocket"), { force: true });
  });

  test("a hostile proxy request is read once per field and extras are never read", async () => {
    const reads: string[] = [];
    const target = request();
    const hostile = new Proxy(target as unknown as Record<string, unknown>, {
      get(target, property) {
        if (typeof property === "string") {
          reads.push(property);
          return target[property];
        }
        return undefined;
      },
      ownKeys() {
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor(target, property) {
        return Reflect.getOwnPropertyDescriptor(target, property);
      },
    });
    const resolved = await resolveControlPlaneRunCommand(registry, hostile as unknown as ControlPlaneRunRequest);
    expect((resolved.command as { runId: string }).runId).toBe("cp-req-1");
    for (const field of ["runId", "projectId", "inputs"]) {
      expect(reads.filter((read) => read === field)).toHaveLength(1);
    }
    expect(reads).not.toContain("extra");
  });

  test("caller mutation after the call cannot change the returned composition", async () => {
    const original = request() as ControlPlaneRunRequest & { inputs: Array<{ id: string; rootId: string; ref: string }>; runId: string };
    const resolved = await resolveControlPlaneRunCommand(registry, original);
    original.inputs.push({ id: "extra", rootId: "tasks", ref: "second-file" });
    original.runId = "mutated";
    expect((resolved.command as { runId: string }).runId).toBe("cp-req-1");
    expect((resolved.command as { inputs: readonly unknown[] }).inputs).toHaveLength(1);
  });

  test("the loaded registry survives a later deletion or corruption of its file", async () => {
    const document = registryDocument({
      projects: [{ id: "later", project_root: projectDir, config_root: configDir, pipeline_root: null, launcher_id: null, input_roots: [{ id: "tasks", root: inputRoot }] }],
    });
    const path = join(root, "registry-later.json");
    writeFileSync(path, JSON.stringify(document), { mode: 0o600 });
    const laterRegistry = await loadControlPlaneProjectRegistry(path);
    rmSync(path, { force: true });
    const resolved = await resolveControlPlaneRunCommand(laterRegistry, {
      runId: "cp-req-3",
      projectId: "later",
      inputs: [{ id: "task", rootId: "tasks", ref: "task-file" }],
    });
    expect((resolved.command as { runId: string }).runId).toBe("cp-req-3");
  });

  test("failures have zero effect: no spawn, no run root, unchanged tree", async () => {
    const stateProbe = join(root, "state-root-probe");
    mkdirSync(stateProbe, { recursive: true });
    const before = fingerprint(root);
    await expectResolveError(registry, request({ projectId: "unknown" }), "project_not_found");
    await expectResolveError(registry, request({ inputs: [{ id: "task", rootId: "tasks", ref: "missing" }] }), "input_not_found");
    try {
      await loadControlPlaneProjectRegistry(join(root, "absent-registry.json"));
    } catch {
      // expected
    }
    const after = fingerprint(root);
    expect(after).toEqual(before);
    expect(existsSync(join(stateProbe, "pipeline-runs"))).toBe(false);
    expect(existsSync(join(root, "pipeline-runs"))).toBe(false);
  });
});

describe("control plane project registry: source restrictions", () => {
  const source = readFileSync(join(import.meta.dir, "..", "src", "control_plane", "project_registry.ts"), "utf8");

  test("one JSON.parse owner, no spawn, no HTTP, no discovery or watchers", () => {
    expect(source.split("JSON.parse(").length - 1).toBe(1);
    expect(source).not.toContain("Bun.spawn");
    expect(source).not.toContain("Bun.serve");
    expect(source).not.toContain("fetch(");
    expect(source).not.toContain("readdir");
    expect(source).not.toContain("watch");
    expect(source).not.toContain("state.json");
    expect(source).not.toContain("shell");
    expect(source).not.toContain("exec(");
  });

  test("no orchestration-internal imports; the shared predicates and router are reused", () => {
    for (const forbidden of [
      'from "../pipeline_v2_state',
      'from "../pipeline_state_store',
      'from "../pipeline_v2_coordinator',
      'from "../pipeline_v2_runner',
      'from "../pipeline_runner',
      'from "../run_snapshot_store',
      'from "../pipeline_v2_docker_runtime',
    ]) {
      expect(source).not.toContain(forbidden);
    }
    expect(source).toContain('from "../pipeline_v2_scalar.ts"');
    expect(source).toContain("routeOrchestratorCommand");
    expect(source).not.toContain('"run_already_exists"');
    expect(source).not.toContain('"worker_failed"');
    expect(source).not.toMatch(/const\s+\w*REASONS\w*\s*=\s*\[/);
  });

  test("input contents are never read: exactly one file read site (the registry document)", () => {
    expect(source.split("readSync(").length - 1).toBe(1);
    expect(source).not.toContain("readFileSync");
  });
});

describe("control plane project registry: real composition smoke", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), "cp-reg-smoke-"));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const configRoot = join(import.meta.dir, "..", "fixtures", "control-plane", "config");
  const pipelineRoot = join(import.meta.dir, "..", "fixtures", "control-plane", "bundle");
  const inputRoot = join(smokeRoot, "inputs");
  const taskFile = join(inputRoot, "task.md");
  const registryPath = join(smokeRoot, "registry.json");
  let client: OrchestratorSubprocessClient;
  let resolvedCommand: Awaited<ReturnType<typeof resolveControlPlaneRunCommand>>["command"];
  let daemonPid = 0;
  const runId = `cprs-${Math.random().toString(36).slice(2, 8)}`;

  const start = async (): Promise<void> => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "smoke project\n", { mode: 0o600 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(taskFile, "SMOKE-TASK\n", { mode: 0o600 });
    const document = {
      schema_version: 1,
      projects: [
        {
          id: "smoke-project",
          project_root: projectDir,
          config_root: configRoot,
          pipeline_root: pipelineRoot,
          launcher_id: null,
          input_roots: [{ id: "tasks", root: inputRoot }],
        },
      ],
    };
    writeFileSync(registryPath, JSON.stringify(document), { mode: 0o600 });
    const daemonLog = openSync(join(smokeRoot, "daemon.log"), "a");
    const daemon = spawn(process.execPath, [join(import.meta.dir, "control_plane_daemon.ts"), socketPath], {
      stdio: ["ignore", daemonLog, daemonLog],
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("smoke daemon socket never appeared");
    const registry = await loadControlPlaneProjectRegistry(registryPath);
    const resolved = await resolveControlPlaneRunCommand(registry, {
      runId,
      projectId: "smoke-project",
      inputs: [{ id: "task", rootId: "tasks", ref: "task.md" }],
    });
    resolvedCommand = resolved.command;
    client = new OrchestratorSubprocessClient({
      bunExecutable: process.execPath,
      orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
      workingDirectory: smokeRoot,
      configRoot: resolved.routingConfig.configRoot,
      pipelineRoot: resolved.routingConfig.pipelineRoot,
      baseEnv: {
        PATH: "/usr/bin:/bin",
        HOME: smokeRoot,
        XDG_CONFIG_HOME: credDir,
        ORCHESTRATOR_STATE_ROOT: stateRoot,
        DOCKER_HELPER_SOCKET_PATH: socketPath,
        ARCHITECT_SOURCE_VAR_1: "smoke-a",
        CODER_SOURCE_VAR_1: "smoke-c",
      },
      stdoutLimitBytes: 4_000_000,
      stderrLimitBytes: 4_000_000,
      sigintGraceMs: 3000,
    });
  };

  afterAll(() => {
    if (daemonPid !== 0) {
      try {
        process.kill(daemonPid);
      } catch {
        // already gone
      }
    }
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  test(
    "registry -> resolve -> existing client -> real CLI -> planReady -> inspect",
    async () => {
      await start();
      const result = await client.run(resolvedCommand, {});
      expect(result.ok).toBe(true);
      if (!result.ok) throw new Error("unreachable");
      const projection = result.projection as { kind: string; runId: string; runRoot: string };
      expect(projection.kind).toBe("planReady");
      expect(projection.runId).toBe(runId);
      expect(JSON.stringify(result.projection)).not.toContain('"state"');
      const inspectResult = await client.run({ kind: "inspect", runId });
      expect(inspectResult.ok).toBe(true);
      if (!inspectResult.ok) throw new Error("unreachable");
      const inspect = inspectResult.projection as unknown as { kind: string; schemaVersion: number; status: string; boundary: { kind: string } };
      expect(inspect.kind).toBe("inspect");
      expect(inspect.schemaVersion).toBe(1);
      expect(inspect.status).toBe("running");
      expect(inspect.boundary.kind).toBe("plan_ready");
    },
    90000,
  );
});
