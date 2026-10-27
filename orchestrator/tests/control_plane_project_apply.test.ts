/**
 * Permanent end-to-end suite of the explicit Project Apply API over the
 * REAL production stack: the loopback HTTP service (with the trusted
 * server-owned apply-store root), the project registry with the one
 * per-project Apply capability, the durable run binding store, the real
 * orchestrator CLI subprocess (project-apply / project-apply-inspect)
 * and the fake Docker Helper daemon whose stage workers write into the
 * shared run project workspace.
 *
 * Matrix: the capability gate (absent capability and the missing
 * apply-store root refuse with zero effects), the happy path (terminal
 * run -> GET changes -> POST apply -> exact result tree), the idempotent
 * repeat, identity conflicts, the read-only GET projection of durable
 * statuses (built through the real journal), the recovery roll-forward
 * through POST only, concurrent same-identity and cross-identity POSTs,
 * the cooperative project lock against the read APIs, tampered packages,
 * a read-only project, a failed terminal run, zero-write fingerprints
 * and the leakage scan.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, openSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync, unlinkSync } from "node:fs";
import { rm } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseControlPlaneRoute, startControlPlaneHttpService, ControlPlaneHttpRequestError, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";
import { preparePipelineV2ProjectApplyCore } from "../src/pipeline_v2_project_apply_internal.ts";
import { applyPreparedProjectCore } from "../src/pipeline_v2_project_apply_writer_internal.ts";
import { realImmutableDocumentIo } from "../src/pipeline_v2_immutable_document_store_internal.ts";
import { defaultPipelineStateIo } from "../src/run_snapshot_store.ts";
import { realProjectWorkspaceVerifyIo } from "../src/pipeline_v2_project_provenance_store_internal.ts";
import { realProjectFileStreamIo } from "../src/pipeline_v2_project_stream_internal.ts";
import { realMetadataProbeIo } from "../src/pipeline_v2_apply_metadata_probe_internal.ts";
import { realApplyFdOps } from "../src/pipeline_v2_apply_fd_internal.ts";
import { acquirePipelineV2ApplyProjectLock } from "../src/pipeline_v2_apply_project_lock_internal.ts";
import { buildFinalizedApplyRun, disposeApplyRoots, faultDocIo, faultFdOps, faultStreamIo } from "./apply_fixtures.ts";
import { linkSync } from "node:fs";

const CANARY = "applyhostpath";

interface RawResponse {
  readonly status: number;
  readonly body: string;
}

function rawRequest(port: number, method: string, path: string, body?: string): Promise<RawResponse> {
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      { host: "127.0.0.1", port, method, path, headers: { host: `127.0.0.1:${port}`, ...(body !== undefined ? { "content-type": "application/json" } : {}) } },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => chunks.push(chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks).toString("utf8") }));
      },
    );
    req.on("error", reject);
    if (body !== undefined) {
      req.write(body);
    }
    req.end();
  });
}

interface TreeRow {
  readonly path: string;
  readonly kind: string;
  readonly mode: number;
  readonly bytes: string;
}

function treeRows(root: string): TreeRow[] {
  const rows: TreeRow[] = [];
  const walk = (dir: string, prefix: string): void => {
    for (const entry of readdirSync(dir).sort()) {
      const full = join(dir, entry);
      const rel = prefix === "" ? entry : `${prefix}/${entry}`;
      const info = statSync(full);
      if (info.isDirectory()) {
        rows.push({ path: rel, kind: "directory", mode: info.mode & 0o7777, bytes: "" });
        walk(full, rel);
      } else {
        rows.push({ path: rel, kind: "file", mode: info.mode & 0o7777, bytes: readFileSync(full).toString("utf8") });
      }
    }
  };
  walk(root, "");
  return rows;
}

function fingerprintTree(root: string): string {
  const rows: string[] = [];
  const walk = (dir: string, prefix: string): void => {
    for (const entry of readdirSync(dir).sort()) {
      const full = join(dir, entry);
      const rel = prefix === "" ? entry : `${prefix}/${entry}`;
      const info = statSync(full);
      rows.push(`${rel}|${info.isDirectory() ? "d" : "f"}|${info.mode & 0o7777}|${info.isDirectory() ? "" : readFileSync(full).length}`);
      if (info.isDirectory()) {
        walk(full, rel);
      }
    }
  };
  walk(root, "");
  return rows.join("\n");
}

function applyStoreFingerprint(root: string): string {
  const rows: string[] = [];
  const walk = (dir: string, prefix: string): void => {
    for (const entry of readdirSync(dir).sort()) {
      const full = join(dir, entry);
      const rel = prefix === "" ? entry : `${prefix}/${entry}`;
      const info = statSync(full);
      rows.push(`${rel}|${info.isDirectory() ? "d" : "f"}|${info.mode & 0o7777}|${info.isDirectory() ? 0 : readFileSync(full).length}|${info.ino}`);
      if (info.isDirectory()) {
        walk(full, rel);
      }
    }
  };
  if (existsSync(root)) {
    walk(root, "");
  }
  return rows.join("\n");
}

const wait = (ms = 30): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

describe("control-plane explicit Project Apply over the real stack", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `apply-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const applyStoreRoot = join(smokeRoot, "applies");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: RunningControlPlaneHttpService;
  let port = 0;
  let daemonPid = 0;
  const suffix = Math.random().toString(36).slice(2, 8);
  const runId = `apply-a-${suffix}`;
  const failRunId = `apply-b-${suffix}`;
  const noCapabilityRunId = `apply-c-${suffix}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, projectDir, applyStoreRoot, "smoke-credential-token", CANARY];
  let observation = "";
  const production = {
    docIo: realImmutableDocumentIo,
    stateIo: defaultPipelineStateIo,
    verifyIo: realProjectWorkspaceVerifyIo,
    fileStreamIo: realProjectFileStreamIo,
    probeIo: realMetadataProbeIo,
  };
  const productionWriter = {
    docIo: realImmutableDocumentIo,
    stateIo: defaultPipelineStateIo,
    fileStreamIo: realProjectFileStreamIo,
    fdOps: realApplyFdOps,
  };

  /** Restores the registered project to the reviewed baseline state. */
  const resetOperatorToBaseline = (): void => {
    for (const name of ["dev-notes.txt", "report-copy.txt"]) {
      try {
        unlinkSync(join(projectDir, name));
      } catch {
        // already absent
      }
    }
    for (const entry of readdirSync(projectDir)) {
      if (entry.startsWith(".apply-tmp-")) {
        rmSync(join(projectDir, entry), { recursive: true, force: true });
      }
    }
    writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
  };

  const startService = async (withApplyStore: boolean): Promise<void> => {
    service = await startControlPlaneHttpService({
      registryPath,
      bindingStoreRoot: storeRoot,
      client: {
        bunExecutable: process.execPath,
        orchestratorEntrypoint: join(import.meta.dir, "..", "src", "main.ts"),
        workingDirectory: smokeRoot,
        baseEnv: {
          PATH: "/usr/bin:/bin",
          HOME: smokeRoot,
          XDG_CONFIG_HOME: credDir,
          ORCHESTRATOR_STATE_ROOT: stateRoot,
          DOCKER_HELPER_SOCKET_PATH: socketPath,
          ARCHITECT_SOURCE_VAR_1: "flow-a",
          CODER_SOURCE_VAR_1: "flow-c",
        },
        stdoutLimitBytes: 4_000_000,
        stderrLimitBytes: 4_000_000,
        sigintGraceMs: 3000,
      },
      port: 0,
      bodyLimitBytes: 4096,
      ...(withApplyStore ? { applyStoreRoot } : {}),
    });
    port = service.port;
  };

  /** The full manual lifecycle of one run through the real stack. */
  const driveRun = async (id: string, taskRef: string, reviseRef: string): Promise<"succeeded" | "failed"> => {
    const created = await rawRequest(port, "POST", `/v1/projects/project-a/runs`, JSON.stringify({
      run_id: id,
      inputs: [{ id: "task", root_id: "tasks", ref: taskRef }],
    }));
    expect(created.status).toBe(200);
    expect((JSON.parse(created.body) as { outcome: string }).outcome).toBe("plan_ready");
    let stage = "stage-1";
    for (let round = 0; round < 10; round++) {
      const inspectRaw = await rawRequest(port, "GET", `/v1/runs/${id}`);
      expect(inspectRaw.status).toBe(200);
      const model = JSON.parse(inspectRaw.body) as { status: string; boundary: Record<string, unknown> };
      const boundaryKind = model.boundary.kind as string;
      if (model.status === "succeeded" || model.status === "failed") {
        return model.status as "succeeded" | "failed";
      }
      if (boundaryKind === "plan_ready") {
        const mutation = await rawRequest(port, "POST", `/v1/runs/${id}/resume-plan`, JSON.stringify({ stage_id: stage, initial_budget: 1 }));
        expect(mutation.status).toBe(200);
        stage = "stage-2";
      } else if (boundaryKind === "waiting") {
        const waitIndex = (model.boundary as { waitIndex: number }).waitIndex;
        if (waitIndex === 1) {
          const mutation = await rawRequest(port, "POST", `/v1/runs/${id}/continue-stage`, JSON.stringify({ wait_index: 1, additional_iterations: 1 }));
          expect(mutation.status).toBe(200);
        } else {
          const mutation = await rawRequest(port, "POST", `/v1/runs/${id}/revise-task`, JSON.stringify({
            wait_index: waitIndex,
            task_id: "task-a",
            task: { root_id: "tasks", ref: reviseRef },
          }));
          expect(mutation.status).toBe(200);
        }
      } else if (boundaryKind === "resumable") {
        await rawRequest(port, "POST", `/v1/runs/${id}/resume`, "{}");
      } else if (boundaryKind === "in_flight") {
        await wait(60);
      } else {
        throw new Error(`unexpected boundary ${boundaryKind}`);
      }
    }
    throw new Error(`the run ${id} never reached a terminal boundary`);
  };

  const getObservation = async (id: string): Promise<string> => {
    const response = await rawRequest(port, "GET", `/v1/runs/${id}/changes?after=0&limit=100`);
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as { observation: string };
    return body.observation;
  };

  const postApply = async (id: string, applyId: string, observationDigest: string): Promise<RawResponse> =>
    await rawRequest(port, "POST", `/v1/runs/${id}/project-applies/${applyId}`, JSON.stringify({ schema_version: 1, observation: observationDigest }));

  const getApply = async (id: string, applyId: string): Promise<RawResponse> =>
    await rawRequest(port, "GET", `/v1/runs/${id}/project-applies/${applyId}`);

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "mode.txt"), "TASK\n", { mode: 0o644 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "CHANGES-TASK\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-fail.md"), "CHANGES-FAIL\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-a-v2.md"), "APPLY-TASK-V2\n", { mode: 0o600 });
    mkdirSync(storeRoot, { mode: 0o700 });
    mkdirSync(applyStoreRoot, { mode: 0o700 });
    writeFileSync(
      registryPath,
      JSON.stringify({
        schema_version: 1,
        projects: [
          {
            id: "project-a",
            project_root: projectDir,
            config_root: join(fixtureRoot, "config"),
            pipeline_root: join(fixtureRoot, "bundle"),
            launcher_id: null,
            input_roots: [{ id: "tasks", root: inputRoot }],
            project_apply: true,
          },
          {
            id: "project-b",
            project_root: join(smokeRoot, "project-b"),
            config_root: join(fixtureRoot, "config"),
            pipeline_root: join(fixtureRoot, "bundle"),
            launcher_id: null,
            input_roots: [],
          },
        ],
      }),
      { mode: 0o600 },
    );
    mkdirSync(join(smokeRoot, "project-b"), { mode: 0o700 });
    const log = join(smokeRoot, "daemon.log");
    const daemonLogFd = openSync(log, "a");
    const daemon = Bun.spawn([process.execPath, join(import.meta.dir, "control_plane_project_changes_daemon.ts"), socketPath, "50"], {
      stdout: daemonLogFd,
      stderr: daemonLogFd,
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("apply daemon socket never appeared");
    await startService(true);
  }, 30000);

  afterAll(async () => {
    if (service !== undefined) await service.stop();
    if (daemonPid !== 0) {
      try {
        process.kill(daemonPid);
      } catch {
        // already gone
      }
    }
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  test("C: the happy path applies the exact result tree to the registered project", async () => {
    const applied = await driveRun(runId, "task.md", "task-a-v2.md");
    expect(applied).toBe("succeeded");
    observation = await getObservation(runId);
    expect(observation).toMatch(/^[0-9a-f]{64}$/);
    const beforeStore = applyStoreFingerprint(applyStoreRoot);
    const response = await postApply(runId, "apply-1", observation);
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body).toEqual({
      schema_version: 1,
      kind: "project_apply_operation",
      run_id: runId,
      apply_id: "apply-1",
      project_id: "project-a",
      observation,
      revision: expect.any(Number),
      status: "applied",
      completed_step_count: expect.any(Number),
      total_step_count: expect.any(Number),
    });
    expect(body["total_step_count"] as number).toBeGreaterThan(0);
    expect(body["completed_step_count"]).toBe(body["total_step_count"]);
    expect(applyStoreFingerprint(applyStoreRoot)).not.toBe(beforeStore);
    const rows = treeRows(projectDir);
    const read = (path: string): TreeRow | null => rows.find((row) => row.path === path) ?? null;
    expect(read("dev-notes.txt")?.bytes).toBe("DEV-NOTES\n");
    expect(read("README.md")?.bytes).toBe("README-CHANGED\n");
    expect(read("report-copy.txt")?.bytes).toBe("REPORT-COPY\n");
    expect(read("mode.txt")?.bytes).toBe("TASK\n");
  }, 180000);

  test("D: the same POST repeated is the idempotent applied result with zero new changes", async () => {
    const beforeProject = fingerprintTree(projectDir);
    const beforeStore = applyStoreFingerprint(applyStoreRoot);
    const response = await postApply(runId, "apply-1", observation);
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body["status"]).toBe("applied");
    expect(body["apply_id"]).toBe("apply-1");
    expect(fingerprintTree(projectDir)).toBe(beforeProject);
    expect(applyStoreFingerprint(applyStoreRoot)).toBe(beforeStore);
  }, 60000);

  test("F: the same apply id with a different observation or run is the typed identity conflict", async () => {
    const applied = await driveRun(noCapabilityRunId, "task.md", "task-a-v2.md");
    expect(applied).toBe("succeeded");
    const beforeProject = fingerprintTree(projectDir);
    const beforeStore = applyStoreFingerprint(applyStoreRoot);
    const wrongObservation = await postApply(runId, "apply-1", "b".repeat(64));
    expect(wrongObservation.status).toBe(200);
    expect(JSON.parse(wrongObservation.body)).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: runId,
      outcome: "failed",
      exit_code: 1,
      reason: "identity_conflict",
    });
    const secondObservation = await getObservation(noCapabilityRunId);
    const foreignRun = await postApply(noCapabilityRunId, "apply-1", secondObservation);
    expect(foreignRun.status).toBe(200);
    expect(JSON.parse(foreignRun.body)["reason"]).toBe("identity_conflict");
    expect(fingerprintTree(projectDir)).toBe(beforeProject);
    expect(applyStoreFingerprint(applyStoreRoot)).toBe(beforeStore);
  }, 180000);

  test("A: a project without the capability refuses POST with zero apply-store and project effects", async () => {
    await service.bindingStore.claim({ runId: `noapply-${suffix}`, projectId: "project-b" });
    const beforeProject = fingerprintTree(projectDir);
    const beforeStore = applyStoreFingerprint(applyStoreRoot);
    const response = await postApply(`noapply-${suffix}`, "nope-1", "a".repeat(64));
    expect(response.status).toBe(409);
    expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "project_apply_disabled" });
    expect(applyStoreFingerprint(applyStoreRoot)).toBe(beforeStore);
    expect(fingerprintTree(projectDir)).toBe(beforeProject);
  }, 60000);

  test("B: an absent apply-store root makes the capability unavailable on both routes", async () => {
    await service.stop();
    await startService(false);
    const post = await postApply(runId, "nostore-1", observation);
    expect(post.status).toBe(409);
    expect(JSON.parse(post.body)["reason"]).toBe("project_apply_disabled");
    const get = await getApply(runId, "nostore-1");
    expect(get.status).toBe(409);
    expect(JSON.parse(get.body)["reason"]).toBe("project_apply_disabled");
    await service.stop();
    await startService(true);
  }, 60000);

  test("G: GET projects the durable preparing, failed and conflict statuses exactly and writes nothing", async () => {
    resetOperatorToBaseline();

    // `preparing`: a graceful fault at the plan publication leaves the
    // operation preparing (the documented retryable window); GET
    // projects it verbatim.
    const preparingId = "status-preparing";
    const docFaultPreparing = faultDocIo();
    docFaultPreparing.failOnce("link", 1);
    try {
      await preparePipelineV2ProjectApplyCore(
        { applyStoreRoot, applyId: preparingId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
        { ...production, docIo: docFaultPreparing.io },
      );
      throw new Error("expected the injected failure");
    } catch {
      // expected graceful failure
    }
    const preparingBefore = applyStoreFingerprint(applyStoreRoot);
    const preparingGet = await getApply(runId, preparingId);
    expect(preparingGet.status).toBe(200);
    expect(JSON.parse(preparingGet.body)).toMatchObject({
      kind: "project_apply_operation",
      run_id: runId,
      apply_id: preparingId,
      project_id: "project-a",
      observation,
      status: "preparing",
      total_step_count: 0,
      completed_step_count: 0,
    });
    expect(applyStoreFingerprint(applyStoreRoot)).toBe(preparingBefore);

    // `failed`: a reviewed result carrying a hardlink group is a typed
    // unsupported_metadata refusal recorded on the journal.
    const fixture = await buildFinalizedApplyRun("apply-e2e-failed", {
      worker: async (ws) => {
        linkSync(join(ws, "base.txt"), join(ws, "hard.txt"));
      },
    });
    await service.bindingStore.claim({ runId: fixture.runId, projectId: "project-a" });
    const failedId = "status-failed";
    let failedReason = "";
    try {
      await preparePipelineV2ProjectApplyCore(
        { applyStoreRoot, applyId: failedId, runId: fixture.runId, projectId: "project-a", projectRoot: fixture.operatorProject, observation: fixture.observation, stateRoot: fixture.stateRoot },
        production,
      );
    } catch (cause) {
      failedReason = (cause as { reason?: string }).reason ?? String(cause);
    }
    expect(failedReason).toBe("unsupported_metadata");
    const failedGet = await getApply(fixture.runId, failedId);
    expect(failedGet.status).toBe(200);
    expect(JSON.parse(failedGet.body)["status"]).toBe("failed");
    expect(JSON.parse(failedGet.body)["run_id"]).toBe(fixture.runId);
    expect(treeRows(projectDir).find((row) => row.path === "README.md")?.bytes).toBe("README\n");
    await disposeApplyRoots();

    // `conflict`: a foreign object at a planned target refuses the real
    // writer before any mutation and lands the terminal conflict status.
    const conflictId = "status-conflict";
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: conflictId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    writeFileSync(join(projectDir, "README.md"), "FOREIGN\n", { mode: 0o644 });
    const before = fingerprintTree(projectDir);
    const refused = await applyPreparedProjectCore(
      { applyStoreRoot, applyId: conflictId, projectId: "project-a", projectRoot: projectDir },
      productionWriter,
    );
    expect(refused.ok).toBe(false);
    if (!refused.ok) expect(refused.reason).toBe("project_conflict");
    expect(fingerprintTree(projectDir)).toBe(before);
    const conflictGet = await getApply(runId, conflictId);
    expect(conflictGet.status).toBe(200);
    expect(JSON.parse(conflictGet.body)["status"]).toBe("conflict");
    expect(JSON.parse(conflictGet.body)["completed_step_count"]).toBe(0);
    // POST is the only public recovery owner: an exact retry keeps the
    // conflict (the foreign object is never removed by the orchestrator).
    const retry = await postApply(runId, conflictId, observation);
    expect(JSON.parse(retry.body)["reason"]).toBe("project_conflict");
    expect(JSON.parse((await getApply(runId, conflictId)).body)["status"]).toBe("conflict");
    resetOperatorToBaseline();
  }, 120000);

  test("I+E: a foreign temp forces recovery_required; POST rolls the recovery forward; the foreign object survives", async () => {
    resetOperatorToBaseline();
    const recoveryId = "recovery-1";
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: recoveryId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    const fops = faultFdOps();
    fops.failCrashOnce("openat", 1, (args: unknown[]) => String((args[1] as Buffer).toString("utf8")).startsWith(".apply-tmp-"));
    let crashed = false;
    try {
      await applyPreparedProjectCore(
        { applyStoreRoot, applyId: recoveryId, projectId: "project-a", projectRoot: projectDir },
        { ...productionWriter, fdOps: fops.ops },
      );
    } catch {
      crashed = true;
    }
    expect(crashed).toBe(true);
    const journal = JSON.parse(readFileSync(join(applyStoreRoot, "operations", recoveryId, "state.json"), "utf8")) as Record<string, unknown>;
    const tempName = String(((journal["execution"] as Record<string, unknown>)["active_step"] as Record<string, unknown>)["temp_name"]);
    mkdirSync(join(projectDir, tempName), { mode: 0o700 });
    const refused = await postApply(runId, recoveryId, observation);
    expect(refused.status).toBe(200);
    expect(JSON.parse(refused.body)).toEqual({
      schema_version: 1,
      kind: "command_result",
      run_id: runId,
      outcome: "failed",
      exit_code: 1,
      reason: "recovery_required",
    });
    const statusGet = await getApply(runId, recoveryId);
    expect(JSON.parse(statusGet.body)["status"]).toBe("recovery_required");
    expect(existsSync(join(projectDir, tempName))).toBe(true);
    // The operator removes the foreign object; POST rolls forward to
    // applied without duplicates.
    await rm(join(projectDir, tempName), { recursive: true, force: true });
    const healed = await postApply(runId, recoveryId, observation);
    expect(healed.status).toBe(200);
    expect(JSON.parse(healed.body)["status"]).toBe("applied");
    expect(treeRows(projectDir).find((row) => row.path === "README.md")?.bytes).toBe("README-CHANGED\n");
  }, 120000);

  test("H: GET of an applying operation neither mutates it nor runs recovery; POST continues it", async () => {
    resetOperatorToBaseline();
    const applyingId = "applying-1";
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: applyingId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    const stream = faultStreamIo();
    stream.failReadCrashOnce(2);
    try {
      await applyPreparedProjectCore(
        { applyStoreRoot, applyId: applyingId, projectId: "project-a", projectRoot: projectDir },
        { ...productionWriter, fileStreamIo: stream.io },
      );
    } catch {
      // expected crash
    }
    const journal = JSON.parse(readFileSync(join(applyStoreRoot, "operations", applyingId, "state.json"), "utf8")) as Record<string, unknown>;
    expect(journal["status"]).toBe("applying");
    const before = fingerprintTree(projectDir);
    const beforeStore = applyStoreFingerprint(applyStoreRoot);
    const got = await getApply(runId, applyingId);
    expect(got.status).toBe(200);
    const body = JSON.parse(got.body) as Record<string, unknown>;
    expect(body["status"]).toBe("applying");
    expect(body["completed_step_count"]).toBe(((journal["execution"] as Record<string, unknown>)["completed_step_count"] as number));
    expect(fingerprintTree(projectDir)).toBe(before);
    expect(applyStoreFingerprint(applyStoreRoot)).toBe(beforeStore);
    const journalAfter = JSON.parse(readFileSync(join(applyStoreRoot, "operations", applyingId, "state.json"), "utf8")) as Record<string, unknown>;
    expect(journalAfter).toEqual(journal);
    // POST is the only public recovery owner: it continues the frontier.
    const continued = await postApply(runId, applyingId, observation);
    expect(continued.status).toBe(200);
    expect(JSON.parse(continued.body)["status"]).toBe("applied");
    expect(treeRows(projectDir).find((row) => row.path === "README.md")?.bytes).toBe("README-CHANGED\n");
  }, 120000);

  test("J: two concurrent POSTs of one apply id converge to one package and one applied result", async () => {
    resetOperatorToBaseline();
    const concurrentId = "concurrent-1";
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: concurrentId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    const beforeStoreRows = applyStoreFingerprint(applyStoreRoot).split("\n").length;
    const [first, second] = await Promise.all([postApply(runId, concurrentId, observation), postApply(runId, concurrentId, observation)]);
    const outcomes = [first, second].map((response) => JSON.parse(response.body) as Record<string, unknown>);
    const markers = outcomes.map((body) => String(body["status"] ?? body["reason"]));
    expect(markers).toContain("applied");
    for (const marker of markers) {
      expect(["applied", "project_busy", "project_conflict", "preparation_busy"]).toContain(marker);
    }
    expect(applyStoreFingerprint(applyStoreRoot).split("\n").length).toBe(beforeStoreRows);
    expect(treeRows(projectDir).find((row) => row.path === "README.md")?.bytes).toBe("README-CHANGED\n");
    expect(treeRows(projectDir).find((row) => row.path === "dev-notes.txt")?.bytes).toBe("DEV-NOTES\n");
  }, 120000);

  test("K: two apply ids of one project are serialized by the exclusive lock; no hybrid tree", async () => {
    resetOperatorToBaseline();
    const leftId = "race-left";
    const rightId = "race-right";
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: leftId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: rightId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    const [left, right] = await Promise.all([postApply(runId, leftId, observation), postApply(runId, rightId, observation)]);
    const outcomes = [left, right].map((response) => JSON.parse(response.body) as Record<string, unknown>);
    const markers = outcomes.map((body) => String(body["status"] ?? body["reason"]));
    expect(markers).toContain("applied");
    for (const marker of markers) {
      expect(["applied", "project_busy", "project_conflict", "preparation_busy"]).toContain(marker);
    }
    const rows = treeRows(projectDir);
    expect(rows.find((row) => row.path === "README.md")?.bytes).toBe("README-CHANGED\n");
    expect(rows.find((row) => row.path === "dev-notes.txt")?.bytes).toBe("DEV-NOTES\n");
    expect(rows.find((row) => row.path === "mode.txt")?.bytes).toBe("TASK\n");
  }, 120000);

  test("L: a held exclusive project lock yields typed busy results for the read APIs, then the full result", async () => {
    resetOperatorToBaseline();
    const acquisition = await acquirePipelineV2ApplyProjectLock(projectDir);
    if (acquisition.busy) throw new Error("the project lock must be free");
    const lock = acquisition.lock;
    try {
      const changes = await rawRequest(port, "GET", `/v1/runs/${runId}/changes?after=0&limit=100`);
      expect(changes.status).toBe(200);
      expect(JSON.parse(changes.body)).toEqual({
        schema_version: 1,
        kind: "command_result",
        run_id: runId,
        outcome: "failed",
        exit_code: 1,
        reason: "project_busy",
      });
      const inspect = await rawRequest(port, "GET", `/v1/runs/${runId}`);
      expect(inspect.status).toBe(200);
    } finally {
      lock.release();
    }
    const changes = await rawRequest(port, "GET", `/v1/runs/${runId}/changes?after=0&limit=100`);
    expect(changes.status).toBe(200);
    expect((JSON.parse(changes.body) as { kind: string }).kind).toBe("project_changes");
  }, 60000);

  test("M: a tampered payload refuses with zero project bytes", async () => {
    resetOperatorToBaseline();
    const tamperedId = "tampered-1";
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: tamperedId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    const payloadDir = join(applyStoreRoot, "operations", tamperedId, "payloads");
    const payload = readdirSync(payloadDir)[0];
    if (payload === undefined) throw new Error("no payload sealed");
    writeFileSync(join(payloadDir, payload), "TAMPERED-BYTES");
    const before = fingerprintTree(projectDir);
    const response = await postApply(runId, tamperedId, observation);
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)["reason"]).toBe("artifact_modified");
    expect(fingerprintTree(projectDir)).toBe(before);
  }, 60000);

  test("N: a read-only project is the typed project_read_only refusal, never internal_error", async () => {
    resetOperatorToBaseline();
    const readOnlyId = "readonly-1";
    await preparePipelineV2ProjectApplyCore(
      { applyStoreRoot, applyId: readOnlyId, runId, projectId: "project-a", projectRoot: projectDir, observation, stateRoot },
      production,
    );
    chmodSync(projectDir, 0o555);
    try {
      const response = await postApply(runId, readOnlyId, observation);
      expect(response.status).toBe(200);
      const body = JSON.parse(response.body) as Record<string, unknown>;
      expect(body["reason"]).toBe("project_read_only");
      expect(treeRows(projectDir).find((row) => row.path === "README.md")?.bytes).toBe("README\n");
    } finally {
      chmodSync(projectDir, 0o700);
    }
    const healed = await postApply(runId, readOnlyId, observation);
    expect(JSON.parse(healed.body)["status"]).toBe("applied");
  }, 120000);

  test("O: a failed terminal run applies its partial changes under the same contract", async () => {
    resetOperatorToBaseline();
    writeFileSync(join(projectDir, "mode.txt"), "FAIL\n", { mode: 0o644 });
    const applied = await driveRun(failRunId, "task.md", "task-a-v2.md");
    expect(applied).toBe("failed");
    const failedObservation = await getObservation(failRunId);
    const response = await postApply(failRunId, "apply-fail-1", failedObservation);
    expect(response.status).toBe(200);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body["status"]).toBe("applied");
    const rows = treeRows(projectDir);
    expect(rows.find((row) => row.path === "dev-notes.txt")?.bytes).toBe("DEV-NOTES\n");
    expect(rows.find((row) => row.path === "README.md")?.bytes).toBe("README-CHANGED\n");
    writeFileSync(join(projectDir, "mode.txt"), "TASK\n", { mode: 0o644 });
    resetOperatorToBaseline();
  }, 180000);

  test("the responses never leak a host path, temp identity or credential", async () => {
    const get = await getApply(runId, "apply-1");
    for (const secret of secrets) {
      expect(get.body).not.toContain(secret);
    }
    expect(get.body).not.toContain(".apply-tmp-");
    expect(get.body).not.toContain("smoke-credential-token");
    const post = await postApply(runId, "apply-1", observation);
    for (const secret of secrets) {
      expect(post.body).not.toContain(secret);
    }
  }, 60000);

  test("the malformed request battery is 400 with zero client calls", async () => {
    for (const body of ["", "not json", "{}", "[]", JSON.stringify({ schema_version: 1 }), JSON.stringify({ observation: "a".repeat(64) }), JSON.stringify({ schema_version: 2, observation: "a".repeat(64) }), JSON.stringify({ schema_version: 1, observation: "a".repeat(64), extra: 1 }), JSON.stringify({ schema_version: 1, observation: "A".repeat(64) })]) {
      const response = await rawRequest(port, "POST", `/v1/runs/${runId}/project-applies/malformed-1`, body);
      expect(response.status, body.slice(0, 40)).toBe(400);
      expect(JSON.parse(response.body)).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_request" });
    }
    const unbound = await getApply(runId, "malformed-1");
    expect(unbound.status).toBe(404);
    expect(JSON.parse(unbound.body)["reason"]).toBe("project_apply_not_found");
  }, 60000);
});

describe("explicit Project Apply: source restrictions", () => {
  const read = (path: string): string => readFileSync(join(import.meta.dir, "..", "src", path), "utf8");

  test("the routing layer is the single command-argv owner for the apply commands", () => {
    const routing = read(join("control_plane", "routing.ts"));
    expect(routing.match(/"project-apply"/g)?.length).toBe(1);
    expect(routing.match(/"project-apply-inspect"/g)?.length).toBe(1);
  });

  test("the HTTP layer never takes a project path, apply-store root or run root from the request", () => {
    const server = read(join("control_plane", "http_server.ts"));
    const postCase = server.slice(server.indexOf('case "project_apply_post"'), server.indexOf('case "resume":'));
    expect(postCase).toContain("authority.project.projectRoot");
    expect(postCase).toContain("deps.applyStoreRoot");
    expect(postCase).toContain("authority.project.id");
    expect(postCase).toContain("body.observation");
    expect(postCase).not.toMatch(/body\.(project|configRoot|applyStoreRoot|runRoot|plan|payload|path)/);
    const getParsed = parseControlPlaneRoute("/v1/runs/r1/project-applies/a1", "GET");
    expect(getParsed instanceof ControlPlaneHttpRequestError).toBe(false);
  });

  test("the capability has exactly one owner and the statuses one vocabulary", () => {
    const registry = read(join("control_plane", "project_registry.ts"));
    expect(registry.match(/projectApply/g)?.length).toBeGreaterThan(0);
    expect((registry.match(/project_apply/g)?.length ?? 0)).toBeGreaterThan(0);
    const client = read(join("control_plane", "subprocess_client.ts"));
    expect(client).toContain('from "../pipeline_v2_apply_state_internal.ts"');
    expect(client).toContain('from "../pipeline_v2_project_apply.ts"');
    expect(client).not.toMatch(/"preparing",\s*"prepared"/);
    const facade = read("pipeline_v2_project_apply.ts");
    expect(facade).toContain("new Set<string>");
    expect(facade.match(/"preparing"/g)?.length ?? 0).toBe(0);
  });

  test("the CLI reports through the shared Apply state vocabulary without a copied list", () => {
    const main = read("main.ts");
    expect(main).toContain("applyPipelineV2ProjectOperation");
    expect(main).toContain("inspectPipelineV2ProjectApplyOperationForRun");
    expect(main).not.toMatch(/"recovery_required"/);
  });
});
