/**
 * Permanent HTTP integration of the browser UI v9 lifecycle-history
 * section: the real `app.js` identity and execution-bindings parsers
 * and merges consume the real endpoints (real CLI subprocess over the
 * loopback service), the full 66-record lifecycle streams paginate
 * into the two-stream pinned state, the derivation places every stage
 * execution into its durable generation/iteration, the compare pair of
 * the two same-template generations uses different plan revisions, the
 * historical interventions appear on a terminal run, the cursor
 * contract (caught-up empty page, beyond-the-snapshot, stale pin) is
 * pinned against the real endpoints, authority failures fail closed,
 * and every identity/execution-bindings GET is zero-write over the run
 * tree.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  applyBindingsPage,
  applyIdentityPage,
  createLifecycleHistoryState,
  deriveLifecycleHistory,
  parseExecutionBindingsDocument,
  parseIdentityDocument,
  type UiLifecycleHistoryState,
} from "../ui/app.js";
import { lifecycleCommands } from "./pipeline_v2_commit_history_fixture.ts";
import { E2E_YAML } from "./pipeline_v2_events_fixture.ts";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";
import { ControlPlaneRunBindingStore } from "../src/control_plane/run_binding_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { pipelineV2RunPipelineIdentity } from "../src/pipeline_v2_digest.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";

const CANARY = "uieventshostpath";

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

function fingerprintTree(root: string): string {
  const rows: string[] = [];
  const walk = (dir: string, prefix: string): void => {
    const entries = readdirSync(dir, { withFileTypes: true });
    entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const entry of entries) {
      const child = join(dir, entry.name);
      const rel = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      const info = lstatSync(child);
      rows.push(`${rel}|${info.isDirectory() ? "d" : "f"}|${info.size}`);
      if (info.isDirectory()) {
        walk(child, rel);
      }
    }
  };
  walk(root, "");
  return rows.join("\n");
}

function expectNoLeak(text: string, secrets: readonly string[]): void {
  for (const secret of secrets) {
    expect(text.includes(secret)).toBe(false);
  }
}

interface IdentityPage {
  readonly schema_version: 1;
  readonly kind: "identity";
  readonly run_id: string;
  readonly revision: number;
  readonly after: number;
  readonly records: readonly Record<string, unknown>[];
  readonly next_after: number | null;
}

describe("control-plane ui lifecycle history: the real endpoints through the UI model", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-lh-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: RunningControlPlaneHttpService | null = null;
  let port = 0;
  const runSuffix = Math.random().toString(36).slice(2, 8);
  const full = `ui-lh-full-${runSuffix}`;
  const ghostRunId = `ui-lh-ghost-${runSuffix}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, "smoke-credential-token", CANARY];

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "UI-LH-TASK\n", { mode: 0o600 });
    mkdirSync(storeRoot, { mode: 0o700 });
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
          },
        ],
      }),
      { mode: 0o600 },
    );

    // the synthetic full run: the 66-command reducer lifecycle written
    // into the service's state root over a real loadable bundle
    // directory, then bound to the registry project
    const bundle = join(smokeRoot, "synth-bundle");
    await mkdir(bundle, { recursive: true });
    await writeFile(join(bundle, "pipeline.yaml"), E2E_YAML);
    await mkdir(join(bundle, "prompts"), { recursive: true });
    await writeFile(join(bundle, "prompts", "architect.md"), "PLAN-THE-WORK\n");
    await writeFile(join(bundle, "prompts", "coder.md"), "CODE-THE-TASK\n");
    await mkdir(join(bundle, "schemas"), { recursive: true });
    await writeFile(join(bundle, "schemas", "plan.schema.json"), JSON.stringify({ type: "object" }));
    const pipeline = await loadPipelineV2(bundle);
    const identity = pipelineV2RunPipelineIdentity(pipeline);
    const all = lifecycleCommands();
    expect(all.length).toBe(66);
    const sink = new PipelineV2RunStateSink({ stateRoot, runId: full });
    for (const command of all) {
      if (command.kind === "create_run") {
        await sink.dispatch({ ...command, runId: full, pipeline: identity } as never);
        continue;
      }
      await sink.dispatch(command as never);
    }
    expect(sink.snapshot?.revision).toBe(66);
    const store = await ControlPlaneRunBindingStore.open(storeRoot);
    const claim = await store.claim({ runId: full, projectId: "project-a" });
    expect(["created", "existing"]).toContain(claim.kind);
    // a bound run id with no durable state at all
    const ghost = await store.claim({ runId: ghostRunId, projectId: "project-a" });
    expect(["created", "existing"]).toContain(ghost.kind);

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
        },
        stdoutLimitBytes: 4_000_000,
        stderrLimitBytes: 4_000_000,
        sigintGraceMs: 3000,
      },
      port: 0,
      bodyLimitBytes: 4096,
    });
    port = service.port;
  }, 60000);

  afterAll(async () => {
    if (service !== null) await service.stop();
    rmSync(smokeRoot, { recursive: true, force: true });
  });

  async function readIdentityPage(runId: string, after: number, limit: number, revision?: number): Promise<{ status: number; page: Record<string, unknown> }> {
    const pin = revision === undefined ? "" : `&revision=${revision}`;
    const response = await rawRequest(port, "GET", `/v1/runs/${runId}/identity?after=${after}&limit=${limit}${pin}`);
    return { status: response.status, page: JSON.parse(response.body) as Record<string, unknown> };
  }

  async function readBindingsPage(runId: string, after: number, limit: number, revision?: number): Promise<{ status: number; page: Record<string, unknown> }> {
    const pin = revision === undefined ? "" : `&revision=${revision}`;
    const response = await rawRequest(port, "GET", `/v1/runs/${runId}/execution-bindings?after=${after}&limit=${limit}${pin}`);
    return { status: response.status, page: JSON.parse(response.body) as Record<string, unknown> };
  }

  test("ih1. the full identity lifecycle paginates through the real endpoint into the UI model", async () => {
    let state: UiLifecycleHistoryState = createLifecycleHistoryState(full, 66);
    const pages: Record<string, unknown>[] = [];
    let cursor = 0;
    for (;;) {
      const { status, page } = await readIdentityPage(full, cursor, 20, 66);
      expect(status).toBe(200);
      expect(page.kind).toBe("identity");
      const document = parseIdentityDocument(page as never, full);
      pages.push(page);
      state = applyIdentityPage(state, document, cursor) as UiLifecycleHistoryState;
      if (state === null) throw new Error("the pinned identity page must apply");
      if (document.nextAfter === null) break;
      cursor = document.nextAfter;
    }
    expect(pages.length).toBe(2);
    for (const page of pages) {
      expect(page.revision).toBe(66);
    }
    expect(state.identity.cursor).toBe(pages.flatMap((page) => page.records as unknown[]).length);
    expect(state.error).toBeNull();
    // the two generations carry the exact durable anchors of the fixture
    const generationRecords = state.identity.records.filter((record) => record.record === "generation");
    expect(generationRecords.map((record) => record.index)).toEqual([1, 2]);
    expect(generationRecords.map((record) => record.templateId)).toEqual(["development", "development"]);
    expect(generationRecords.map((record) => record.initialBudget)).toEqual([2, 3]);
    // the identity stream alone is placement-incomplete: stage
    // executions wait for the bindings page (not loaded yet, honest)
    const partial = deriveLifecycleHistory(state, true);
    expect(partial.complete).toBe(false);
  }, 60000);

  test("ih2. both streams together place every stage execution and expose the pair", async () => {
    let state: UiLifecycleHistoryState = createLifecycleHistoryState(full, 66);
    const { page: bindingPage, status: bindingStatus } = await readBindingsPage(full, 0, 200, 66);
    console.log("IH2 bindings count:", (bindingPage.records as unknown[]).length, JSON.stringify(bindingPage.records).slice(0, 600));
    expect(bindingStatus).toBe(200);
    expect(bindingPage.kind).toBe("execution_bindings");
    const bindings = parseExecutionBindingsDocument(bindingPage as never, full);
    state = applyBindingsPage(state, bindings, 0) as UiLifecycleHistoryState;
    expect(state === null).toBe(false);
    for (let cursor = 0; ; cursor += 20) {
      const { page: identityPage, status } = await readIdentityPage(full, cursor, 20, 66);
      expect(status).toBe(200);
      const document = parseIdentityDocument(identityPage as never, full);
      const merged = applyIdentityPage(state, document, cursor) as UiLifecycleHistoryState;
      expect(merged === null).toBe(false);
      state = merged;
      if (document.nextAfter === null) break;
    }
    const derived = deriveLifecycleHistory(
      state,
      true,
    ) as {
      complete: boolean;
      error: { reason: string } | null;
      generations: { generation: { index: number; templateId: string; planSha256: string }; iterations: unknown[]; executions: { record: { executionRole: string; stateId: string; stageTask: unknown } }[] }[];
      unplacedExecutions: { record: { executionRole: string; stateId: string } }[];
      grants: unknown[];
      historical: unknown;
    };
    expect(derived.complete).toBe(true);
    expect(derived.error).toBeNull();
    // two generations, three iterations, one grant, two answered waits
    expect(derived.generations.length).toBe(2);
    expect(derived.generations.map((card) => card.iterations.length).sort()).toEqual([1, 2]);
    expect(derived.grants.length).toBe(1);
    // the unplaced execution is exactly the initial planning execution;
    // the replanned planning execution is durably bound to the still-open
    // generation at its start and renders inside that generation's card
    const unplaced = derived.unplacedExecutions.map((view) => `${view.record.executionRole} ${view.record.stateId}`).sort();
    expect(unplaced).toEqual(["planning architect"]);
    // the stage executions sit in their recorded generations across
    // both generations (iteration 1, iteration 2, generation 2 iteration 1)
    const placed = derived.generations.map((card) => card.executions.length).sort();
    expect(placed).toEqual([1, 3]);
    // the compare pair: same template, different plan revisions
    const first = derived.generations[0];
    const second = derived.generations[1] ?? null;
    expect(first).not.toBeNull();
    expect(second).not.toBeNull();
    expect((first as { generation: { templateId: string } }).generation.templateId).toBe((second as { generation: { templateId: string } }).generation.templateId);
    expect(
      (first as { generation: { planSha256: string } }).generation.planSha256 === (second as { generation: { planSha256: string } }).generation.planSha256,
    ).toBe(false);
    // the terminal run exposes the historical interventions group
    expect(derived.historical === null).toBe(false);
    const historical = derived.historical as { waits: { index: number; response: { actionId: string } | null }[]; actions: unknown[]; grants: unknown[] };
    expect(historical.waits.length).toBe(2);
    expect(historical.waits.map((wait) => wait.response === null ? "open" : wait.response.actionId).sort()).toEqual(["continue_stage", "revise_task"]);
    expect(historical.actions.length).toBe(4);
    expectNoLeak(JSON.stringify({ generations: derived.generations, historical: derived.historical }), secrets);
  }, 60000);

  test("ih3. the cursor contract: caught-up empty page, beyond-the-snapshot, stale pin", async () => {
    // determine the exact record total through the pagination walk
    let total = 0;
    for (;;) {
      const page = await readIdentityPage(full, total, 20, 66);
      expect(page.status).toBe(200);
      total = total + ((page.page.records as unknown[]) ?? []).length;
      if ((page.page.next_after as unknown) === null) break;
    }
    // caught up: an empty page with a null cursor
    const caught = await readIdentityPage(full, total, 20, 66);
    expect(caught.status).toBe(200);
    expect((caught.page.next_after as unknown as number | null) === null).toBe(true);
    expect(caught.page.after).toBe(total);
    // beyond the snapshot is a typed request-layer failure before any cursor work
    const beyond = await readIdentityPage(full, total + 1, 20, 66);
    expect(beyond.status).toBe(400);
    expect(beyond.page).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" });
    // a changed pin during continuation is the typed stale refusal
    const stale = await readIdentityPage(full, 0, 20, 65);
    expect(stale.status).toBe(409);
    expect(stale.page).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "stale_snapshot" });
    // the same contract holds for the execution-bindings endpoint
    const bindingsBeyond = await readBindingsPage(full, 999, 20);
    expect(bindingsBeyond.status).toBe(400);
    expect(bindingsBeyond.page).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" });
  }, 60000);

  test("ih4. an unbound run and a bound-but-missing run fail closed", async () => {
    const unboundIdentity = await readIdentityPage("never-bound-lh", 0, 20);
    expect(unboundIdentity.status).toBe(404);
    expect(unboundIdentity.page).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
    const unboundBindings = await readBindingsPage("never-bound-lh", 0, 20);
    expect(unboundBindings.status).toBe(404);
    expect(unboundBindings.page).toEqual({ schema_version: 1, kind: "error", layer: "binding", reason: "binding_not_found" });
    const ghostIdentity = await readIdentityPage(ghostRunId, 0, 20);
    expect(ghostIdentity.status).toBe(200);
    expect(ghostIdentity.page).toMatchObject({
      schema_version: 1,
      kind: "command_result",
      run_id: ghostRunId,
      outcome: "failed",
      exit_code: 1,
      reason: "run_not_found",
    });
    const ghostBindings = await readBindingsPage(ghostRunId, 0, 20);
    expect(ghostBindings.status).toBe(200);
    expect(ghostBindings.page).toMatchObject({
      schema_version: 1,
      kind: "command_result",
      run_id: ghostRunId,
      outcome: "failed",
      exit_code: 1,
      reason: "run_not_found",
    });
  }, 60000);

  test("ih5. every identity and execution-bindings GET is zero-write over the run tree", async () => {
    const runTree = join(stateRoot, "pipeline-runs", full);
    const before = fingerprintTree(runTree);
    for (const after of [0, 20]) {
      const identityResult = await readIdentityPage(full, after, 20, 66);
      expect(identityResult.status).toBe(200);
    }
    // the bindings page carries five records, so only the initial
    // cursor exists
    const bindingsResult = await readBindingsPage(full, 0, 200, 66);
    expect(bindingsResult.status).toBe(200);
    expect(fingerprintTree(runTree)).toBe(before);
    expect(existsSync(runTree)).toBe(true);
  }, 60000);
});
