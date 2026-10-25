/**
 * Permanent end-to-end suite of the browser UI v7 read-only Project
 * Change Content panel over the REAL production stack: the loopback
 * HTTP service, the project registry, the durable run binding store,
 * the real orchestrator CLI subprocess, the schema-v10 provenance and
 * the fake Docker Helper daemon whose stage workers write modified,
 * deleted, binary, symlink, directory and large-file changes into the
 * shared run project workspace.
 *
 * Matrix: the explicit Review-driven content GETs with byte-exact
 * reassembly, the added binary hex view, the deleted implicit-empty
 * diff side, the symlink/directory metadata-only sides, the stale
 * observation on a mid-file continuation, the diverged baseline
 * unavailability, the chunk reassembly against a one-shot full read,
 * the invalid cursor, the provenance tamper refusal, the restart
 * determinism, the zero-write fingerprints and the leakage scan.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, openSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  buildFreshRunBody,
  buildMutationRequest,
  buildRunActionModel,
  parseApiDocument,
  type UiApiModel,
  type UiRunActionModel,
} from "../ui/app.js";
import { createUiHarness, type UiHarness } from "./ui_dom_harness.ts";
import { startControlPlaneHttpService, type RunningControlPlaneHttpService } from "../src/control_plane/http_server.ts";

const CANARY = "uictnthostpath";

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

const wait = (ms = 30): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(predicate: () => boolean, what: string): Promise<void> {
  const deadline = Date.now() + 20000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`waitFor timeout: ${what}`);
    await wait(25);
  }
}

async function waitForInspect(harness: UiHarness): Promise<void> {
  await waitFor(() => harness.detailStatus() !== "" || harness.error() !== "", "inspect");
}

async function waitForChanges(harness: UiHarness): Promise<void> {
  await waitFor(() => harness.changesStatus() !== "Loading…" && (harness.changesStatus() !== "" || harness.changesMeta() !== ""), "changes settle");
}

describe("control-plane UI project change content: the read-only panel over the real stack", () => {
  const smokeRoot = mkdtempSync(join(tmpdir(), `ui-content-${CANARY}-`));
  const stateRoot = join(smokeRoot, "state");
  const credDir = join(smokeRoot, "xdg");
  const socketPath = join(smokeRoot, "helper.sock");
  const projectDir = join(smokeRoot, "project");
  const inputRoot = join(smokeRoot, "inputs");
  const storeRoot = join(smokeRoot, "bindings");
  const registryPath = join(smokeRoot, "registry.json");
  const fixtureRoot = join(import.meta.dir, "..", "fixtures", "control-plane");
  let service: RunningControlPlaneHttpService;
  let port = 0;
  let daemonPid = 0;
  const suffix = Math.random().toString(36).slice(2, 8);
  const runId = `cnt-a-${suffix}`;
  const secrets = [smokeRoot, stateRoot, credDir, inputRoot, socketPath, "smoke-credential-token", CANARY];
  const changeIndexByPath = new Map<string, number>();
  let currentObservation = "";
  const contentGets: string[] = [];
  const rawBodies: string[] = [];

  const startService = async (): Promise<void> => {
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
    });
    port = service.port;
  };

  const rawInspect = async (id: string): Promise<UiApiModel & { kind: "run" }> => {
    const response = await rawRequest(port, "GET", `/v1/runs/${id}`);
    expect(response.status).toBe(200);
    const model = parseApiDocument(JSON.parse(response.body));
    expect(model.kind).toBe("run");
    return model as UiApiModel & { kind: "run" };
  };

  const rawMutate = async (action: unknown, formValues: unknown): Promise<Record<string, unknown>> => {
    const request = buildMutationRequest(action, formValues);
    const response = await rawRequest(port, "POST", request.path, JSON.stringify(request.body));
    expect(response.status).toBe(200);
    const model = parseApiDocument(JSON.parse(response.body));
    expect(["command_result", "error"]).toContain(model.kind);
    return model as unknown as Record<string, unknown>;
  };

  const driveRun = async (id: string): Promise<"succeeded" | "failed"> => {
    const body = buildFreshRunBody("project-a", id, [{ id: "task", rootId: "tasks", ref: "task.md" }]);
    const created = await rawRequest(port, "POST", `/v1/projects/project-a/runs`, JSON.stringify(body));
    expect(created.status).toBe(200);
    const createdModel = parseApiDocument(JSON.parse(created.body)) as { kind: string; outcome?: string };
    expect(createdModel.outcome).toBe("plan_ready");
    const projects = parseApiDocument(JSON.parse((await rawRequest(port, "GET", "/v1/projects")).body)) as UiApiModel & { kind: "projects" };
    const runsPage = parseApiDocument(JSON.parse((await rawRequest(port, "GET", "/v1/runs?limit=100")).body));
    const entry = ((runsPage as unknown as { runs: { runId: string }[] }).runs.find((run) => run.runId === id) ?? null);
    let model = await rawInspect(id);
    let usedContinue = false;
    for (let round = 0; round < 8; round++) {
      const action = buildRunActionModel(model, { projectsModel: projects, runEntry: entry as never });
      const kind = (action as UiRunActionModel).kind;
      if (kind === "terminal") {
        return model.status === "succeeded" ? "succeeded" : "failed";
      }
      if (kind === "resume_plan") {
        const planningIndex = (model.boundary as { planningExecutionIndex?: number }).planningExecutionIndex;
        const step = await rawMutate(action, { stageId: planningIndex === 1 ? "stage-1" : "stage-2", initialBudget: "1" });
        expect(["waiting", "failed", "succeeded"]).toContain(step["outcome"] as string);
        if (step["outcome"] === "failed") {
          model = await rawInspect(id);
          continue;
        }
      } else if (kind === "waiting") {
        const controls = (action as unknown as { controls: Array<Record<string, unknown>> }).controls;
        const continueControl = controls.find((control) => control["kind"] === "continue_stage");
        const reviseControl = controls.find((control) => control["kind"] === "revise_task");
        if (continueControl !== undefined && !usedContinue) {
          usedContinue = true;
          const step = await rawMutate(continueControl, { additionalIterations: "1" });
          expect(step["outcome"]).toBe("waiting");
        } else if (reviseControl !== undefined) {
          const step = await rawMutate(reviseControl, { taskId: "task-a", rootId: "tasks", ref: "task-a-v2.md" });
          expect(step["outcome"]).toBe("plan_ready");
        } else {
          throw new Error(`no usable waiting control: ${JSON.stringify(controls)}`);
        }
      } else {
        throw new Error(`unexpected action kind ${kind}`);
      }
      model = await rawInspect(id);
    }
    throw new Error("the run never reached a terminal boundary");
  };

  const makeHarness = async (): Promise<{ harness: UiHarness; gets: string[]; posts: string[] }> => {
    const gets: string[] = [];
    const posts: string[] = [];
    const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
      const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
      if (method === "GET") {
        gets.push(path);
        if (path.includes("/content")) {
          contentGets.push(path);
        }
      } else {
        posts.push(path);
      }
      const response = await rawRequest(port, method, path);
      rawBodies.push(response.body);
      return new Response(response.body, { status: response.status, headers: { "content-type": "application/json" } });
    };
    const harness = await createUiHarness({ fetchImpl });
    return { harness, gets, posts };
  };

  const bootAndLoadChanges = async (): Promise<{ harness: UiHarness; posts: string[] }> => {
    const { harness, posts } = await makeHarness();
    harness.click("refresh-runs");
    await wait(50);
    harness.clickRunInspect(0);
    await waitForInspect(harness);
    await waitForChanges(harness);
    return { harness, posts };
  };

  const changesRowFor = (harness: UiHarness, path: string): number => {
    const rows = harness.changesRows();
    const index = rows.findIndex((row) => row.startsWith(`${path}|`));
    if (index < 0) {
      throw new Error(`changes row missing for ${path}: ${JSON.stringify(rows)}`);
    }
    return index;
  };

  const reviewButton = (harness: UiHarness, row: number) => {
    const buttons = harness.element("changes-body").children[row]!.querySelectorAll("button");
    const button = buttons.find((node) => node.textContent === "Review content");
    if (button === undefined) {
      throw new Error(`no Review content button on changes row ${row}`);
    }
    return button;
  };

  const fullChanges = async (id: string): Promise<Record<string, unknown>> => {
    const response = await rawRequest(port, "GET", `/v1/runs/${id}/changes?after=0&limit=200`);
    expect(response.status).toBe(200);
    rawBodies.push(response.body);
    return JSON.parse(response.body) as Record<string, unknown>;
  };

  const contentFull = async (id: string, side: string, observation: string, changeIndex: number): Promise<string> => {
    const response = await rawRequest(port, "GET", `/v1/runs/${id}/changes/${changeIndex}/content?side=${side}&observation=${observation}&limit=262144`);
    expect(response.status).toBe(200);
    rawBodies.push(response.body);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body["status"]).toBe("available");
    return Buffer.from(body["data_base64"] as string, "base64").toString("utf8");
  };

  beforeAll(async () => {
    mkdirSync(stateRoot, { mode: 0o700 });
    mkdirSync(join(credDir, "docker-helper"), { recursive: true, mode: 0o700 });
    writeFileSync(join(credDir, "docker-helper", "credential.token"), "smoke-credential-token", { mode: 0o600 });
    mkdirSync(projectDir, { mode: 0o700 });
    writeFileSync(join(projectDir, "README.md"), "README\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "mode.txt"), "CONTENT\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "text.txt"), "TEXT-BASE\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "old.txt"), "OLD\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "bin.dat"), "BINOLD\n", { mode: 0o644 });
    symlinkSync("target", join(projectDir, "link"));
    mkdirSync(join(projectDir, "dir"), { mode: 0o755 });
    writeFileSync(join(projectDir, "dir", "keep.txt"), "KEEP\n", { mode: 0o644 });
    writeFileSync(join(projectDir, "big.txt"), `${"L".repeat(99)}\n`.repeat(1024), { mode: 0o644 });
    writeFileSync(join(projectDir, "moded.txt"), "MODEDBASE\n", { mode: 0o644 });
    mkdirSync(inputRoot, { mode: 0o700 });
    writeFileSync(join(inputRoot, "task.md"), "CONTENT-TASK\n", { mode: 0o600 });
    writeFileSync(join(inputRoot, "task-a-v2.md"), "CONTENT-TASK-V2\n", { mode: 0o600 });
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
    const log = join(smokeRoot, "daemon.log");
    const daemonLogFd = openSync(log, "a");
    const daemon = Bun.spawn([process.execPath, join(import.meta.dir, "control_plane_project_changes_daemon.ts"), socketPath, "150"], {
      stdout: daemonLogFd,
      stderr: daemonLogFd,
    });
    daemonPid = daemon.pid ?? 0;
    daemon.unref();
    for (let i = 0; i < 60 && !existsSync(socketPath); i += 1) {
      await Bun.sleep(100);
    }
    if (!existsSync(socketPath)) throw new Error("content daemon socket never appeared");
    await startService();
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

  test(
    "f1. the CONTENT run reaches a terminal boundary and the changes page indexes every scenario",
    async () => {
      expect(await driveRun(runId)).toBe("succeeded");
      const page = await fullChanges(runId);
      currentObservation = page["observation"] as string;
      for (const entry of page["changes"] as Array<Record<string, unknown>>) {
        changeIndexByPath.set(entry["path"] as string, entry["index"] as number);
      }
      for (const path of ["text.txt", "old.txt", "bin.dat", "blob.bin", "link", "newdir", "moded.txt", "big.txt"]) {
        if (!changeIndexByPath.has(path)) {
          throw new Error(`missing change path ${path}; got ${JSON.stringify([...changeIndexByPath.keys()])}`);
        }
      }
    },
    180000,
  );

  test("f2. the terminal inspect loads the metadata; zero content GETs happen without a Review", async () => {
    const { harness } = await bootAndLoadChanges();
    expect(contentGets.length).toBe(0);
    expect(harness.element("content-review-panel").hidden).toBe(true);
    expect(harness.changesRows().length).toBeGreaterThan(0);
  });

  test("f3. the modified text file: the explicit Review loads both sides and renders the diff", async () => {
    const { harness } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "text.txt");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("available"), "text baseline");
    await waitFor(() => harness.element("content-result-status").textContent.includes("available"), "text result");
    expect(harness.element("content-baseline-view").textContent).toBe("TEXT-BASE\n");
    expect(harness.element("content-result-view").textContent).toBe("TEXT-CHANGED\n");
    expect(harness.element("content-diff-view").textContent).toContain("- TEXT-BASE");
    expect(harness.element("content-diff-view").textContent).toContain("+ TEXT-CHANGED");
    expect(harness.element("content-review-meta").textContent).toContain("text.txt");
  });

  test("f4. the added binary file renders through the bounded hex view", async () => {
    const { harness } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "blob.bin");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("empty side"), "binary baseline");
    await waitFor(() => harness.element("content-result-status").textContent.includes("available"), "binary result");
    const hex = harness.element("content-result-view").textContent;
    expect(hex).toContain("00000000");
    expect(hex).toContain("01 02 00");
  });

  test("f5. the modified binary file: the text baseline and the binary result refuse the diff", async () => {
    const { harness } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "bin.dat");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-result-status").textContent.includes("available"), "modified binary result");
    expect(harness.element("content-baseline-view").textContent).toBe("BINOLD\n");
    expect(harness.element("content-result-view").textContent).toContain("42 49 4e 00");
    expect(harness.element("content-diff-status").textContent).toContain("not UTF-8 text");
  });

  test("f6. the deleted file: the operator baseline side and the implicit-empty result diff", async () => {
    const { harness } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "old.txt");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("available"), "deleted baseline");
    await waitFor(() => harness.element("content-result-status").textContent.includes("empty side"), "deleted result");
    expect(harness.element("content-result-status").textContent).toContain("empty side");
    expect(harness.element("content-baseline-view").textContent).toBe("OLD\n");
    expect(harness.element("content-diff-view").textContent).toContain("- OLD");
  });

  test("f7. the retargeted symlink, the added directory and the file mode change", async () => {
    const { harness } = await bootAndLoadChanges();
    const linkRow = changesRowFor(harness, "link");
    reviewButton(harness, linkRow).click();
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("metadata only"), "symlink baseline");
    await waitFor(() => harness.element("content-result-status").textContent.includes("metadata only"), "symlink result");
    expect(harness.element("content-baseline-view").hidden).toBe(true);
    const dirRow = changesRowFor(harness, "newdir");
    reviewButton(harness, dirRow).click();
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("empty side"), "added directory baseline");
    await waitFor(() => harness.element("content-result-status").textContent.includes("metadata only"), "added directory result");
    expect(harness.element("content-diff-status").textContent).toContain("diff unavailable");
    const modeRow = changesRowFor(harness, "moded.txt");
    reviewButton(harness, modeRow).click();
    await waitFor(() => harness.element("content-result-status").textContent.includes("available"), "mode-only result");
    expect(harness.element("content-diff-status").textContent).toContain("Content identical; metadata changed.");
  });

  test("f8. the large file: two content chunks reassemble to exactly the one-shot full read", async () => {
    const { harness } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "big.txt");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("65536 of 102400"), "big baseline first chunk");
    await waitFor(() => harness.element("content-result-status").textContent.includes("65536 of 102400"), "big result first chunk");
    harness.click("content-review-load-result");
    await waitFor(() => harness.element("content-result-status").textContent.includes("102400 of 102400"), "big result fully loaded");
    harness.click("content-review-load-baseline");
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("102400 of 102400"), "big baseline fully loaded");
    const full = await contentFull(runId, "result", currentObservation, changeIndexByPath.get("big.txt")!);
    const rendered = harness.element("content-result-view").textContent;
    expect(rendered).toBe(full);
    expect(rendered.length).toBe(102400);
    // the diff of two ~1000-line files refuses inside the work budget
    expect(harness.element("content-diff-status").textContent).toContain("work budget");
  }, 30000);

  test("f9. the reads leave the run tree and the operator project byte-identical", async () => {
    const runTree = join(stateRoot, "pipeline-runs", runId);
    const runBefore = fingerprintTree(runTree);
    const projectBefore = fingerprintTree(projectDir);
    const { harness } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "text.txt");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-result-status").textContent.includes("available"), "fingerprint review");
    expect(fingerprintTree(runTree)).toBe(runBefore);
    expect(fingerprintTree(projectDir)).toBe(projectBefore);
  });

  test("f10. an operator change mid-review: the continuation is stale; Reload restores metadata without content", async () => {
    const { harness } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "big.txt");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-result-status").textContent.includes("65536 of 102400"), "first chunk");
    // the operator project diverges while the second chunk is pending
    writeFileSync(join(projectDir, "text.txt"), "TEXT-DIVERGED-OPERATOR\n", { mode: 0o644 });
    harness.click("content-review-load-result");
    await waitFor(() => harness.element("content-review-status").textContent.includes("Stale"), "stale continuation");
    expect(harness.element("content-result-view").hidden).toBe(true);
    expect(harness.element("content-baseline-view").hidden).toBe(true);
    expect(harness.element("content-review-reload-changes").hidden).toBe(false);
    const getsBefore = contentGets.length;
    harness.click("content-review-reload-changes");
    await waitForChanges(harness);
    // the reload is a changes-metadata GET; no content GET is re-issued
    expect(contentGets.length).toBe(getsBefore);
    const fresh = await fullChanges(runId);
    currentObservation = fresh["observation"] as string;
    const freshRow = changesRowFor(harness, "text.txt");
    reviewButton(harness, freshRow).click();
    await waitFor(() => harness.element("content-baseline-status").textContent.includes("no longer be proven"), "diverged baseline");
    expect(harness.element("content-baseline-view").hidden).toBe(true);
    await waitFor(() => harness.element("content-result-status").textContent.includes("available"), "diverged result");
    expect(harness.element("content-result-status").textContent).toContain("available");
  }, 30000);

  test("f11. the service restart reproduces the identical content document", async () => {
    const before = await contentFull(runId, "result", currentObservation, changeIndexByPath.get("big.txt")!);
    await service.stop();
    await startService();
    const after = await contentFull(runId, "result", currentObservation, changeIndexByPath.get("big.txt")!);
    expect(after).toBe(before);
  });

  test("f12. an invalid raw-byte cursor is the request-layer 400 envelope", async () => {
    const response = await rawRequest(port, "GET", `/v1/runs/${runId}/changes/1/content?side=result&observation=${currentObservation}&offset=99999999`);
    expect(response.status).toBe(400);
    const body = JSON.parse(response.body) as Record<string, unknown>;
    expect(body).toEqual({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" });
  });

  test("f14. the leakage scan: no host path, secret or canary in any served document", () => {
    const seen = rawBodies.join("\n");
    for (const secret of secrets) {
      expect(seen.includes(secret)).toBe(false);
    }
  });

  test("f13. the UI content region never POSTs", async () => {
    const { harness, posts } = await bootAndLoadChanges();
    const row = changesRowFor(harness, "bin.dat");
    reviewButton(harness, row).click();
    await waitFor(() => harness.element("content-result-status").textContent.includes("available"), "post-free review");
    expect(posts).toEqual([]);
    expect(contentGets.every((path) => path.includes("/content"))).toBe(true);
  });
});
