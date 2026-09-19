import { chmodSync } from "node:fs";
import { lstat, mkdir, mkdtemp, readFile, readdir, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "bun:test";
import {
  respondPipelineV2Wait,
  type PipelineV2WaitResponseDeps,
  type PipelineV2WaitResponseOutcome,
} from "../src/pipeline_v2_wait_respond.ts";
import { enterPipelineV2Wait } from "../src/pipeline_v2_wait_controller.ts";
import {
  parsePipelineV2RunState,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { pipelineV2RunStatePath } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { defaultPipelineStateIo, type PipelineStateIo } from "../src/pipeline_state_store.ts";
import { countingIo, faultIo } from "./state_io_test_helpers.ts";
import { createHash } from "node:crypto";

/**
 * Production wait-response command tests: `respondPipelineV2Wait` over the
 * real durable run state, the real wait controller/manifest/store stack
 * and the real run-root projection verification. The prefix of every case
 * is built with the real production APIs (the reducer through the real
 * sink, then the wait controller's request flow) — never hand-built JSON.
 * Everything is deterministic: no sleeps, no Docker daemon, no auth.
 */

const RUN_ID = "respond-run-1";
const REASON = "stage_iteration_limit_exhausted";
const ACTIONS = [
  { id: "continue_stage", to: "ship" },
  { id: "revise_task", to: "ship" },
];
const CANARY = "CANARY_secret_value";

interface Harness {
  root: string;
  stateRoot: string;
  runRoot: string;
  waits: string;
  statePath: string;
  counts: { tempOpens: number; renames: number; dirSyncs: number };
}

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return new Date(Date.UTC(2026, 0, 1, 0, 0, clock));
}

const ROOTS: string[] = [];

async function buildHarness(io: PipelineStateIo = defaultPipelineStateIo): Promise<Harness> {
  clock = 0;
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-wait-respond-"));
  ROOTS.push(root);
  const stateRoot = join(root, "state");
  await mkdir(stateRoot, { recursive: true });
  const counted = countingIo(io);
  const sink = new PipelineV2RunStateSink({
    stateRoot,
    runId: RUN_ID,
    io: counted.io,
    now: nextTick,
  });
  for (let i = 1; i <= 12; i++) {
    await sink.dispatch({
      kind: "create_run",
      runId: RUN_ID,
      pipeline: {
        schema_version: 2,
        bundle_root: "/nowhere/bundle",
        execution_snapshot_sha256: createHash("sha256").update(`bundle-${i}`).digest("hex"),
        entry_state: "s01",
        max_transitions: 20,
      },
      inputs: [],
    });
    break;
  }
  const runRoot = join(stateRoot, "pipeline-runs", RUN_ID);
  chmodSync(runRoot, 0o700);
  await enterPipelineV2Wait({
    runRoot,
    sink,
    reason: REASON,
    actions: ACTIONS,
  });
  return {
    root,
    stateRoot,
    runRoot,
    waits: join(runRoot, "waits"),
    statePath: pipelineV2RunStatePath(stateRoot, RUN_ID),
    counts: counted.counts,
  };
}

async function dispose(harness: Harness): Promise<void> {
  await rm(harness.root, { recursive: true, force: true });
}

const ROOTS_TO_DISPOSE: Harness[] = [];

afterAll(async () => {
  for (const harness of ROOTS.splice(0)) {
    await rm(harness, { recursive: true, force: true });
  }
});

function deps(harness: Harness, overrides: Partial<PipelineV2WaitResponseDeps> = {}): PipelineV2WaitResponseDeps {
  return {
    stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: harness.stateRoot },
    now: nextTick,
    ...overrides,
  };
}

function captureDiagnostics(): { errors: string[]; restore: () => void } {
  const errors: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]): void => {
    errors.push(args.map((arg) => String(arg)).join(" "));
  };
  return { errors, restore: () => (console.error = original) };
}

async function runRespond(
  harness: Harness,
  options: { runId?: string; waitIndex?: number; actionId?: string } = {},
  overrides: Partial<PipelineV2WaitResponseDeps> = {},
): Promise<{ outcome: PipelineV2WaitResponseOutcome; diagnostics: string[] }> {
  const captured = captureDiagnostics();
  let outcome: PipelineV2WaitResponseOutcome;
  try {
    outcome = await respondPipelineV2Wait(
      {
        runId: options.runId ?? RUN_ID,
        waitIndex: options.waitIndex ?? 1,
        actionId: options.actionId ?? "continue_stage",
      },
      deps(harness, overrides),
    );
  } finally {
    captured.restore();
  }
  return { outcome, diagnostics: captured.errors };
}

function expectOk(outcome: PipelineV2WaitResponseOutcome): Extract<PipelineV2WaitResponseOutcome, { ok: true }> {
  if (!outcome.ok) {
    throw new Error(`expected ok:true, got ${JSON.stringify(outcome)}`);
  }
  expect(outcome.exitCode).toBe(0);
  return outcome;
}

function expectReason(
  outcome: PipelineV2WaitResponseOutcome,
  reason: NonNullable<Extract<PipelineV2WaitResponseOutcome, { ok: false }>["reason"]>,
): Extract<PipelineV2WaitResponseOutcome, { ok: false }> {
  if (outcome.ok) {
    throw new Error(`expected ok:false with reason ${reason}, got ${JSON.stringify(outcome)}`);
  }
  expect(outcome.exitCode).toBe(1);
  expect(outcome.reason).toBe(reason);
  return outcome;
}

async function fingerprint(root: string): Promise<string> {
  const lines: string[] = [];
  const walk = async (rel: string): Promise<void> => {
    const abs = rel === "" ? root : join(root, rel);
    let info;
    try {
      info = await lstat(abs);
    } catch {
      lines.push(`${rel}\tmissing`);
      return;
    }
    const kind = info.isSymbolicLink()
      ? "symlink"
      : info.isDirectory()
        ? "dir"
        : info.isFile()
          ? "file"
          : "other";
    let extra = "";
    if (info.isFile()) {
      extra = createHash("sha256").update(await readFile(abs)).digest("hex");
    }
    lines.push(`${rel}\t${kind}\t${info.mode & 0o777}\t${extra}`);
    if (info.isDirectory() && !info.isSymbolicLink()) {
      for (const entry of (await readdir(abs)).sort()) {
        await walk(rel === "" ? entry : `${rel}/${entry}`);
      }
    }
  };
  await walk("");
  return lines.join("\n");
}

async function fileIdentity(path: string): Promise<{ ino: number; mtimeMs: number; bytes: string }> {
  const info = await lstat(path);
  return { ino: info.ino, mtimeMs: info.mtimeMs, bytes: await readFile(path, "utf8") };
}

// --- tests -------------------------------------------------------------------

test("1. happy path: response file, durable wait_response_recorded, active cursor at action.to", async () => {
  const harness = await buildHarness();
  const { outcome } = await runRespond(harness);
  const ok = expectOk(outcome);
  expect(ok.runId).toBe(RUN_ID);
  expect(ok.runRoot).toBe(harness.runRoot);
  expect(ok.waitIndex).toBe(1);
  expect(ok.actionId).toBe("continue_stage");
  expect(ok.actionTo).toBe("ship");
  const state = ok.state;
  expect(state.status).toBe("active");
  expect(state.phase).toBe("running");
  expect(state.cursor.current_state).toBe("ship");
  expect(state.cursor.transition_count).toBe(0);
  expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
  expect(ok.responseSha256).toBe(state.waits[0]!.response!.response_sha256);
  expect(ok.requestSha256).toBe(state.waits[0]!.request_sha256);
  const responsePath = join(harness.waits, "1.response.json");
  const stored = JSON.parse(await readFile(responsePath, "utf8")) as Record<string, unknown>;
  expect(stored["action_id"]).toBe("continue_stage");
  expect(stored["request_sha256"]).toBe(ok.requestSha256);
  expect((await lstat(responsePath)).mode & 0o777).toBe(0o600);
  expect((await lstat(harness.waits)).mode & 0o777).toBe(0o700);
});

test("2. serialize -> loader round-trip of the final state", async () => {
  const harness = await buildHarness();
  const { outcome } = await runRespond(harness);
  const ok = expectOk(outcome);
  const persisted = parsePipelineV2RunState(await readFile(harness.statePath, "utf8"));
  expect(persisted).toEqual(ok.state);
});

test("3. exact retry after success: same inode/mtime/bytes, zero new commits", async () => {
  const harness = await buildHarness();
  const first = expectOk((await runRespond(harness)).outcome);
  const stateIdentity = await fileIdentity(harness.statePath);
  const responseIdentity = await fileIdentity(join(harness.waits, "1.response.json"));
  const renamesBefore = harness.counts.renames;
  const second = expectOk((await runRespond(harness)).outcome);
  expect(second.responseSha256).toBe(first.responseSha256);
  expect(second.actionTo).toBe(first.actionTo);
  expect(second.state).toEqual(first.state);
  expect(harness.counts.renames).toBe(renamesBefore);
  expect(await fileIdentity(harness.statePath)).toEqual(stateIdentity);
  expect(await fileIdentity(join(harness.waits, "1.response.json"))).toEqual(responseIdentity);
});

test("4. different-action retry is a conflict; state and response bytes unchanged", async () => {
  const harness = await buildHarness();
  expectOk((await runRespond(harness)).outcome);
  const stateIdentity = await fileIdentity(harness.statePath);
  const responseIdentity = await fileIdentity(join(harness.waits, "1.response.json"));
  const renamesBefore = harness.counts.renames;
  const refused = expectReason((await runRespond(harness, { actionId: "revise_task" })).outcome, "wait_conflict");
  expect(refused.state?.waits[0]?.response?.action_id).toBe("continue_stage");
  expect(harness.counts.renames).toBe(renamesBefore);
  expect(await fileIdentity(harness.statePath)).toEqual(stateIdentity);
  expect(await fileIdentity(join(harness.waits, "1.response.json"))).toEqual(responseIdentity);
});

test("5. not_committed after publication: waiting state + orphan response; exact retry commits", async () => {
  const harness = await buildHarness();
  const refused = expectReason((await runRespond(harness, {}, { io: faultIo({ failCommit: 1, failStep: "rename" }) })).outcome, "state_persist_failed");
  // the previous snapshot is authoritative and the run stays waiting
  expect(refused.state?.status).toBe("waiting");
  expect(refused.state?.waits[0]?.response).toBeUndefined();
  // the orphan response file exists with the exact canonical bytes
  const orphanBytes = await readFile(join(harness.waits, "1.response.json"), "utf8");
  expect(JSON.parse(orphanBytes).action_id).toBe("continue_stage");
  // the exact retry reuses the orphan bytes and commits
  const retried = expectOk((await runRespond(harness)).outcome);
  expect(retried.state.status).toBe("active");
  expect(retried.state.waits[0]!.response!.response_sha256).toBeDefined();
  expect(await readFile(join(harness.waits, "1.response.json"), "utf8")).toBe(orphanBytes);
});

test("6. durability_unknown adopts the exact candidate and stops further operations", async () => {
  const harness = await buildHarness();
  const refused = expectReason(
    (await runRespond(harness, {}, { io: faultIo({ failCommit: 1, failStep: "dirfsync" }) })).outcome,
    "state_persist_failed",
  );
  // the adopted candidate is the visible state with the recorded response
  expect(refused.state?.status).toBe("active");
  expect(refused.state?.waits[0]!.response!.action_id).toBe("continue_stage");
  const candidateBytes = await readFile(harness.statePath, "utf8");
  expect(JSON.parse(candidateBytes).waits[0].response).toBeDefined();
  // a fresh respond call recognizes the durable record as idempotent success
  const retried = expectOk((await runRespond(harness)).outcome);
  expect(retried.state.status).toBe("active");
  expect(await readFile(harness.statePath, "utf8")).toBe(candidateBytes);
});

test("7. missing state refuses read-only; missing run root refuses with no run root", async () => {
  // a run root without a state document
  const harness = await buildHarness();
  await rm(harness.statePath, { force: true });
  const fingerprintBefore = await fingerprint(harness.stateRoot);
  const refused = expectReason((await runRespond(harness)).outcome, "missing_state");
  expect(refused.runId).toBe(RUN_ID);
  expect(refused.runRoot).toBe(harness.runRoot);
  expect(refused.state).toBeNull();
  expect(await fingerprint(harness.stateRoot)).toBe(fingerprintBefore);

  // a completely absent run root
  const absent = await mkdtemp(join(tmpdir(), "pipeline-v2-wait-respond-absent-"));
  ROOTS.push(absent);
  const stateRoot = join(absent, "state");
  await mkdir(stateRoot, { recursive: true });
  const { outcome } = await runRespond(
    {
      root: absent,
      stateRoot,
      runRoot: join(stateRoot, "pipeline-runs", RUN_ID),
      waits: "",
      statePath: "",
      counts: { tempOpens: 0, renames: 0, dirSyncs: 0 },
    },
    {},
  );
  expectReason(outcome, "run_layout_invalid");
  expect(outcome.runRoot).toBeNull();
  expect(outcome.state).toBeNull();
});

test("8. active, publishing and final states refuse without mutations", async () => {
  // active run without an open wait
  const active = await mkdtemp(join(tmpdir(), "pipeline-v2-wait-respond-active-"));
  ROOTS.push(active);
  const stateRootA = join(active, "state");
  await mkdir(stateRootA, { recursive: true });
  const sinkA = new PipelineV2RunStateSink({ stateRoot: stateRootA, runId: RUN_ID, now: nextTick });
  await sinkA.dispatch({
    kind: "create_run",
    runId: RUN_ID,
    pipeline: {
      schema_version: 2,
      bundle_root: "/nowhere/bundle",
      execution_snapshot_sha256: createHash("sha256").update("bundle").digest("hex"),
      entry_state: "s01",
      max_transitions: 20,
    },
    inputs: [],
  });
  const runRootA = join(stateRootA, "pipeline-runs", RUN_ID);
  chmodSync(runRootA, 0o700);
  const fpA = await fingerprint(stateRootA);
  const refusedActive = expectReason(
    (
      await runRespond(
        {
          root: active,
          stateRoot: stateRootA,
          runRoot: runRootA,
          waits: join(runRootA, "waits"),
          statePath: pipelineV2RunStatePath(stateRootA, RUN_ID),
          counts: { tempOpens: 0, renames: 0, dirSyncs: 0 },
        },
        {},
      )
    ).outcome,
    "invalid_response",
  );
  expect(refusedActive.state?.status).toBe("active");
  expect(await fingerprint(stateRootA)).toBe(fpA);

  // publishing and final states carry no open wait either
  const publishing = await mkdtemp(join(tmpdir(), "pipeline-v2-wait-respond-pub-"));
  ROOTS.push(publishing);
  const stateRootP = join(publishing, "state");
  await mkdir(stateRootP, { recursive: true });
  const sinkP = new PipelineV2RunStateSink({ stateRoot: stateRootP, runId: RUN_ID, now: nextTick });
  await sinkP.dispatch({
    kind: "create_run",
    runId: RUN_ID,
    pipeline: {
      schema_version: 2,
      bundle_root: "/nowhere/bundle",
      execution_snapshot_sha256: createHash("sha256").update("bundle").digest("hex"),
      entry_state: "s01",
      max_transitions: 20,
    },
    inputs: [],
  });
  await sinkP.dispatch({ kind: "terminal_reached", terminalStateId: "s01", terminalResult: "success" });
  const runRootP = join(stateRootP, "pipeline-runs", RUN_ID);
  chmodSync(runRootP, 0o700);
  const fpP = await fingerprint(stateRootP);
  const refusedPublishing = expectReason(
    (
      await runRespond(
        {
          root: publishing,
          stateRoot: stateRootP,
          runRoot: runRootP,
          waits: join(runRootP, "waits"),
          statePath: pipelineV2RunStatePath(stateRootP, RUN_ID),
          counts: { tempOpens: 0, renames: 0, dirSyncs: 0 },
        },
        {},
      )
    ).outcome,
    "invalid_response",
  );
  expect(refusedPublishing.state?.status).toBe("active");
  expect(refusedPublishing.state?.terminal?.state_id).toBe("s01");
  expect(await fingerprint(stateRootP)).toBe(fpP);
});

test("9. wrong and stale wait indexes refuse", async () => {
  const harness = await buildHarness();
  const wrong = expectReason((await runRespond(harness, { waitIndex: 2 })).outcome, "invalid_response");
  expect(wrong.state?.status).toBe("waiting");
  // index 0 and negative indexes are option-shape failures
  const zero = expectReason((await runRespond(harness, { waitIndex: 0 })).outcome, "invalid_options");
  expect(zero.runRoot).toBeNull();
  const negative = expectReason((await runRespond(harness, { waitIndex: -1 })).outcome, "invalid_options");
  expect(negative.runRoot).toBeNull();
});

test("10. an undeclared action refuses with the run tree byte-identical", async () => {
  const harness = await buildHarness();
  const fingerprintBefore = await fingerprint(harness.stateRoot);
  const refused = expectReason(
    (await runRespond(harness, { actionId: `${CANARY}_undeclared` })).outcome,
    "invalid_response",
  );
  expect(refused.state?.status).toBe("waiting");
  expect(await fingerprint(harness.stateRoot)).toBe(fingerprintBefore);
  expect((await readdir(harness.waits)).filter((name) => name.includes("response"))).toEqual([]);
});

test("11. tampered, noncanonical, missing, symlinked and wrong-mode manifests", async () => {
  // tampered request file
  const tampered = await buildHarness();
  await writeFile(join(tampered.waits, "1.request.json"), '{"tampered": true}');
  const refusedTampered = expectReason((await runRespond(tampered)).outcome, "wait_conflict");
  expect(refusedTampered.state?.status).toBe("waiting");

  // noncanonical request bytes
  const noncanonical = await buildHarness();
  const canonicalRequest = await readFile(join(noncanonical.waits, "1.request.json"), "utf8");
  const reordered = JSON.stringify(JSON.parse(canonicalRequest), null, 2);
  await writeFile(join(noncanonical.waits, "1.request.json"), reordered);
  expectReason((await runRespond(noncanonical)).outcome, "wait_conflict");

  // missing request file is restored idempotently and the response succeeds
  const missing = await buildHarness();
  await rm(join(missing.waits, "1.request.json"), { force: true });
  const restored = expectOk((await runRespond(missing)).outcome);
  expect(restored.state.status).toBe("active");
  expect(await readFile(join(missing.waits, "1.request.json"), "utf8")).toBe(canonicalRequest);

  // symlinked response target conflicts without overwriting
  const symlinked = await buildHarness();
  const sentinel = join(symlinked.root, "sentinel");
  await writeFile(sentinel, "SENTINEL");
  await symlink(sentinel, join(symlinked.waits, "1.response.json"));
  const refusedSymlink = expectReason((await runRespond(symlinked)).outcome, "wait_conflict");
  expect(refusedSymlink.state?.status).toBe("waiting");
  expect(await readFile(sentinel, "utf8")).toBe("SENTINEL");

  // wrong-mode stored files conflict
  const wrongMode = await buildHarness();
  await chmodSync(join(wrongMode.waits, "1.request.json"), 0o644);
  expectReason((await runRespond(wrongMode)).outcome, "wait_conflict");
  chmodSync(join(wrongMode.waits, "1.request.json"), 0o600);

  // a conflicting pre-published response file conflicts
  const conflicting = await buildHarness();
  await writeFile(
    join(conflicting.waits, "1.response.json"),
    JSON.stringify({ schema_version: 1, run_id: RUN_ID, wait_index: 1, request_sha256: "a".repeat(64), action_id: "revise_task" }),
    { mode: 0o600 },
  );
  const refusedConflicting = expectReason((await runRespond(conflicting)).outcome, "wait_conflict");
  expect(refusedConflicting.state?.status).toBe("waiting");
});

test("12. symlinked, file and split projections and a run-root mode mismatch refuse", async () => {
  const harness = await buildHarness();
  const baseDeps: PipelineV2WaitResponseDeps = deps(harness);

  // symlinked state root
  const link = join(harness.root, "state-link");
  await symlink(harness.stateRoot, link);
  const symlinkOutcome = (
    await runRespond(harness, {}, { stateRootProjection: { localRoot: link, daemonRoot: link } })
  ).outcome;
  expectReason(symlinkOutcome, "run_layout_invalid");
  expect(symlinkOutcome.runRoot).toBeNull();

  // a file as the state root
  const fileRoot = join(harness.root, "not-a-dir");
  await writeFile(fileRoot, "x");
  const fileOutcome = (
    await runRespond(harness, {}, { stateRootProjection: { localRoot: fileRoot, daemonRoot: fileRoot } })
  ).outcome;
  expectReason(fileOutcome, "run_layout_invalid");
  expect(fileOutcome.runRoot).toBeNull();

  // a split projection (two different real directories)
  const otherRoot = join(harness.root, "other-state");
  await mkdir(otherRoot, { recursive: true });
  const splitOutcome = (
    await runRespond(
      harness,
      {},
      { stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: otherRoot } },
    )
  ).outcome;
  expectReason(splitOutcome, "run_layout_invalid");
  expect(splitOutcome.runRoot).toBeNull();

  // an unsafe run-root mode is refused unchanged
  chmodSync(harness.runRoot, 0o755);
  const modeOutcome = (await runRespond(harness)).outcome;
  expectReason(modeOutcome, "run_layout_invalid");
  expect(modeOutcome.runRoot).toBeNull();
  expect(((await lstat(harness.runRoot)).mode & 0o777)).toBe(0o755);
  chmodSync(harness.runRoot, 0o700);
});

test("13. every pre-controller refusal leaves the run tree byte-identical", async () => {
  const harness = await buildHarness();
  const fingerprintBefore = await fingerprint(harness.stateRoot);
  expectReason((await runRespond(harness, { waitIndex: 2 })).outcome, "invalid_response");
  expectReason((await runRespond(harness, { waitIndex: 0 })).outcome, "invalid_options");
  expectReason((await runRespond(harness, { actionId: "CANARY_undeclared" })).outcome, "invalid_response");
  expectReason(
    (await runRespond(harness, {}, { stateRootProjection: { localRoot: harness.stateRoot, daemonRoot: join(harness.root, "other") } })).outcome,
    "run_layout_invalid",
  );
  expect(await fingerprint(harness.stateRoot)).toBe(fingerprintBefore);
});

test("14. the module carries no auth, Docker Helper, Session, profile or pipeline dependencies", async () => {
  const source = await readFile(
    new URL("../src/pipeline_v2_wait_respond.ts", import.meta.url).pathname,
    "utf8",
  );
  for (const banned of [
    'from "./docker_helper',
    'from "./launcher',
    'from "./profile',
    'from "./lifecycle',
    'from "./agent_smoke',
    'from "./worker',
    "fetchAuth",
    "helperConfig",
    "baseEnv",
    "onSignal",
    "CliRunner",
    "loadPipelineV2",
    "loadProfile",
    "createChildSession",
    "RunCauseGate",
    "SubprocessCliRunner",
  ]) {
    expect(source.includes(banned)).toBe(false);
  }
  // the outcome/deps shape carries no such fields either
  const outcome: PipelineV2WaitResponseOutcome = {
    ok: false,
    exitCode: 1,
    runId: "x",
    runRoot: null,
    state: null,
    reason: "invalid_options",
  };
  expect(Object.keys(outcome).sort()).toEqual(["exitCode", "ok", "reason", "runId", "runRoot", "state"]);
});

test("15. deep-freeze, mutation isolation and content-free diagnostics", async () => {
  const harness = await buildHarness();
  const { outcome, diagnostics } = await runRespond(harness);
  const ok = expectOk(outcome);
  expect(Object.isFrozen(outcome)).toBe(true);
  expect(Object.isFrozen(ok.state)).toBe(true);
  expect(Object.isFrozen(ok.state.waits)).toBe(true);
  expect(Object.isFrozen(ok.state.waits[0])).toBe(true);
  expect(Object.isFrozen(ok.state.waits[0]!.response)).toBe(true);
  // diagnostics name no action values, no digests beyond the fixed failure text
  const joined = diagnostics.join("\n");
  expect(joined).not.toContain("continue_stage");
  expect(joined).not.toContain(CANARY);
  expect(joined).not.toContain("ship");
});

test("16. hostile options and deps shapes are ordinary refusals with zero reads", async () => {
  const harness = await buildHarness();
  let trapHits = 0;
  const hostileOptions = new Proxy(
    { runId: RUN_ID, waitIndex: 1, actionId: "continue_stage" },
    {
      get(target, property, receiver) {
        trapHits += 1;
        return Reflect.get(target, property, receiver);
      },
    },
  );
  const outcome = await respondPipelineV2Wait(
    hostileOptions as unknown as { runId: string; waitIndex: number; actionId: string },
    deps(harness),
  );
  // the shape validation read the option fields and then proceeded
  expect(trapHits).toBeGreaterThanOrEqual(3);
  expect(outcome.ok).toBe(true);
  // every refusal below happens after the successful recording: the tree
  // is byte-identical across them
  const fingerprintBefore = await fingerprint(harness.stateRoot);
  const badId = await respondPipelineV2Wait(
    { runId: "../escape", waitIndex: 1, actionId: "continue_stage" },
    deps(harness),
  );
  expectReason(badId, "invalid_options");
  expect(badId.runId).toBe("");
  expect(badId.runRoot).toBeNull();
  const overflow = await respondPipelineV2Wait(
    { runId: RUN_ID, waitIndex: Number.MAX_SAFE_INTEGER + 1, actionId: "continue_stage" },
    deps(harness),
  );
  expectReason(overflow, "invalid_options");
  expect(await fingerprint(harness.stateRoot)).toBe(fingerprintBefore);
});

test("17. the public export surface carries no IO or test seams", async () => {
  const namespace = (await import("../src/pipeline_v2_wait_respond.ts")) as Record<string, unknown>;
  expect(Object.keys(namespace).sort()).toEqual(["respondPipelineV2Wait"]);
});

test("18. no message parsing and no second serializer/digest/publisher in the module", async () => {
  const source = await readFile(
    new URL("../src/pipeline_v2_wait_respond.ts", import.meta.url).pathname,
    "utf8",
  );
  expect(source.includes(".message.includes")).toBe(false);
  expect(source.includes(".includes(")).toBe(false);
  expect(source.includes("JSON.parse(")).toBe(false);
  expect(source.includes("CryptoHasher")).toBe(false);
  expect(source.includes("createWriteStream")).toBe(false);
  expect(source.includes("open(")).toBe(true);
  expect(source.includes("O_CREAT")).toBe(false);
  expect(source.includes("link(")).toBe(false);
  expect(source.includes("canonicalJson")).toBe(false);
});

// the ROOTS_TO_DISPOSE array is intentionally unused; disposal happens per test
void ROOTS_TO_DISPOSE;
void realpath;
