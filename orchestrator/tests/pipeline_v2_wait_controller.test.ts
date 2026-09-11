import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  parsePipelineV2RunState,
  type PipelineV2RunCommand,
  type PipelineV2RunState,
} from "../src/pipeline_v2_state.ts";
import { defaultPipelineStateIo, type PipelineStateIo } from "../src/run_snapshot_store.ts";
import { pipelineV2RunStatePath } from "../src/pipeline_v2_state_store.ts";
import { PipelineV2RunStateSink } from "../src/pipeline_v2_state_sink.ts";
import { hex, successCommands, tick } from "./pipeline_v2_state_fixtures.ts";
import { countingIo, faultIo, type IoCounts } from "./state_io_test_helpers.ts";
import {
  enterPipelineV2Wait,
  PipelineV2WaitControllerError,
  recordPipelineV2WaitResponse,
  type EnterPipelineV2WaitOptions,
  type PipelineV2WaitControllerFailureReason,
  type PipelineV2WaitControllerSink,
  type RecordedPipelineV2WaitResponse,
} from "../src/pipeline_v2_wait_controller.ts";
import { preparePipelineV2WaitRequest } from "../src/pipeline_v2_wait_manifest.ts";

const RUN_ID = "run-1";
const REASON = "stage_iteration_limit_exhausted";
const OTHER_REASON = "different_policy_reason";
const ACTIONS = [
  { id: "continue_stage", to: "ship" },
  { id: "revise_task", to: "ship" },
];
const CANARY = "CANARY_secret_value";

interface Ctx {
  root: string;
  stateRoot: string;
  runRoot: string;
  waits: string;
  sink: PipelineV2RunStateSink;
  counts: IoCounts;
  statePath: string;
}

let clock = 0;
function nextTick(): Date {
  clock += 1;
  return tick(clock - 1);
}

async function setup(io: PipelineStateIo = defaultPipelineStateIo): Promise<Ctx> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-wait-controller-"));
  const stateRoot = join(root, "state-root");
  await mkdir(stateRoot, { recursive: true });
  const runRoot = join(root, "runs", RUN_ID);
  await mkdir(runRoot, { mode: 0o700, recursive: true });
  const counted = countingIo(io);
  const sink = new PipelineV2RunStateSink({
    stateRoot,
    runId: RUN_ID,
    io: counted.io,
    now: nextTick,
  });
  for (const command of successCommands(RUN_ID).slice(0, 12)) {
    await sink.dispatch(command);
  }
  return {
    root,
    stateRoot,
    runRoot,
    waits: join(runRoot, "waits"),
    sink,
    counts: counted.counts,
    statePath: pipelineV2RunStatePath(stateRoot, RUN_ID),
  };
}

async function dispose(ctx: Ctx): Promise<void> {
  await rm(ctx.root, { recursive: true, force: true });
}

function expectControllerError(
  cause: unknown,
  reason: PipelineV2WaitControllerFailureReason,
  operation: "enter_wait" | "record_response",
): PipelineV2WaitControllerError {
  expect(cause).toBeInstanceOf(PipelineV2WaitControllerError);
  const error = cause as PipelineV2WaitControllerError;
  expect(error.reason).toBe(reason);
  expect(error.operation).toBe(operation);
  return error;
}

function requestFileBytes(state: PipelineV2RunState, waitIndex: number): string {
  const record = state.waits.find((candidate) => candidate.index === waitIndex);
  if (record === undefined) {
    throw new Error(`no wait record ${waitIndex}`);
  }
  return preparePipelineV2WaitRequest({
    schema_version: 1,
    run_id: state.run_id,
    wait_index: record.index,
    transition_count: record.transition_count,
    state_id: record.state_id,
    reason: record.reason,
    actions: record.actions,
  }).canonical_json;
}

function responseRaw(
  state: PipelineV2RunState,
  waitIndex: number,
  actionId: string,
  overrides: Record<string, unknown> = {},
): string {
  const record = state.waits.find((candidate) => candidate.index === waitIndex);
  if (record === undefined) {
    throw new Error(`no wait record ${waitIndex}`);
  }
  return JSON.stringify({
    schema_version: 1,
    run_id: state.run_id,
    wait_index: waitIndex,
    request_sha256: record.request_sha256,
    action_id: actionId,
    ...overrides,
  });
}

async function fileIdentity(
  path: string,
): Promise<{ ino: number; mtimeMs: number; bytes: string }> {
  const info = await stat(path);
  return { ino: info.ino, mtimeMs: info.mtimeMs, bytes: await readFile(path, "utf8") };
}

function barrierSink(real: PipelineV2RunStateSink): PipelineV2WaitControllerSink {
  let arrived = 0;
  let release: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  return {
    get snapshot() {
      return real.snapshot;
    },
    get poisoned() {
      return real.poisoned;
    },
    dispatch: async (command: PipelineV2RunCommand) => {
      arrived += 1;
      if (arrived === 2) {
        release();
      }
      await gate;
      return await real.dispatch(command);
    },
  };
}

test("1. request happy path: derived manifest, publication, run_waiting, round-trip", async () => {
  const ctx = await setup();
  try {
    const result = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(result.wait_index).toBe(1);
    expect(result.state).toBe(state);
    expect(state.status).toBe("waiting");
    expect(state.revision).toBe(13);
    const record = state.waits[0];
    expect(record?.index).toBe(1);
    expect(record?.transition_count).toBe(2);
    expect(record?.state_id).toBe("ship");
    expect(record?.reason).toBe(REASON);
    expect(record?.response).toBeUndefined();
    expect(result.request_sha256).toBe(record!.request_sha256);
    expect(record?.actions).toEqual(ACTIONS);
    expect(await readFile(join(ctx.waits, "1.request.json"), "utf8")).toBe(
      requestFileBytes(state, 1),
    );
    expect(parsePipelineV2RunState(await readFile(ctx.statePath, "utf8"))).toEqual(state);
  } finally {
    await dispose(ctx);
  }
});

test("2. response happy path: durable request, self-loaded request, response publication", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const result = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    });
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(result.wait_index).toBe(1);
    expect(result.action_id).toBe("continue_stage");
    expect(result.action_to).toBe("ship");
    expect(result.state).toBe(state);
    expect(state.status).toBe("active");
    expect(state.phase).toBe("running");
    expect(state.cursor.current_state).toBe("ship");
    expect(state.cursor.transition_count).toBe(2);
    expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
    expect(result.response_sha256).toBe(state.waits[0]!.response!.response_sha256);
    expect(result.request_sha256).toBe(state.waits[0]!.request_sha256);
    const responsePath = join(ctx.waits, "1.response.json");
    const storedResponse = JSON.parse(await readFile(responsePath, "utf8")) as Record<string, unknown>;
    expect(storedResponse["action_id"]).toBe("continue_stage");
    expect(storedResponse["request_sha256"]).toBe(state.waits[0]?.request_sha256);
    expect((await stat(responsePath)).mode & 0o777).toBe(0o600);
    expect(parsePipelineV2RunState(await readFile(ctx.statePath, "utf8"))).toEqual(state);
  } finally {
    await dispose(ctx);
  }
});

test("3. the caller cannot inject cursor, run, index or target values", async () => {
  const ctx = await setup();
  try {
    const hostile = {
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
      run_id: "other-run",
      wait_index: 99,
      transition_count: 999,
      state_id: "elsewhere",
      target: "hijacked",
      request_sha256: hex("f"),
    } as unknown as EnterPipelineV2WaitOptions;
    const result = await enterPipelineV2Wait(hostile);
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(result.wait_index).toBe(1);
    expect(state.waits[0]?.state_id).toBe("ship");
    expect(state.waits[0]?.transition_count).toBe(2);
    expect(state.run_id).toBe(RUN_ID);
  } finally {
    await dispose(ctx);
  }
});

test("4. a request publication failure dispatches nothing and keeps the state", async () => {
  const ctx = await setup();
  try {
    const blockedParent = join(ctx.root, "blocked");
    await mkdir(blockedParent, { mode: 0o700 });
    const blocked = join(blockedParent, RUN_ID);
    await writeFile(blocked, "not a directory\n");
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const renamesBefore = ctx.counts.renames;
    const cause = await enterPipelineV2Wait({
      runRoot: blocked,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(cause, "wait_storage_failed", "enter_wait");
    expect(ctx.sink.snapshot?.revision).toBe(12);
    expect(ctx.counts.renames).toBe(renamesBefore);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
  } finally {
    await dispose(ctx);
  }
});

test("5. a conflicting pre-published request target conflicts before any dispatch", async () => {
  const ctx = await setup();
  try {
    const other = preparePipelineV2WaitRequest({
      schema_version: 1,
      run_id: RUN_ID,
      wait_index: 1,
      transition_count: 2,
      state_id: "ship",
      reason: OTHER_REASON,
      actions: ACTIONS,
    });
    await mkdir(ctx.waits, { mode: 0o700 });
    await writeFile(join(ctx.waits, "1.request.json"), other.canonical_json, { mode: 0o600 });
    const renamesBefore = ctx.counts.renames;
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const cause = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(cause, "wait_conflict", "enter_wait");
    expect(ctx.counts.renames).toBe(renamesBefore);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
    expect(await readFile(join(ctx.waits, "1.request.json"), "utf8")).toBe(other.canonical_json);
  } finally {
    await dispose(ctx);
  }
});

test("6. a not_committed run_waiting leaves the orphan and the previous state", async () => {
  const ctx = await setup(faultIo({ failCommit: 13 }));
  try {
    const cause = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(cause, "state_persist_failed", "enter_wait");
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.revision).toBe(12);
    expect(state.status).toBe("active");
    expect(state.waits).toEqual([]);
    const orphan = await readFile(join(ctx.waits, "1.request.json"), "utf8");
    expect(orphan).toBe(
      preparePipelineV2WaitRequest({
        schema_version: 1,
        run_id: RUN_ID,
        wait_index: 1,
        transition_count: 2,
        state_id: "ship",
        reason: REASON,
        actions: ACTIONS,
      }).canonical_json,
    );
    expect(parsePipelineV2RunState(await readFile(ctx.statePath, "utf8"))).toEqual(state);
  } finally {
    await dispose(ctx);
  }
});

test("7. the exact retry after not_committed reuses the orphan bytes and commits", async () => {
  const ctx = await setup(faultIo({ failCommit: 13 }));
  try {
    const first = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(first, "state_persist_failed", "enter_wait");
    const orphan = await fileIdentity(join(ctx.waits, "1.request.json"));
    const options = {
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    };
    const result = await enterPipelineV2Wait(options);
    const after = await fileIdentity(join(ctx.waits, "1.request.json"));
    expect(after.ino).toBe(orphan.ino);
    expect(after.mtimeMs).toBe(orphan.mtimeMs);
    expect(after.bytes).toBe(orphan.bytes);
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.revision).toBe(13);
    expect(state.status).toBe("waiting");
    expect(result.request_sha256).toBe(state.waits[0]!.request_sha256);
  } finally {
    await dispose(ctx);
  }
});

test("8. a durability-unknown run_waiting adopts the candidate and dispatches nothing more", async () => {
  const ctx = await setup(faultIo({ failCommit: 13, failStep: "dirfsync" }));
  try {
    const cause = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    const error = expectControllerError(cause, "state_persist_failed", "enter_wait");
    expect(ctx.sink.poisoned).toBe(true);
    const candidate = ctx.sink.snapshot as PipelineV2RunState;
    expect(error.state).toBe(candidate);
    expect(candidate.status).toBe("waiting");
    expect(candidate.waits[0]?.request_sha256).toBeDefined();
    const renamesAfterFailure = ctx.counts.renames;
    const emptyRunRoot = join(ctx.root, "runs", "fresh");
    await mkdir(emptyRunRoot, { mode: 0o700 });
    const again = await enterPipelineV2Wait({
      runRoot: emptyRunRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(again, "invalid_state", "enter_wait");
    expect(ctx.counts.renames).toBe(renamesAfterFailure);
    expect(await readdir(emptyRunRoot)).toEqual([]);
  } finally {
    await dispose(ctx);
  }
});

test("9. a repeat against the durable open wait succeeds without a second command", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const stateFileBefore = await readFile(ctx.statePath, "utf8");
    const fileBefore = await fileIdentity(join(ctx.waits, "1.request.json"));
    const renamesBefore = ctx.counts.renames;
    const result = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    expect(result.wait_index).toBe(1);
    expect(result.state.revision).toBe(13);
    expect(ctx.counts.renames).toBe(renamesBefore);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateFileBefore);
    const fileAfter = await fileIdentity(join(ctx.waits, "1.request.json"));
    expect(fileAfter.ino).toBe(fileBefore.ino);
    expect(fileAfter.mtimeMs).toBe(fileBefore.mtimeMs);
  } finally {
    await dispose(ctx);
  }
});

test("10. a different request against the open wait is a conflict without writes", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const fileBefore = await fileIdentity(join(ctx.waits, "1.request.json"));
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const cause = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: OTHER_REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(cause, "wait_conflict", "enter_wait");
    const fileAfter = await fileIdentity(join(ctx.waits, "1.request.json"));
    expect(fileAfter.ino).toBe(fileBefore.ino);
    expect(fileAfter.bytes).toBe(fileBefore.bytes);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
  } finally {
    await dispose(ctx);
  }
});

test("11. a tampered request file forbids the response publication", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const requestPath = join(ctx.waits, "1.request.json");
    await unlink(requestPath);
    await writeFile(requestPath, requestFileBytes(waiting, 1).replace("stage_iteration", "tampered_x"), {
      mode: 0o600,
    });
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const cause = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    }).catch((error) => error);
    expectControllerError(cause, "wait_conflict", "record_response");
    expect(ctx.sink.snapshot?.status).toBe("waiting");
    expect(ctx.counts.renames).toBe(13);
    expect(await readdir(ctx.waits).then((names) => names.filter((n) => n.endsWith(".response.json")))).toEqual(
      [],
    );
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
  } finally {
    await dispose(ctx);
  }
});

test("12. a missing request file is restored before the response is recorded", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    await unlink(join(ctx.waits, "1.request.json"));
    const result = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    });
    expect(result.action_to).toBe("ship");
    expect(await readFile(join(ctx.waits, "1.request.json"), "utf8")).toBe(
      requestFileBytes(waiting, 1),
    );
    expect((await stat(join(ctx.waits, "1.request.json"))).mode & 0o777).toBe(0o600);
    expect(ctx.sink.snapshot?.status).toBe("active");
  } finally {
    await dispose(ctx);
  }
});

test("13. malformed, undeclared and foreign responses are rejected before publication", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const renamesBefore = ctx.counts.renames;
    const malformed = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: "{not json",
    }).catch((error) => error);
    expectControllerError(malformed, "invalid_response", "record_response");
    const undeclared = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "undeclared_action"),
    }).catch((error) => error);
    expectControllerError(undeclared, "invalid_response", "record_response");
    const foreign = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage", { request_sha256: hex("f") }),
    }).catch((error) => error);
    expectControllerError(foreign, "invalid_response", "record_response");
    const unknown = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 7,
      raw: "{}",
    }).catch((error) => error);
    expectControllerError(unknown, "invalid_response", "record_response");
    expect(ctx.sink.snapshot?.status).toBe("waiting");
    expect(ctx.counts.renames).toBe(renamesBefore);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
    expect(
      await readdir(ctx.waits).then((names) => names.filter((n) => n.endsWith(".response.json"))),
    ).toEqual([]);
  } finally {
    await dispose(ctx);
  }
});

test("14. a response publication conflict keeps the run waiting without dispatch", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    await mkdir(join(ctx.waits, "1.response.json"));
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const renamesBefore = ctx.counts.renames;
    const cause = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    }).catch((error) => error);
    expectControllerError(cause, "wait_conflict", "record_response");
    expect(ctx.sink.snapshot?.status).toBe("waiting");
    expect(ctx.counts.renames).toBe(renamesBefore);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
  } finally {
    await dispose(ctx);
  }
});

test("15. a not_committed response leaves the orphan and the exact retry commits", async () => {
  const ctx = await setup(faultIo({ failCommit: 14 }));
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const raw = responseRaw(waiting, 1, "continue_stage");
    const options = { runRoot: ctx.runRoot, sink: ctx.sink, waitIndex: 1, raw };
    const cause = await recordPipelineV2WaitResponse(options).catch((error) => error);
    expectControllerError(cause, "state_persist_failed", "record_response");
    expect(ctx.sink.snapshot?.status).toBe("waiting");
    expect(ctx.sink.snapshot?.revision).toBe(13);
    const orphan = await fileIdentity(join(ctx.waits, "1.response.json"));
    const result = await recordPipelineV2WaitResponse(options);
    const after = await fileIdentity(join(ctx.waits, "1.response.json"));
    expect(after.ino).toBe(orphan.ino);
    expect(after.bytes).toBe(orphan.bytes);
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.revision).toBe(14);
    expect(state.status).toBe("active");
    expect(state.waits[0]?.response?.response_sha256).toBe(result.response_sha256);
  } finally {
    await dispose(ctx);
  }
});

test("16. a durability-unknown response adopts the recorded candidate", async () => {
  const ctx = await setup(faultIo({ failCommit: 14, failStep: "dirfsync" }));
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const cause = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    }).catch((error) => error);
    const error = expectControllerError(cause, "state_persist_failed", "record_response");
    expect(ctx.sink.poisoned).toBe(true);
    const candidate = ctx.sink.snapshot as PipelineV2RunState;
    expect(error.state).toBe(candidate);
    expect(candidate.status).toBe("active");
    expect(candidate.waits[0]?.response?.action_id).toBe("continue_stage");
    const renames = ctx.counts.renames;
    const again = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    }).catch((retryError) => retryError);
    expectControllerError(again, "invalid_state", "record_response");
    expect(ctx.counts.renames).toBe(renames);
  } finally {
    await dispose(ctx);
  }
});

test("17. repeating the durable response succeeds without a dispatch", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const raw = responseRaw(waiting, 1, "continue_stage");
    await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw,
    });
    const responseBefore = await fileIdentity(join(ctx.waits, "1.response.json"));
    const requestBefore = await fileIdentity(join(ctx.waits, "1.request.json"));
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const renamesBefore = ctx.counts.renames;
    const result = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw,
    });
    expect(result.wait_index).toBe(1);
    expect(ctx.counts.renames).toBe(renamesBefore);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
    const responseAfter = await fileIdentity(join(ctx.waits, "1.response.json"));
    expect(responseAfter.ino).toBe(responseBefore.ino);
    expect(responseAfter.mtimeMs).toBe(responseBefore.mtimeMs);
    const requestAfter = await fileIdentity(join(ctx.waits, "1.request.json"));
    expect(requestAfter.ino).toBe(requestBefore.ino);
  } finally {
    await dispose(ctx);
  }
});

test("18. a different response for the answered wait is a conflict without writes", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    });
    const responseBefore = await fileIdentity(join(ctx.waits, "1.response.json"));
    const stateBefore = await readFile(ctx.statePath, "utf8");
    const cause = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "revise_task"),
    }).catch((error) => error);
    expectControllerError(cause, "wait_conflict", "record_response");
    const responseAfter = await fileIdentity(join(ctx.waits, "1.response.json"));
    expect(responseAfter.bytes).toBe(responseBefore.bytes);
    expect(await readFile(ctx.statePath, "utf8")).toBe(stateBefore);
    expect(ctx.sink.snapshot?.cursor.current_state).toBe("ship");
  } finally {
    await dispose(ctx);
  }
});

test("19. two sequential wait cycles keep the first journal record byte-identical", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    let state = ctx.sink.snapshot as PipelineV2RunState;
    const firstFile = await fileIdentity(join(ctx.waits, "1.request.json"));
    await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(state, 1, "continue_stage"),
    });
    state = ctx.sink.snapshot as PipelineV2RunState;
    const firstRecord = state.waits[0];
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.waits.length).toBe(2);
    expect(state.waits[0]).toEqual(firstRecord);
    expect(state.waits[1]?.index).toBe(2);
    const firstFileAfter = await fileIdentity(join(ctx.waits, "1.request.json"));
    expect(firstFileAfter.ino).toBe(firstFile.ino);
    expect(firstFileAfter.bytes).toBe(firstFile.bytes);
    await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 2,
      raw: responseRaw(state, 2, "continue_stage"),
    });
    state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.waits[0]).toEqual(firstRecord);
    expect(state.waits[1]?.response?.action_id).toBe("continue_stage");
    expect(parsePipelineV2RunState(await readFile(ctx.statePath, "utf8"))).toEqual(state);
  } finally {
    await dispose(ctx);
  }
});

test("20. concurrent identical requests: both succeed, one durable run_waiting", async () => {
  const ctx = await setup();
  try {
    const sink = barrierSink(ctx.sink);
    const both = await Promise.allSettled([
      enterPipelineV2Wait({ runRoot: ctx.runRoot, sink, reason: REASON, actions: ACTIONS }),
      enterPipelineV2Wait({ runRoot: ctx.runRoot, sink, reason: REASON, actions: ACTIONS }),
    ]);
    expect(both.map((entry) => entry.status)).toEqual(["fulfilled", "fulfilled"]);
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.revision).toBe(13);
    expect(state.waits.length).toBe(1);
    expect(state.waits[0]?.reason).toBe(REASON);
  } finally {
    await dispose(ctx);
  }
});

test("21. concurrent different requests: one wins, the loser conflicts", async () => {
  const ctx = await setup();
  try {
    const both = await Promise.allSettled([
      enterPipelineV2Wait({ runRoot: ctx.runRoot, sink: ctx.sink, reason: REASON, actions: ACTIONS }),
      enterPipelineV2Wait({
        runRoot: ctx.runRoot,
        sink: ctx.sink,
        reason: OTHER_REASON,
        actions: ACTIONS,
      }),
    ]);
    const statuses = both.map((entry) => entry.status).sort();
    expect(statuses).toEqual(["fulfilled", "rejected"]);
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.revision).toBe(13);
    expect(state.waits.length).toBe(1);
    const stored = await readFile(join(ctx.waits, "1.request.json"), "utf8");
    expect(stored).toContain(state.waits[0]!.reason);
  } finally {
    await dispose(ctx);
  }
});

test("22. concurrent identical responses: both succeed, one durable response", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const raw = responseRaw(waiting, 1, "continue_stage");
    const sink = barrierSink(ctx.sink);
    const both = await Promise.allSettled([
      recordPipelineV2WaitResponse({ runRoot: ctx.runRoot, sink, waitIndex: 1, raw }),
      recordPipelineV2WaitResponse({ runRoot: ctx.runRoot, sink, waitIndex: 1, raw }),
    ]);
    expect(both.map((entry) => entry.status)).toEqual(["fulfilled", "fulfilled"]);
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.revision).toBe(14);
    expect(state.waits[0]?.response?.action_id).toBe("continue_stage");
  } finally {
    await dispose(ctx);
  }
});

test("23. concurrent different responses: one wins, the loser conflicts", async () => {
  const ctx = await setup();
  try {
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const both = await Promise.allSettled([
      recordPipelineV2WaitResponse({
        runRoot: ctx.runRoot,
        sink: ctx.sink,
        waitIndex: 1,
        raw: responseRaw(waiting, 1, "continue_stage"),
      }),
      recordPipelineV2WaitResponse({
        runRoot: ctx.runRoot,
        sink: ctx.sink,
        waitIndex: 1,
        raw: responseRaw(waiting, 1, "revise_task"),
      }),
    ]);
    expect(both.map((entry) => entry.status).sort()).toEqual(["fulfilled", "rejected"]);
    const state = ctx.sink.snapshot as PipelineV2RunState;
    expect(state.revision).toBe(14);
    expect(state.waits[0]?.response).toBeDefined();
    const stored = await readFile(join(ctx.waits, "1.response.json"), "utf8");
    expect(["continue_stage", "revise_task"]).toContain(
      (JSON.parse(stored) as Record<string, string>)["action_id"]!,
    );
  } finally {
    await dispose(ctx);
  }
});

test("24. reassigning sink.dispatch after the call cannot hijack the captured dispatch", async () => {
  const ctx = await setup();
  try {
    const promise = enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    (ctx.sink as unknown as { dispatch: unknown }).dispatch = () => {
      throw new Error("hijacked dispatch");
    };
    const result = await promise;
    expect(result.wait_index).toBe(1);
    expect(ctx.sink.snapshot?.status).toBe("waiting");
  } finally {
    await dispose(ctx);
  }
});

test("25. a poisoned sink is rejected before any filesystem effect", async () => {
  const ctx = await setup();
  try {
    const fake: PipelineV2WaitControllerSink = {
      snapshot: null,
      poisoned: true,
      dispatch: async () => undefined,
    };
    const emptyRunRoot = join(ctx.root, "runs", "fresh");
    await mkdir(emptyRunRoot, { mode: 0o700 });
    const cause = await enterPipelineV2Wait({
      runRoot: emptyRunRoot,
      sink: fake,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(cause, "invalid_state", "enter_wait");
    expect(await readdir(emptyRunRoot)).toEqual([]);
  } finally {
    await dispose(ctx);
  }
});

test("26. a run root that does not belong to the run is rejected before the filesystem", async () => {
  const ctx = await setup();
  try {
    const foreignRoot = join(ctx.root, "runs", "run-2");
    await mkdir(foreignRoot, { mode: 0o700 });
    const cause = await enterPipelineV2Wait({
      runRoot: foreignRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(cause, "invalid_state", "enter_wait");
    expect(await readdir(foreignRoot)).toEqual([]);
    const responseCause = await recordPipelineV2WaitResponse({
      runRoot: foreignRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: "{}",
    }).catch((error) => error);
    expectControllerError(responseCause, "invalid_state", "record_response");
    expect(await readdir(foreignRoot)).toEqual([]);
  } finally {
    await dispose(ctx);
  }
});

test("27. results are deep-frozen and input mutations cannot reach them", async () => {
  const ctx = await setup();
  try {
    const actions = [{ id: "continue_stage", to: "ship" }, { id: "revise_task", to: "ship" }];
    const options = {
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions,
    };
    const result = await enterPipelineV2Wait(options);
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.state)).toBe(true);
    expect(Object.isFrozen(result.state.waits[0])).toBe(true);
    (actions[0] as { id: string }).id = "mutated_action";
    expect(result.state.waits[0]?.actions[0]?.id).toBe("continue_stage");
    expect(() => {
      (result as unknown as { wait_index: number }).wait_index = 42;
    }).toThrow();
    const response = (await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(ctx.sink.snapshot as PipelineV2RunState, 1, "continue_stage"),
    })) as RecordedPipelineV2WaitResponse;
    expect(Object.isFrozen(response)).toBe(true);
    expect(Object.isFrozen(response.state)).toBe(true);
  } finally {
    await dispose(ctx);
  }
});

test("28. diagnostics never carry manifest content, response bodies or canaries", async () => {
  const ctx = await setup();
  try {
    const badRequest = await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: `bad/${CANARY}`,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(badRequest, "invalid_request", "enter_wait");
    expect((badRequest as Error).message).not.toContain(CANARY);
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    const badResponse = await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, `act_${CANARY}`),
    }).catch((error) => error);
    expectControllerError(badResponse, "invalid_response", "record_response");
    expect((badResponse as Error).message).not.toContain(CANARY);
    expect((badResponse as Error).message).not.toContain("continue_stage");
    const foreignRoot = join(ctx.root, "runs", "run-2");
    await mkdir(foreignRoot, { mode: 0o700 });
    const mismatch = await enterPipelineV2Wait({
      runRoot: foreignRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    }).catch((error) => error);
    expectControllerError(mismatch, "invalid_state", "enter_wait");
    expect((mismatch as Error).message).not.toContain(foreignRoot);
  } finally {
    await dispose(ctx);
  }
});

test("29. the controller changes nothing outside the sink protocol and the waits directory", async () => {
  const ctx = await setup();
  try {
    await mkdir(join(ctx.runRoot, "project"), { mode: 0o700 });
    await writeFile(join(ctx.runRoot, "project", "a.md"), "project body\n", { mode: 0o600 });
    await writeFile(join(ctx.runRoot, "sentinel"), "sentinel\n", { mode: 0o600 });
    await enterPipelineV2Wait({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      reason: REASON,
      actions: ACTIONS,
    });
    const waiting = ctx.sink.snapshot as PipelineV2RunState;
    await recordPipelineV2WaitResponse({
      runRoot: ctx.runRoot,
      sink: ctx.sink,
      waitIndex: 1,
      raw: responseRaw(waiting, 1, "continue_stage"),
    });
    expect(await readFile(join(ctx.runRoot, "sentinel"), "utf8")).toBe("sentinel\n");
    expect(await readFile(join(ctx.runRoot, "project", "a.md"), "utf8")).toBe("project body\n");
    expect((await readdir(ctx.runRoot)).sort()).toEqual(["project", "sentinel", "waits"]);
  } finally {
    await dispose(ctx);
  }
});

test("30. no message-text classification exists in the controller source", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync(
    join(import.meta.dir, "../src/pipeline_v2_wait_controller.ts"),
    "utf8",
  );
  expect(source.includes(".message.includes")).toBe(false);
  expect(source.includes(".message.match")).toBe(false);
  expect(source.includes(".message.startsWith")).toBe(false);
  expect(source.includes("new RegExp")).toBe(false);
});

test("31. the public export surface carries no IO or test seams", async () => {
  const namespace = (await import("../src/pipeline_v2_wait_controller.ts")) as Record<
    string,
    unknown
  >;
  expect(Object.keys(namespace).sort()).toEqual([
    "PipelineV2WaitControllerError",
    "enterPipelineV2Wait",
    "recordPipelineV2WaitResponse",
  ]);
});
