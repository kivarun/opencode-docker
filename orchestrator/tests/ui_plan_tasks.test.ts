/**
 * Permanent contract suite of the browser UI "Plan and tasks" slice
 * (S6): the two P-1 content parsers (the byte-exact task-chunk
 * envelope and the paged plan projection), the two URL builders, the
 * two single-flight content transports over the shared stream
 * factory, the pinned identity-ledger merge, the WebCrypto body
 * recomputation and the frozen cross-targets derivation. The wire
 * shapes mirror the shipped endpoints (`GET
 * /v1/runs/:runId/tasks/:taskId/revisions/:revision` and
 * `GET /v1/runs/:runId/plans/:revision`).
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  S6_BODY_RETAINED_BYTES_CAP,
  S6_PLAN_RECORDS_RETAINED_CAP,
  applyPlanPage,
  applyPlanTasksIdentityPage,
  applyTaskChunk,
  createIdentityTransport,
  createPlanContentTransport,
  createPlanTasksState,
  createPlanTasksViewer,
  createTaskContentTransport,
  derivePlanTasksView,
  parsePlanContentDocument,
  parsePlanContentEndpointDocument,
  parseTaskContentDocument,
  parseTaskContentEndpointDocument,
  planContentRequestPath,
  taskContentRequestPath,
  verifyTaskBodyDigest,
  UiContractError,
} from "../ui/app.js";
import { stageTaskBodyDigest } from "../src/pipeline_v2_stage_task.ts";

const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");

const RUN = "run-pt-model";
const TEXT_ENCODER = new TextEncoder();
const sha = (char: string) => char.repeat(64);
const b64 = (text: string) => Buffer.from(text, "utf8").toString("base64");

interface S6TaskChunkBody {
  byteCount: number;
  bodySha256: string;
  offset: number;
  limit: number;
  returnedByteCount: number | null;
  nextOffset: number | null;
  data: Uint8Array;
}
type S6TaskChunk = ReturnType<typeof parseTaskContentDocument> & { body: S6TaskChunkBody; [key: string]: unknown };
type S6PlanPage = ReturnType<typeof parsePlanContentDocument> & { [key: string]: unknown };

const TASK_DOCUMENT_BASE = {
  schema_version: 1,
  kind: "task_revision",
  run_id: RUN,
  task_id: "task-alpha",
  revision: 2,
  manifest_sha256: sha("5"),
  previous_sha256: sha("6"),
  origin: "planning_proposal",
};

function taskDocument(overrides: Record<string, unknown> = {}, bodyOverrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    ...TASK_DOCUMENT_BASE,
    ...overrides,
    body: {
      byte_count: 5,
      sha256: sha("7"),
      offset: 0,
      limit: 65536,
      returned_byte_count: 5,
      next_offset: null,
      data_base64: b64("alpha"),
      ...bodyOverrides,
    },
  };
}

const PLAN_DOCUMENT_BASE = {
  schema_version: 1,
  kind: "plan_revision",
  run_id: RUN,
  revision: 1,
  manifest_sha256: sha("3"),
  previous_sha256: null,
  origin_execution: 1,
};

function planDocument(overrides: Record<string, unknown> = {}, records: unknown[] | null = null): Record<string, unknown> {
  return {
    ...PLAN_DOCUMENT_BASE,
    after: 0,
    records:
      records ??
      [
        { record: "stage", stage_position: 1, id: "stage-one", template: "development" },
        { record: "task", stage_position: 1, task_position: 1, id: "task-alpha", revision: 1, sha256: sha("1") },
      ],
    next_after: null,
    ...overrides,
  };
}

const TASK_BINDING = { runId: RUN, taskId: "task-alpha", revision: 2 };
const PLAN_BINDING = { runId: RUN, revision: 1 };

function expectContractError(label: string, call: () => unknown) {
  try {
    call();
  } catch (cause) {
    expect((cause as UiContractError).name, label).toBe("UiContractError");
    return;
  }
  throw new Error(`unreachable: ${label}`);
}

describe("plan/tasks S6: the task-content document parser", () => {
  test("parses one well-formed terminal chunk with the exact frozen shape", () => {
    const model = parseTaskContentDocument(taskDocument(), TASK_BINDING) as S6TaskChunk;
    expect(model.kind).toBe("task_revision");
    expect(model.runId).toBe(RUN);
    expect(model.taskId).toBe("task-alpha");
    expect(model.revision).toBe(2);
    expect(model.manifestSha256).toBe(sha("5"));
    expect(model.previousSha256).toBe(sha("6"));
    expect(model.origin).toBe("planning_proposal");
    expect({
      byteCount: model.body.byteCount,
      bodySha256: model.body.bodySha256,
      offset: model.body.offset,
      limit: model.body.limit,
      returnedByteCount: model.body.returnedByteCount,
      nextOffset: model.body.nextOffset,
    }).toEqual({
      byteCount: 5,
      bodySha256: sha("7"),
      offset: 0,
      limit: 65536,
      returnedByteCount: 5,
      nextOffset: null,
    });
    expect(Buffer.from(model.body.data).toString("utf8")).toBe("alpha");
  });
  test("zero progress exists only as the terminal chunk of a complete body", () => {
    const model = parseTaskContentDocument(
      taskDocument({}, { byte_count: 0, returned_byte_count: 0, data_base64: "" }),
      TASK_BINDING,
    ) as S6TaskChunk;
    expect(model.body.returnedByteCount).toBe(0);
    expect(model.body.nextOffset).toBeNull();
    expect(model.body.data.byteLength).toBe(0);
  });
  test("rejects malformed, foreign and hostile documents with typed contract errors", () => {
    const dropField = (document: Record<string, unknown>, field: string) => {
      const copy = { ...document } as Record<string, unknown>;
      delete copy[field];
      return copy;
    };
    const cases: Array<[string, () => unknown]> = [
      ["not a document", () => parseTaskContentDocument(null, null)],
      ["unknown schema version", () => parseTaskContentDocument(taskDocument({ schema_version: 2 }), null)],
      ["unknown kind", () => parseTaskContentDocument(taskDocument({ kind: "identity" }), null)],
      ["missing field", () => parseTaskContentDocument(dropField(taskDocument(), "origin"), null)],
      ["extra field", () => parseTaskContentDocument(taskDocument({ extra: true }), null)],
      ["body extra field", () =>
        parseTaskContentDocument(taskDocument({}, { extra: true }), null)],
      ["limit below the returned bytes", () => parseTaskContentDocument(taskDocument({}, { limit: 4 }), null)],
      ["decoded byte count mismatch", () => parseTaskContentDocument(taskDocument({}, { returned_byte_count: 4 }), null)],
      ["zero-result non-terminal chunk", () =>
        parseTaskContentDocument(taskDocument({}, { byte_count: 10, returned_byte_count: 0, next_offset: 0, data_base64: "" }), null)],
      ["pointer beyond the body", () => parseTaskContentDocument(taskDocument({}, { next_offset: 5 }), null)],
      ["pointer without the end", () => parseTaskContentDocument(taskDocument({}, { byte_count: 9, next_offset: null }), null)],
      ["byte_count overrun", () => parseTaskContentDocument(taskDocument({}, { byte_count: 4 }), null)],
      ["non-canonical base64", () => parseTaskContentDocument(taskDocument({}, { data_base64: "YWxwaGE==" }), null)],
      ["base64 charset hostile", () => parseTaskContentDocument(taskDocument({}, { data_base64: "YW*x()*" }), null)],
      ["run foreign", () => parseTaskContentDocument(taskDocument({ run_id: "run-other" }), TASK_BINDING)],
      ["task foreign", () => parseTaskContentDocument(taskDocument(), { runId: RUN, taskId: "task-beta", revision: 2 })],
      ["revision foreign", () => parseTaskContentDocument(taskDocument({ revision: 3 }), TASK_BINDING)],
      ["origin outside the vocabulary", () => parseTaskContentDocument(taskDocument({ origin: "operator" }), null)],
      ["binding object hostile", () => parseTaskContentDocument(taskDocument(), { runId: RUN, taskId: "task-alpha" })],
      ["next_offset type hostile", () => parseTaskContentDocument(taskDocument({}, { next_offset: "5" }), null)],
    ];
    for (const [label, call] of cases) {
      expectContractError(label, call);
    }
  });
});

describe("plan/tasks S6: the plan-content document parser", () => {
  test("parses one well-formed complete page with the exact frozen shape", () => {
    const model = parsePlanContentDocument(planDocument(), PLAN_BINDING) as S6PlanPage;
    expect(model.kind).toBe("plan_revision");
    expect(model.revision).toBe(1);
    expect(model.originExecution).toBe(1);
    expect(model.manifestSha256).toBe(sha("3"));
    expect(model.previousSha256).toBeNull();
    expect(model.records).toHaveLength(2);
    expect(model.records[0]).toEqual({ record: "stage", stagePosition: 1, id: "stage-one", template: "development" });
    expect(model.records[1]).toEqual({
      record: "task",
      stagePosition: 1,
      taskPosition: 1,
      id: "task-alpha",
      revision: 1,
      sha256: sha("1"),
    });
    expect(model.nextAfter).toBeNull();
  });
  test("renders all three projected record kinds with positions", () => {
    const model = parsePlanContentDocument(planDocument({}, [
      { record: "stage", stage_position: 1, id: "stage-two", template: "development" },
      { record: "task", stage_position: 1, task_position: 2, id: "task-beta", revision: 1, sha256: sha("2") },
      { record: "task_dependency", stage_position: 1, task_position: 2, dependency_position: 1, task_id: "task-alpha" },
    ]), null) as S6PlanPage;
    expect(model.records.map((record) => record.record)).toEqual(["stage", "task", "task_dependency"]);
  });
  test("rejects malformed, foreign and hostile documents with typed contract errors", () => {
    const dropField = (document: Record<string, unknown>, field: string) => {
      const copy = { ...document } as Record<string, unknown>;
      delete copy[field];
      return copy;
    };
    const cases: Array<[string, () => unknown]> = [
      ["not a document", () => parsePlanContentDocument(null, null)],
      ["unknown schema version", () => parsePlanContentDocument(planDocument({ schema_version: 2 }), null)],
      ["unknown kind", () => parsePlanContentDocument(planDocument({ kind: "identity" }), null)],
      ["missing field", () => parsePlanContentDocument(dropField(planDocument(), "origin_execution"), null)],
      ["next_after beyond a page", () => parsePlanContentDocument(planDocument({ next_after: 3 }), null)],
      ["empty page with a cursor", () => parsePlanContentDocument(planDocument({ records: [], next_after: 1 }), null)],
      ["run foreign", () => parsePlanContentDocument(planDocument({ run_id: "run-other" }), PLAN_BINDING)],
      ["revision foreign", () => parsePlanContentDocument(planDocument({ revision: 2 }), PLAN_BINDING)],
      ["stage record hostile", () => parsePlanContentDocument(planDocument({}, [{ record: "stage", stage_position: 1, id: "s1", extra: true }]), null)],
      ["record kind unknown", () => parsePlanContentDocument(planDocument({}, [{ record: "wait" }]), null)],
      ["records non-list", () => parsePlanContentDocument(planDocument({ records: "no" }), null)],
      ["binding object hostile", () => parsePlanContentDocument(planDocument(), { runId: RUN })],
    ];
    for (const [label, call] of cases) {
      expectContractError(label, call);
    }
  });
});

describe("plan/tasks S6: the endpoint classifiers", () => {
  test("accept the selection shapes and the endpoint failure envelopes", () => {
    expect((parseTaskContentEndpointDocument(taskDocument(), TASK_BINDING) as { kind: string }).kind).toBe("task_revision");
    expect((parsePlanContentEndpointDocument(planDocument(), PLAN_BINDING) as { kind: string }).kind).toBe("plan_revision");
    for (const parse of [parseTaskContentEndpointDocument, parsePlanContentEndpointDocument]) {
      const failed = parse(
        {
          schema_version: 1,
          kind: "command_result",
          run_id: RUN,
          outcome: "failed",
          exit_code: 1,
          reason: "artifact_modified",
        },
        null,
      ) as { kind: string; outcome: string; reason: string | null };
      expect(failed.outcome).toBe("failed");
      expect(failed.reason).toBe("artifact_modified");
      const error = parse({ schema_version: 1, kind: "error", layer: "request", reason: "invalid_cursor" }, null) as { kind: string };
      expect(error.kind).toBe("error");
      expectContractError("succeeded result", () =>
        parse({ schema_version: 1, kind: "command_result", run_id: RUN, outcome: "succeeded", exit_code: 0 }, null));
      expectContractError("unknown kind", () => parse({ schema_version: 1, kind: "identity" }, null));
      expectContractError("unsupported version", () => parse({ schema_version: 2, kind: "error", layer: "request", reason: "invalid_cursor" }, null));
    }
  });
  test("reject hostiles before any awaited work", () => {
    expectContractError("not a document (task)", () => parseTaskContentEndpointDocument("no", null));
    expectContractError("not a document (plan)", () => parsePlanContentEndpointDocument([], null));
  });
});

describe("plan/tasks S6: the URL builders", () => {
  test("build the exact P-1 paths", () => {
    expect(taskContentRequestPath(RUN, "task-alpha", 2, 0, 65536)).toBe(`/v1/runs/${RUN}/tasks/task-alpha/revisions/2?offset=0&limit=65536`);
    expect(planContentRequestPath(RUN, 1, 0, 100)).toBe(`/v1/runs/${RUN}/plans/1?after=0&limit=100`);
  });
  test("reject unsafe and cursor-invalid inputs", () => {
    const cases: Array<[string, () => unknown]> = [
      ["task run unsafe", () => taskContentRequestPath("run pt", "task-alpha", 1, 0, 100)],
      ["task id unsafe", () => taskContentRequestPath(RUN, "task/alpha", 1, 0, 100)],
      ["task revision zero", () => taskContentRequestPath(RUN, "task-alpha", 0, 0, 100)],
      ["task offset negative", () => taskContentRequestPath(RUN, "task-alpha", 1, -1, 100)],
      ["task limit over the max", () => taskContentRequestPath(RUN, "task-alpha", 1, 0, 193537)],
      ["plan run unsafe", () => planContentRequestPath("run pt", 1, 0, 100)],
      ["plan revision zero", () => planContentRequestPath(RUN, 0, 0, 100)],
      ["plan after negative", () => planContentRequestPath(RUN, 1, -1, 100)],
      ["plan limit over the max", () => planContentRequestPath(RUN, 1, 0, 201)],
    ];
    for (const [label, call] of cases) {
      expectContractError(label, call);
    }
  });
});

describe("plan/tasks S6: the transports share the stream factory", () => {
  function io(fetchKey: string, impl: (path: string, runId: string, signal: AbortSignal, binding: unknown) => unknown) {
    return {
      [fetchKey]: impl,
      onStarted: () => {},
      onSettled: () => {},
    };
  }
  test("the task and plan transports pass the request binding to the client", async () => {
    const seen: Array<{ path: string; runId: string; binding: unknown }> = [];
    const taskIo = io("fetchTaskContent", async (path, runId, _signal, binding) => {
      seen.push({ path, runId, binding });
      return { ok: true, model: parseTaskContentDocument(taskDocument(), binding as typeof TASK_BINDING) };
    });
    const taskTransport = createTaskContentTransport(taskIo as never);
    const applied: unknown[] = [];
    await taskTransport.request({
      runId: RUN,
      kind: "initial",
      binding: TASK_BINDING,
      path: taskContentRequestPath(RUN, "task-alpha", 2, 0, 65536),
      onApplied: (result) => {
        applied.push(result);
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(seen).toHaveLength(1);
    expect(seen[0]?.binding).toEqual(TASK_BINDING);
    const model = (applied[0] as { model: { kind: string } }).model;
    expect(model.kind).toBe("task_revision");
  });
});

function mergeIdentity(
  state: ReturnType<typeof createPlanTasksState> | null,
  page: unknown,
  after: number,
): ReturnType<typeof createPlanTasksState> {
  const merged = applyPlanTasksIdentityPage(state, page, after);
  if (merged === null) {
    throw new Error("unreachable: identity merge rejected a contract-valid page");
  }
  return merged;
}
function mergeChunk(
  state: ReturnType<typeof createPlanTasksState> | null,
  model: unknown,
  offset: number,
  limit: number,
  readerId: number,
): ReturnType<typeof createPlanTasksState> {
  const merged = applyTaskChunk(state, model, offset, limit, readerId);
  if (merged === null) {
    throw new Error("unreachable: task chunk merge rejected a contract-valid chunk");
  }
  return merged;
}
function mergePlan(
  state: ReturnType<typeof createPlanTasksState> | null,
  model: unknown,
  after: number,
  limit: number,
  readerId: number,
): ReturnType<typeof createPlanTasksState> {
  const merged = applyPlanPage(state, model, after, limit, readerId);
  if (merged === null) {
    throw new Error("unreachable: plan page merge rejected a contract-valid page");
  }
  return merged;
}
describe("plan/tasks S6: the pinned ledger merge", () => {
  function identityPage(revision: number, after: number, records: unknown[], nextAfter: number | null) {
    return { kind: "identity", revision, after, records, nextAfter };
  }
  const planRecord = { record: "plan_revision", index: 1, revision: 1, sha256: sha("3"), previousSha256: null, originExecution: 1 };
  const taskRecord = { record: "task_revision", index: 1, taskId: "task-alpha", revision: 1, sha256: sha("1"), previousSha256: null, waitIndex: null, intentSha256: null };
  test("createPlanTasksState guards its scalars and starts unloaded", () => {
    const state = createPlanTasksState(RUN, 5);
    expect(state.runId).toBe(RUN);
    expect(state.pin).toBe(5);
    expect(state.identity.records).toEqual([]);
    expect(state.taskTarget).toBeNull();
    expect(state.planTarget).toBeNull();
    for (const [badId, badPin] of [["run pt", 1], [RUN, 0]] as const) {
      expectContractError(`state ${badId}:${badPin}`, () => createPlanTasksState(badId, badPin));
    }
  });
  test("applies pages cursor/recise-pinned and completes the stream", () => {
    let state = createPlanTasksState(RUN, 1);
    const first = mergeIdentity(state, identityPage(1, 0, [planRecord, taskRecord], 2), 0);
    expect(first.identity.records).toHaveLength(2);
    expect(first.identity.cursor).toBe(2);
    expect(first.identity.loaded).toBe(true);
    expectContractError("stale continuation anchor", () => applyPlanTasksIdentityPage(first, identityPage(1, 0, [], null), 0));
    expectContractError("foreign revision page", () => applyPlanTasksIdentityPage(first, identityPage(2, 2, [], null), 2));
    const complete = mergeIdentity(first, identityPage(1, 2, [], null), 2);
    expect(complete.identity.nextAfter).toBeNull();
    expect(derivePlanTasksView(complete).complete).toBe(true);
    expectContractError("non-identity page", () => applyPlanTasksIdentityPage(complete, planDocument(), 0));
  });
  test("derivePlanTasksView guards its input", () => {
    expectContractError("state hostile", () => derivePlanTasksView({ runId: RUN } as never));
  });
});

describe("plan/tasks S6: the task chunk merge", () => {
  function taskTargetFixture(overrides: Record<string, unknown> = {}) {
    return {
      kind: "task",
      key: "task:task-alpha:2",
      taskId: "task-alpha",
      revision: 2,
      epoch: 1,
      ledgerSha256: sha("5"),
      ledgerPreviousSha256: sha("6"),
      readerId: 1,
      inFlightLimit: 65536,
      truncatedPending: false,
      chunkCount: 0,
      status: "streaming",
      envelope: null,
      cursor: 0,
      retained: Object.freeze([]),
      retainedBytes: 0,
      decodedText: "",
      decoder: null,
      terminal: false,
      truncated: false,
      integrity: "pending",
      verifiedDigest: null,
      failure: null,
      ...overrides,
    };
  }
  function stateWithTask(overrides: Record<string, unknown> = {}) {
    const state = createPlanTasksState(RUN, 1);
    return { ...state, taskTarget: taskTargetFixture(overrides) };
  }
  test("streams two chunks and reaches the verifying terminal", () => {
    const first = parseTaskContentDocument(
      taskDocument({}, { byte_count: 10, returned_byte_count: 5, data_base64: b64("alpha"), next_offset: 5 }),
      TASK_BINDING,
    );
    const afterFirst = mergeChunk(stateWithTask(), first, 0, 65536, 1);
    expect(afterFirst.taskTarget?.status).toBe("streaming");
    expect(afterFirst.taskTarget?.retainedBytes).toBe(5);
    expect(afterFirst.taskTarget?.cursor).toBe(5);
    expect(afterFirst.taskTarget?.decodedText).toBe("alpha");
    expect(afterFirst.taskTarget?.envelope).toEqual({
      manifestSha256: sha("5"),
      previousSha256: sha("6"),
      origin: "planning_proposal",
      byteCount: 10,
      bodySha256: sha("7"),
    });
    expect(afterFirst.taskTarget?.retained).toHaveLength(1);
    const second = parseTaskContentDocument(
      taskDocument({}, { byte_count: 10, returned_byte_count: 5, offset: 5, data_base64: b64("beta!"), next_offset: null }),
      TASK_BINDING,
    );
    const afterSecond = mergeChunk(afterFirst, second, 5, 65536, 2);
    expect(afterSecond.taskTarget?.status).toBe("verifying");
    expect(afterSecond.taskTarget?.terminal).toBe(true);
    expect(afterSecond.taskTarget?.decodedText).toBe("alphabeta!");
    expect(afterSecond.taskTarget?.truncated).toBe(false);
    expect(afterSecond.taskTarget?.envelope).toEqual(afterFirst.taskTarget?.envelope);
  });
  test("the terminal chunk flushes the streaming decoder exactly once", () => {
    const chunks = ["alpha", "beta!", "gamma"].map((text, index) =>
      parseTaskContentDocument(
        taskDocument({}, {
          byte_count: 15,
          returned_byte_count: 5,
          offset: index * 5,
          data_base64: b64(text),
          next_offset: index === 2 ? null : (index + 1) * 5,
        }),
        TASK_BINDING,
      ),
    );
    let state: ReturnType<typeof mergeChunk> = mergeChunk(stateWithTask(), chunks[0]!, 0, 65536, 1);
    state = mergeChunk(state, chunks[1]!, 5, 65536, 2);
    state = mergeChunk(state, chunks[2]!, 10, 65536, 3);
    expect(state.taskTarget?.decodedText).toBe("alphabeta!gamma");
  });
  test("rejects protocol violations with typed contract errors", () => {
    const first = parseTaskContentDocument(taskDocument(), TASK_BINDING);
    const applied = mergeChunk(stateWithTask(), first, 0, 65536, 1);
    const caseList: Array<[string, () => unknown]> = [
      ["stale state", () => applyTaskChunk(createPlanTasksState(RUN, 1), first, 0, 65536, 1)],
      ["stale cursor", () => applyTaskChunk(stateWithTask({ cursor: 7 }), first, 0, 65536, 1)],
      ["stale reader", () => applyTaskChunk(stateWithTask(), first, 0, 65536, 99)],
      ["wrong requested limit", () => applyTaskChunk(stateWithTask(), first, 0, 111, 1)],
      ["model non-task", () => applyTaskChunk(stateWithTask(), planDocument(), 0, 65536, 1)],
      ["foreign ledger binding", () =>
        applyTaskChunk(stateWithTask({ ledgerSha256: sha("8") }), first, 0, 65536, 1)],
      ["envelope drift origin", () =>
        applyTaskChunk(
          stateWithTask({
            envelope: Object.freeze({
              manifestSha256: sha("5"),
              previousSha256: sha("6"),
              origin: "user_response",
              byteCount: 5,
              bodySha256: sha("7"),
            }),
          }),
          first,
          0,
          65536,
          1,
        )],
      ["retention cap", () =>
        applyTaskChunk(
          stateWithTask({ retainedBytes: S6_BODY_RETAINED_BYTES_CAP, truncatedPending: true }),
          first,
          0,
          65536,
          1,
        )],
    ];
    for (const [label, call] of caseList) {
      expectContractError(label, call);
    }
  });
});

describe("plan/tasks S6: the plan page merge", () => {
  function planTargetFixture(overrides: Record<string, unknown> = {}) {
    return {
      kind: "plan",
      key: "plan:1",
      revision: 1,
      epoch: 1,
      ledgerSha256: sha("3"),
      ledgerPreviousSha256: null,
      ledgerOriginExecution: 1,
      readerId: 1,
      inFlightLimit: 100,
      status: "streaming",
      envelope: null,
      cursor: 0,
      records: Object.freeze([]),
      terminal: false,
      truncated: false,
      failure: null,
      ...overrides,
    };
  }
  function stateWithPlan(overrides: Record<string, unknown> = {}) {
    const state = createPlanTasksState(RUN, 1);
    return { ...state, planTarget: planTargetFixture(overrides) };
  }
  test("streams two pages and reaches the ready terminal", () => {
    const page = (after: number, nextAfter: number | null, records: unknown[]) =>
      parsePlanContentDocument(planDocument({ after, next_after: nextAfter }, records), PLAN_BINDING);
    const r1 = [
      { record: "stage", stage_position: 1, id: "stage-one", template: "development" },
    ];
    const r2 = [
      { record: "task", stage_position: 1, task_position: 1, id: "task-alpha", revision: 1, sha256: sha("1") },
    ];
    const first = mergePlan(stateWithPlan(), page(0, 1, r1), 0, 100, 1);
    expect(first.planTarget?.status).toBe("streaming");
    expect(first.planTarget?.cursor).toBe(1);
    expect(first.planTarget?.envelope).toEqual({ manifestSha256: sha("3"), previousSha256: null, originExecution: 1 });
    const second = mergePlan(first, page(1, null, r2), 1, 100, 2);
    expect(second.planTarget?.status).toBe("ready");
    expect(second.planTarget?.terminal).toBe(true);
    expect(second.planTarget?.records).toHaveLength(2);
  });
  test("rejects protocol violations with typed contract errors", () => {
    const page = parsePlanContentDocument(planDocument(), PLAN_BINDING);
    const caseList: Array<[string, () => unknown]> = [
      ["empty state", () => applyPlanPage(createPlanTasksState(RUN, 1), page, 0, 100, 1)],
      ["stale cursor", () => applyPlanPage(stateWithPlan({ cursor: 4 }), page, 0, 100, 1)],
      ["stale reader", () => applyPlanPage(stateWithPlan(), page, 0, 100, 9)],
      ["wrong requested limit", () => applyPlanPage(stateWithPlan(), page, 0, 1, 1)],
      ["after mismatch", () => applyPlanPage(stateWithPlan(), parsePlanContentDocument(planDocument({ after: 1 }), PLAN_BINDING), 0, 100, 1)],
      ["model non-plan", () => applyPlanPage(stateWithPlan(), taskDocument(), 0, 100, 1)],
      ["retention cap", () =>
        applyPlanPage(
          stateWithPlan({ records: Array.from({ length: S6_PLAN_RECORDS_RETAINED_CAP }, () => ({})) }),
          page,
          0,
          100,
          1,
        )],
    ];
    for (const [label, call] of caseList) {
      expectContractError(label, call);
    }
  });
});

describe("plan/tasks S6: the frozen derivation", () => {
  /** The identity stream fixture shared by the cross-target checks:
   * task-alpha@1 accepted with two stage executions binding the same
   * manifest digest but two DIFFERENT body digests, plan r2 selecting
   * the pointered revision, and one execution without the stage-task
   * binding. */
  function fixtureState(overrides: {
    planRevision?: number;
    taskBodySha?: string;
    includeStageTask?: boolean;
    includePlanPointer?: boolean;
  } = {}) {
    const planRevision = overrides.planRevision ?? 1;
    const taskBodySha = overrides.taskBodySha ?? sha("b");
    const records: Array<Record<string, unknown>> = [
      { record: "plan_revision", index: 1, revision: planRevision, sha256: sha("3"), previousSha256: null, originExecution: 1 },
      { record: "task_revision", index: 1, taskId: "task-alpha", revision: 1, sha256: sha("1"), previousSha256: null, waitIndex: null, intentSha256: null },
      {
        record: "execution_agent",
        index: 1,
        stateId: "architect",
        executionRole: "stage",
        phase: "cleanup_completed",
        iterationIndex: 1,
        stageTask: null,
      },
      {
        record: "execution_agent",
        index: 2,
        stateId: "dev-entry",
        executionRole: "stage",
        phase: "cleanup_completed",
        iterationIndex: 1,
        stageTask: {
          taskId: "task-alpha",
          taskRevision: 1,
          taskManifestSha256: sha("1"),
          bodySha256: sha("a"),
        },
      },
      { record: "agent_output", executionIndex: 1, id: "report", digest: sha("a") },
    ];
    if (overrides.includeStageTask !== false) {
      records.push({
        record: "execution_agent",
        index: 3,
        stateId: "dev-entry-two",
        executionRole: "stage",
        phase: "cleanup_completed",
        iterationIndex: 2,
        stageTask: {
          taskId: "task-alpha",
          taskRevision: 1,
          taskManifestSha256: sha("1"),
          bodySha256: taskBodySha,
        },
      });
    } else {
      for (const record of records) {
        if (record["record"] === "execution_agent") {
          record["stageTask"] = null;
        }
      }
    }
    if (overrides.includePlanPointer !== false) {
      records.push({ record: "plan_revision", index: 2, revision: 2, sha256: sha("4"), previousSha256: sha("3"), originExecution: 3 });
    }
    const state = createPlanTasksState(RUN, 4);
    return {
      ...state,
      identity: Object.freeze({
        records: Object.freeze(records as never[]),
        cursor: 99,
        nextAfter: null,
        loaded: true,
      }),
    };
  }
  function taskState(bodySha: string) {
    const state = fixtureState({ taskBodySha: bodySha });
    return {
      ...state,
      taskTarget: {
        kind: "task",
        key: "task:task-alpha:1",
        taskId: "task-alpha",
        revision: 1,
        epoch: 1,
        ledgerSha256: sha("1"),
        ledgerPreviousSha256: null,
        status: "ready",
        envelope: Object.freeze({ manifestSha256: sha("1"), byteCount: 5, bodySha256: bodySha }),
        cursor: 5,
        readerId: 2,
        chunkCount: 1,
        retainedBytes: 5,
        decodedText: "alpha",
        decoder: null,
        terminal: true,
        truncated: false,
        integrity: "verified",
        verifiedDigest: bodySha,
        failure: null,
      },
    };
  }
  test("pending stream and pending integrity render the honest pending forms", () => {
    const state = fixtureState();
    const partial = { ...state, identity: { ...state.identity, nextAfter: 3 } };
    const derived = derivePlanTasksView(partial);
    expect(derived.complete).toBe(false);
    expect(derived.taskTarget).toBeNull();
    expect(derived.planTarget).toBeNull();
    const loaded = taskState(sha("b"));
    const pendingState = {
      ...loaded,
      identity: { ...loaded.identity, nextAfter: 7 },
    };
    const pendingView = derivePlanTasksView(pendingState);
    expect(pendingView.taskTarget?.durable).toBe("pending_stream");
    expect(pendingView.taskTarget?.text).toBe("alpha");
  });
  test("D5/D6/D7 hold together with the byte-level digest of the production owner", async () => {
    const bodyBytes = TEXT_ENCODER.encode("alpha body");
    const realDigest = stageTaskBodyDigest(bodyBytes);
    const state = taskState(realDigest);
    const derived = derivePlanTasksView(state);
    const target = derived.taskTarget;
    expect(target?.status).toBe("ready");
    expect(target?.integrity).toBe("verified");
    expect(target?.durable).toBe("verified");
    const wrongBytes = TEXT_ENCODER.encode("alpha body X");
    expect(await verifyTaskBodyDigest(wrongBytes, realDigest)).toBeNull();
    expect(await verifyTaskBodyDigest(bodyBytes, realDigest)).toBe(realDigest);
  });
  test("runs with the body published only when the digest verified (unverified truncated)", () => {
    const state = fixtureState();
    const truncated = {
      ...state,
      taskTarget: {
        kind: "task",
        key: "task:task-alpha:1",
        taskId: "task-alpha",
        revision: 1,
        epoch: 1,
        ledgerSha256: sha("1"),
        ledgerPreviousSha256: null,
        status: "truncated",
        envelope: Object.freeze({ manifestSha256: sha("1"), byteCount: S6_BODY_RETAINED_BYTES_CAP + 5, bodySha256: sha("7") }),
        cursor: S6_BODY_RETAINED_BYTES_CAP,
        readerId: 2,
        chunkCount: 4,
        retainedBytes: S6_BODY_RETAINED_BYTES_CAP,
        decodedText: "partial preview",
        decoder: null,
        terminal: true,
        truncated: true,
        integrity: "pending",
        verifiedDigest: null,
        failure: null,
      },
    };
    const view = derivePlanTasksView(truncated);
    expect(view.taskTarget?.integrity).toBe("unverified_truncated");
    expect(view.taskTarget?.text).toBe("partial preview");
    expect(view.taskTarget?.durable).toBe("not_verified");
  });
  test("D5 detects the byte drift between the artifact and the ledger binding", () => {
    const base = taskState(sha("b"));
    const target = base.taskTarget as Record<string, unknown>;
    const drifted = {
      ...base,
      taskTarget: {
        ...target,
        verifiedDigest: sha("f"),
        envelope: Object.freeze({ ...(target.envelope as Record<string, unknown>), bodySha256: sha("f") }),
      },
    };
    const view = derivePlanTasksView(drifted);
    expect(view.taskTarget?.durable).toBe("failed");
  });
  test("D4 renders absent when no stage execution binds the selected pair", () => {
    const state = taskState(sha("b"));
    const stripped = fixtureState({ includeStageTask: false });
    void state;
    const noBindings = {
      ...stripped,
      taskTarget: state.taskTarget,
    };
    const view = derivePlanTasksView(noBindings);
    expect(view.taskTarget?.durable).toBe("absent");
  });
  test("D6/D7 pointer verification over the plan target", () => {
    const planRecords = [
      { record: "stage", stagePosition: 1, id: "stage-one", template: "development" },
      { record: "task", stagePosition: 1, taskPosition: 1, id: "task-alpha", revision: 1, sha256: sha("1") },
    ];
    const state = fixtureState() as Record<string, unknown>;
    const planState = {
      ...state,
      planTarget: {
        kind: "plan",
        key: "plan:1",
        revision: 1,
        epoch: 1,
        ledgerSha256: sha("3"),
        ledgerPreviousSha256: null,
        ledgerOriginExecution: 1,
        status: "ready",
        envelope: Object.freeze({ manifestSha256: sha("3"), previousSha256: null, originExecution: 1 }),
        cursor: planRecords.length,
        readerId: 1,
        inFlightLimit: 100,
        records: planRecords,
        terminal: true,
        truncated: false,
        failure: null,
      },
    };
    expect(derivePlanTasksView(planState).planTarget?.pointerStatus).toBe("verified");
    const mismatchState = {
      ...planState,
      planTarget: {
        ...(planState.planTarget as Record<string, unknown>),
        records: [{ record: "task", stagePosition: 1, taskPosition: 1, id: "task-alpha", revision: 1, sha256: sha("b") }],
      },
    };
    const mismatch = derivePlanTasksView(mismatchState);
    expect(mismatch.planTarget?.pointerStatus).toBe("failed");
    expect(mismatch.taskTarget).toBeNull();
  });
});

describe("plan/tasks S6: the WebCrypto body verification", () => {
  const TEXT = "alpha\nbeta\n";
  test("reproduces the production stage-task body digest exactly", async () => {
    const bytes = TEXT_ENCODER.encode(TEXT);
    const hex = await verifyTaskBodyDigest(bytes, stageTaskBodyDigest(bytes));
    expect(hex).toBe(stageTaskBodyDigest(bytes));
    const modified = TEXT_ENCODER.encode(TEXT + "!");
    expect(await verifyTaskBodyDigest(modified, stageTaskBodyDigest(bytes))).toBeNull();
  });
  test("refuses hostiles and rethrows webcrypto absence", async () => {
    const expectReject = async (label: string, promise: Promise<unknown>) => {
      try {
        await promise;
        throw new Error(`unreachable: ${label}`);
      } catch (cause) {
        expect((cause as UiContractError).name, label).toBe("UiContractError");
      }
    };
    await expectReject("bytes hostile", verifyTaskBodyDigest("no" as never, sha("7")));
    await expectReject("digest form hostile", verifyTaskBodyDigest(TEXT_ENCODER.encode(TEXT), "not-a-sha" as never));
  });
  test("the empty body digests deterministically", async () => {
    const empty = TEXT_ENCODER.encode("");
    const hex = await verifyTaskBodyDigest(empty, stageTaskBodyDigest(empty));
    expect(hex).toBe(stageTaskBodyDigest(empty));
  });
});

describe("plan/tasks S6: source restrictions", () => {
  test("the shared stream factory owns the two content transports exactly once", () => {
    expect(appSource.split('createStreamTransport("fetchTaskContent"').length - 1).toBe(1);
    expect(appSource.split('createStreamTransport("fetchPlanContent"').length - 1).toBe(1);
  });
  test("the body digest domain appears exactly once (the browser recomputation)", () => {
    expect(appSource.split("pipeline-v2-stage-task-body").length - 1).toBe(1);
  });
  test("the section consumes no timers and no mutating verbs", () => {
    expect(appSource.includes("setInterval")).toBe(false);
    expect(appSource.split('method: "POST"').length - 1).toBe(2);
  });
});

describe("plan/tasks S6: the viewer state machine (smoke over fake transports)", () => {
  const flush = () => new Promise((resolve) => setTimeout(resolve, 0));
  function identityCompleting(result: unknown) {
    return createIdentityTransport({
      fetchIdentity: async () => result,
      onStarted: () => {},
      onSettled: () => {},
    } as never);
  }
  function noopContentIo(fetchKey: string) {
    return {
      [fetchKey]: async () => undefined,
      onStarted: () => {},
      onSettled: () => {},
    };
  }
  function viewerOver(identityResult: unknown, taskFetch?: (path: string, runId: string, signal: AbortSignal, binding: unknown) => unknown, planFetch?: (path: string, runId: string, signal: AbortSignal, binding: unknown) => unknown) {
    const states: Array<unknown> = [];
    const viewer = createPlanTasksViewer({
      identityTransport: identityCompleting(identityResult),
      taskTransport: createTaskContentTransport({
        fetchTaskContent: taskFetch ?? (async () => undefined),
        onStarted: () => {},
        onSettled: () => {},
      } as never),
      planTransport: createPlanContentTransport({
        fetchPlanContent: planFetch ?? (async () => undefined),
        onStarted: () => {},
        onSettled: () => {},
      } as never),
      onState: (state: unknown) => {
        states.push(state);
      },
    });
    return { viewer, states };
  }
  test("inspectApplied issues the initial pinned request; a failed outcome clears the state", async () => {
    const { viewer, states } = viewerOver(undefined);
    viewer.inspectApplied({ kind: "run", runId: RUN, revision: 1 });
    expect(states.length).toBeGreaterThanOrEqual(1);
    await flush();
    const current = viewer.currentState() as Record<string, unknown> | null;
    expect(current?.staleCleared).toBe(true);
    viewer.dispose();
    expect(viewer.currentState()).toBeNull();
  });
  test("a stale-snapshot error clears the whole view and waits", async () => {
    const { viewer } = viewerOver({
      ok: true,
      model: { schema_version: 1, kind: "error", layer: "request", reason: "stale_snapshot" },
    });
    viewer.inspectApplied({ kind: "run", runId: RUN, revision: 1 });
    await flush();
    const current = viewer.currentState();
    expect(current?.staleCleared).toBe(true);
    expect(current?.identity.records).toEqual([]);
    viewer.dispose();
  });
  test("selectTask with an unresolved ledger row stays inert (no task transport call)", async () => {
    let taskRequests = 0;
    const { viewer } = viewerOver(undefined, async () => {
      taskRequests += 1;
      return undefined;
    });
    viewer.inspectApplied({ kind: "run", runId: RUN, revision: 1 });
    await flush();
    viewer.selectTask("task-alpha", 1);
    expect(taskRequests).toBe(0);
    viewer.dispose();
  });
  test("the completed ledger opens the pinned selection over the fake task stream", async () => {
    const realDigest = stageTaskBodyDigest(TEXT_ENCODER.encode("alpha"));
    const identityRecords = [
      { record: "plan_revision", index: 1, revision: 1, sha256: sha("3"), previousSha256: null, originExecution: 1 },
      { record: "task_revision", index: 1, taskId: "task-alpha", revision: 1, sha256: sha("1"), previousSha256: null, waitIndex: null, intentSha256: null },
    ];
    const archived = taskDocument(
      { manifest_sha256: sha("1"), previous_sha256: null, revision: 1 },
      { byte_count: 5, returned_byte_count: 5, data_base64: b64("alpha"), sha256: realDigest },
    );
    const { viewer } = viewerOver(
      { ok: true, model: { schema_version: 1, kind: "identity", revision: 1, after: 0, records: identityRecords, nextAfter: null } },
      async (_path, _runId, _signal, binding) => ({
        ok: true,
        model: parseTaskContentDocument(archived, binding as typeof TASK_BINDING),
      }),
    );
    viewer.inspectApplied({ kind: "run", runId: RUN, revision: 1 });
    await flush();
    viewer.selectTask("task-alpha", 1);
    await flush();
    await flush();
    const current = viewer.currentState() as Record<string, unknown> | null;
    const target = current?.taskTarget as Record<string, unknown> | null;
    expect(target).not.toBeNull();
    expect(target?.status).toBe("ready");
    expect(target?.integrity).toBe("verified");
    expect(target?.verifiedDigest).toBe(realDigest);
    expect(derivePlanTasksView(current as never).taskTarget?.durable).toBe("absent");
    viewer.dispose();
  });
});
