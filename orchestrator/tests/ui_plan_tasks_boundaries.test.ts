/**
 * Permanent boundary and multi-binding suite of the browser UI
 * "Plan and tasks" slice (S6 corrective): the body-retention cap must
 * be reachable THROUGH the real viewer flow (a body above
 * `S6_BODY_RETAINED_BYTES_CAP` ends in the typed truncated view after
 * the cap-boundary chunk, and a body of exactly the cap completes and
 * verifies normally), and the durable task evidence must fail closed
 * unless EVERY `stage_task` binding of the selected ledger pair agrees
 * with the verified envelope on both the manifest digest and the body
 * digest — one agreeing binding must never hide a conflicting one, and
 * a fully-foreign manifest is conflict evidence, not absence.
 */
import { describe, expect, test } from "bun:test";
import {
  createIdentityTransport,
  createPlanContentTransport,
  createPlanTasksViewer,
  createTaskContentTransport,
  derivePlanTasksView,
  parseIdentityDocument,
  parseTaskContentDocument,
  S6_BODY_RETAINED_BYTES_CAP,
} from "../ui/app.js";
import { stageTaskBodyDigest } from "../src/pipeline_v2_stage_task.ts";

const RUN = "run-pt-cap";
const TASK_ID = "task-alpha";
const TASK_REVISION = 1;
const CHUNK = 65536;

const sha = (char: string) => char.repeat(64);
const TEXT_ENCODER = new TextEncoder();

interface ViewerLike {
  inspectApplied(model: Record<string, unknown>): void;
  selectTask(taskId: string, revision: number): void;
  loadMoreTaskBody(): void;
  currentState(): Record<string, unknown> | null;
  dispose(): void;
}

interface TaskTargetLike {
  status: string;
  truncated: boolean;
  terminal: boolean;
  retainedBytes: number;
  byteCount: number | null;
  integrity: string;
  durable: string;
  text: string | null;
}

const taskTargetOf = (viewer: ViewerLike): TaskTargetLike => {
  const current = viewer.currentState();
  if (current === null || current["taskTarget"] === null) {
    throw new Error("unreachable: no task target");
  }
  return derivePlanTasksView(current as never).taskTarget as unknown as TaskTargetLike;
};

function agreeingIdentityRecords(bodySha256: string): unknown[] {
  return [
    { record: "plan_revision", index: 1, revision: 1, sha256: sha("3"), previous_sha256: null, origin_execution: 1 },
    { record: "task_revision", index: 1, task_id: TASK_ID, revision: TASK_REVISION, sha256: sha("1"), previous_sha256: null, wait_index: null, intent_sha256: null },
    {
      record: "execution_agent",
      index: 2,
      type: "agent",
      state_id: "dev-entry",
      execution_role: "stage",
      iteration_index: 1,
      phase: "cleanup_completed",
      stage_task: { task_id: TASK_ID, task_revision: TASK_REVISION, task_manifest_sha256: sha("1"), body_sha256: bodySha256 },
    },
    {
      record: "execution_agent",
      index: 3,
      type: "agent",
      state_id: "dev-entry-two",
      execution_role: "stage",
      iteration_index: 2,
      phase: "cleanup_completed",
      stage_task: { task_id: TASK_ID, task_revision: TASK_REVISION, task_manifest_sha256: sha("1"), body_sha256: bodySha256 },
    },
  ];
}

function chunkFetcher(bodyBytes: Uint8Array, issued: { paths: string[] }) {
  const fullDigest = stageTaskBodyDigest(bodyBytes);
  return (_path: string, _runId: string, _signal: unknown, binding: unknown) => {
    const url = new URL("https://ui.invalid" + (_path as string));
    const offset = Number(url.searchParams.get("offset"));
    const limit = Number(url.searchParams.get("limit"));
    issued.paths.push(_path);
    const slice = bodyBytes.subarray(offset, offset + limit);
    const next = offset + slice.length;
    const document = {
      schema_version: 1,
      kind: "task_revision",
      run_id: RUN,
      task_id: TASK_ID,
      revision: TASK_REVISION,
      manifest_sha256: sha("1"),
      previous_sha256: null,
      origin: "planning_proposal",
      body: {
        byte_count: bodyBytes.length,
        sha256: fullDigest,
        offset,
        limit,
        returned_byte_count: slice.length,
        next_offset: next >= bodyBytes.length ? null : next,
        data_base64: Buffer.from(slice).toString("base64"),
      },
    };
    return { ok: true, model: parseTaskContentDocument(document, binding as Record<string, unknown>) };
  };
}

function viewerFor(bodySha256: string, fetchTaskContent: (path: string, runId: string, signal: unknown, binding: unknown) => unknown): ViewerLike {
  const page = parseIdentityDocument(
    {
      schema_version: 1,
      kind: "identity",
      run_id: RUN,
      revision: 1,
      after: 0,
      records: agreeingIdentityRecords(bodySha256),
      next_after: null,
    },
    RUN,
  );
  const viewer = createPlanTasksViewer({
    identityTransport: createIdentityTransport({
      fetchIdentity: async () => ({
        ok: true,
        model: page,
      }),
      onStarted: () => {},
      onSettled: () => {},
    } as never),
    taskTransport: createTaskContentTransport({
      fetchTaskContent: fetchTaskContent as never,
      onStarted: () => {},
      onSettled: () => {},
    } as never),
    planTransport: createPlanContentTransport({
      fetchPlanContent: async () => undefined,
      onStarted: () => {},
      onSettled: () => {},
    } as never),
    onState: () => {},
  } as never) as unknown as ViewerLike;
  return viewer;
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

/** Drives the real viewer to the end of the stream (or to its
 * [non-]progress ceiling): one explicit load-more per settled chunk. */
async function streamToEnd(viewer: ViewerLike): Promise<void> {
  viewer.inspectApplied({ kind: "run", runId: RUN, revision: 1 });
  await flush();
  viewer.selectTask(TASK_ID, TASK_REVISION);
  await flush();
  await flush();
  for (let click = 0; click < 8; click += 1) {
    const target = viewer.currentState() !== null ? (viewer.currentState() as Record<string, unknown>)["taskTarget"] as Record<string, unknown> : null;
    if (target === null || target["status"] !== "streaming") {
      break;
    }
    viewer.loadMoreTaskBody();
    await flush();
    await flush();
  }
  await flush();
  await flush();
}

describe("plan/tasks S6 corrective: the body-retention cap through the real viewer", () => {
  test("a body of exactly the retained cap completes, verifies and publishes", async () => {
    const exact = new Uint8Array(S6_BODY_RETAINED_BYTES_CAP);
    exact.forEach((_byte, index) => {
      exact[index] = 33 + (index % 90);
    });
    const issued = { paths: [] as string[] };
    const viewer = viewerFor(stageTaskBodyDigest(exact), chunkFetcher(exact, issued));
    try {
      await streamToEnd(viewer);
      const target = taskTargetOf(viewer);
      expect(target.status).toBe("ready");
      expect(target.truncated).toBe(false);
      expect(target.retainedBytes).toBe(S6_BODY_RETAINED_BYTES_CAP);
      expect(target.byteCount).toBe(S6_BODY_RETAINED_BYTES_CAP);
      expect(target.integrity).toBe("verified");
      expect(target.durable).toBe("verified");
      expect(target.text?.length).toBe(S6_BODY_RETAINED_BYTES_CAP);
      // four chunks of 65536; no continuation is issued after the
      // wire terminal
      expect(issued.paths).toHaveLength(4);
      expect(issued.paths.every((path) => new URL(`https://ui.invalid${path}`).searchParams.get("limit") === String(CHUNK))).toBe(true);
    } finally {
      viewer.dispose();
    }
  });

  test("a body one byte over the cap ends truncated at the cap; load-more is a closed no-op", async () => {
    const over = new Uint8Array(S6_BODY_RETAINED_BYTES_CAP + 1);
    over.forEach((_byte, index) => {
      over[index] = 33 + (index % 90);
    });
    const issued = { paths: [] as string[] };
    const viewer = viewerFor(stageTaskBodyDigest(over), chunkFetcher(over, issued));
    try {
      await streamToEnd(viewer);
      const target = taskTargetOf(viewer);
      expect(target.status).toBe("truncated");
      expect(target.truncated).toBe(true);
      expect(target.integrity).toBe("unverified_truncated");
      expect(target.durable).toBe("not_verified");
      expect(target.byteCount).toBe(S6_BODY_RETAINED_BYTES_CAP + 1);
      expect(target.retainedBytes).toBe(S6_BODY_RETAINED_BYTES_CAP);
      // the preview publishes the retained prefix, never the full body
      expect(target.text?.length).toBe(S6_BODY_RETAINED_BYTES_CAP);
      // the cap-boundary chunk is the last issued GET; further load-more
      // presses issue nothing
      expect(issued.paths).toHaveLength(4);
      const before = issued.paths.length;
      viewer.loadMoreTaskBody();
      await flush();
      expect(issued.paths).toHaveLength(before);
      const targetAfter = taskTargetOf(viewer);
      expect(targetAfter.status).toBe("truncated");
    } finally {
      viewer.dispose();
    }
  });
});

/** The wire record list of one derivation fixture parsed through the
 * production page parser, so the derivation battery runs over exactly
 * the record shapes the real viewer state carries. */
function parsedIdentity(records: unknown[]): unknown[] {
  return parseIdentityDocument(
    { schema_version: 1, kind: "identity", run_id: RUN, revision: 1, after: 0, records, next_after: null },
    RUN,
  )["records"] as unknown[];
}

function deriveOver(records: unknown[], overrides: Record<string, unknown> = {}): TaskTargetLike {
  const target = {
    kind: "task",
    key: `task:${TASK_ID}:1`,
    taskId: TASK_ID,
    revision: 1,
    epoch: 1,
    ledgerSha256: sha("1"),
    ledgerPreviousSha256: null,
    status: "ready",
    envelope: { manifestSha256: sha("1"), previousSha256: null, origin: "planning_proposal", byteCount: 5, bodySha256: sha("b") },
    cursor: 5,
    readerId: 2,
    chunkCount: 1,
    retainedBytes: 5,
    decodedText: "alpha",
    decoder: null,
    terminal: true,
    truncated: false,
    integrity: "verified",
    verifiedDigest: sha("b"),
    failure: null,
    ...overrides,
  };
  const state = {
    runId: RUN,
    pin: 1,
    staleCleared: false,
    error: null,
    identity: { records: Object.freeze(records), cursor: 99, nextAfter: null, loaded: true },
    taskTarget: target,
    planTarget: null,
  };
  return derivePlanTasksView(state as never).taskTarget as unknown as TaskTargetLike;
}

describe("plan/tasks S6 corrective: the durable task evidence over every binding", () => {
  test("every binding of the pair agreeing with the envelope verifies", () => {
    const view = deriveOver(parsedIdentity(agreeingIdentityRecords(sha("b"))));
    expect(view.durable).toBe("verified");
  });

  test("one agreeing binding never hides a conflicting one (fail closed)", () => {
    const wireWithConflict = [
      ...agreeingIdentityRecords(sha("b")),
      {
        record: "execution_agent",
        index: 4,
        type: "agent",
        state_id: "dev-entry-three",
        execution_role: "stage",
        iteration_index: 3,
        phase: "cleanup_completed",
        stage_task: { task_id: TASK_ID, task_revision: TASK_REVISION, task_manifest_sha256: sha("1"), body_sha256: sha("a") },
      },
    ];
    const view = deriveOver(parsedIdentity(wireWithConflict));
    expect(view.durable).toBe("failed");
  });

  test("a fully foreign manifest on every binding is conflict evidence, not absence", () => {
    const foreign = agreeingIdentityRecords(sha("b")).map((record) => {
      const entry = record as Record<string, unknown>;
      if (entry["record"] !== "execution_agent" || entry["stage_task"] === null) {
        return record;
      }
      const stageTask = entry["stage_task"] as Record<string, unknown>;
      return { ...entry, stage_task: { ...stageTask, task_manifest_sha256: sha("9") } };
    });
    const view = deriveOver(parsedIdentity(foreign));
    expect(view.durable).toBe("failed");
  });

  test("zero bindings for the pair stays the absence statement", () => {
    const stripped = agreeingIdentityRecords(sha("b")).map((record) => {
      const entry = record as Record<string, unknown>;
      if (entry["record"] !== "execution_agent") {
        return record;
      }
      return { ...entry, stage_task: null };
    });
    const view = deriveOver(parsedIdentity(stripped));
    expect(view.durable).toBe("absent");
  });
});
