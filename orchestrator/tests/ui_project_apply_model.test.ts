/**
 * The pure model battery of the Browser UI v8 explicit durable Project
 * Apply panel: the operation parser, the URL/body builders, the closed
 * eligibility owner, the recovery-record storage owner and the source
 * scans. No DOM, no fetch — only the frozen contracts.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  APPLY_RECORD_STORAGE_PREFIX,
  PROJECT_APPLY_INELIGIBILITY_REASONS,
  PROJECT_APPLY_OPERATION_STATUSES,
  PROJECT_APPLY_UI_STATES,
  createApplyRecoveryStore,
  formatFailure,
  parseApplyRecoveryRecord,
  parseApiDocument,
  parseProjectApplyOperationDocument,
  projectApplyEligibility,
  projectApplyPostBody,
  projectApplyRequestPath,
  UiContractError,
} from "../ui/app.js";
import { PIPELINE_V2_APPLY_OPERATION_STATUSES } from "../src/pipeline_v2_apply_state_internal.ts";

const OBS = "a".repeat(64);
const OBS2 = "b".repeat(64);

function contractReason(fn: () => unknown): string {
  try {
    fn();
    return "accepted";
  } catch (error) {
    return error instanceof UiContractError ? error.reason : `not-typed: ${String(error)}`;
  }
}

function operationDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "project_apply_operation",
    run_id: "run-a",
    apply_id: OBS,
    project_id: "project-a",
    observation: OBS,
    revision: 7,
    status: "applying",
    completed_step_count: 2,
    total_step_count: 5,
    ...overrides,
  };
}

const EXPECTED = { runId: "run-a", applyId: OBS, observation: OBS };

describe("project apply model: the operation document parser", () => {
  test("the exact document parses into the frozen projection", () => {
    const model = parseProjectApplyOperationDocument(operationDocument(), EXPECTED);
    expect(model).toEqual({
      kind: "project_apply_operation",
      runId: "run-a",
      applyId: OBS,
      projectId: "project-a",
      observation: OBS,
      revision: 7,
      status: "applying",
      completedStepCount: 2,
      totalStepCount: 5,
    });
    expect(Object.isFrozen(model)).toBe(true);
    expect(parseProjectApplyOperationDocument(operationDocument(), EXPECTED)).toEqual(model);
  });

  test("every durable operation status is accepted under its exact name", () => {
    for (const status of PROJECT_APPLY_OPERATION_STATUSES) {
      expect(parseProjectApplyOperationDocument(operationDocument({ status }), EXPECTED).status).toBe(status);
    }
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ status: "ghost_status" }), EXPECTED))).toBe("invalid_shape");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ status: 7 }), EXPECTED))).toBe("invalid_shape");
  });

  test("the response is bound to the exact run id, apply id and observation", () => {
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ run_id: "run-b" }), EXPECTED))).toBe("unexpected_run");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ apply_id: OBS2 }), EXPECTED))).toBe("apply_id_mismatch");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ observation: OBS2 }), EXPECTED))).toBe("observation_mismatch");
    expect(
      contractReason(() => parseProjectApplyOperationDocument(operationDocument(), { runId: "run-a", applyId: OBS, observation: OBS2 })),
    ).toBe("observation_mismatch");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument(), null as never))).toBe("invalid_request");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument(), { runId: "run-a", applyId: OBS, observation: "nothex" }))).toBe(
      "invalid_request",
    );
  });

  test("unknown fields, wrong kind and version fail closed without echoing values", () => {
    expect(contractReason(() => parseProjectApplyOperationDocument({ ...operationDocument(), extra: 1 }, EXPECTED))).toBe("invalid_shape");
    expect(contractReason(() => parseProjectApplyOperationDocument({ ...operationDocument(), temp_name: ".apply-tmp-x" }, EXPECTED))).toBe("invalid_shape");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ kind: "project_changes" }), EXPECTED))).toBe("unknown_kind");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ schema_version: 2 }), EXPECTED))).toBe("unknown_schema_version");
    expect(contractReason(() => parseProjectApplyOperationDocument("nope", EXPECTED))).toBe("not_a_document");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ project_id: "bad id!" }), EXPECTED))).toBe("invalid_shape");
  });

  test("safe-integer and coherence boundaries of the step counters", () => {
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ revision: 0 }), EXPECTED))).toBe("invalid_shape");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ revision: 1.5 }), EXPECTED))).toBe("invalid_shape");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ revision: -1 }), EXPECTED))).toBe("invalid_shape");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ completed_step_count: -1 }), EXPECTED))).toBe("invalid_shape");
    expect(contractReason(() => parseProjectApplyOperationDocument(operationDocument({ completed_step_count: 6 }), EXPECTED))).toBe("invalid_shape");
    expect(parseProjectApplyOperationDocument(operationDocument({ completed_step_count: 5, total_step_count: 5 }), EXPECTED).completedStepCount).toBe(5);
    expect(parseProjectApplyOperationDocument(operationDocument({ completed_step_count: 0, total_step_count: 0 }), EXPECTED).totalStepCount).toBe(0);
  });

  test("the UI status vocabulary mirrors the production owner exactly", () => {
    expect([...PROJECT_APPLY_OPERATION_STATUSES]).toEqual([...PIPELINE_V2_APPLY_OPERATION_STATUSES]);
    expect(Object.isFrozen(PROJECT_APPLY_OPERATION_STATUSES)).toBe(true);
    expect(Object.isFrozen(PROJECT_APPLY_UI_STATES)).toBe(true);
    // the durable statuses keep their exact names inside the UI union
    for (const status of PROJECT_APPLY_OPERATION_STATUSES) {
      expect(PROJECT_APPLY_UI_STATES.includes(status)).toBe(true);
    }
  });
});

describe("project apply model: the general client keeps rejecting the apply kind", () => {
  test("parseApiDocument rejects project_apply_operation fail closed", () => {
    expect(contractReason(() => parseApiDocument(operationDocument()))).toBe("unknown_kind");
    expect(contractReason(() => parseApiDocument({ schema_version: 1, kind: "project_apply_record", run_id: "r" }))).toBe("unknown_kind");
  });
});

describe("project apply model: URL and body builders", () => {
  test("the exact operation path carries only the two safe ids", () => {
    expect(projectApplyRequestPath("run-a", OBS)).toBe(`/v1/runs/run-a/project-applies/${OBS}`);
    expect(contractReason(() => projectApplyRequestPath("bad/id", OBS))).toBe("invalid_request");
    expect(contractReason(() => projectApplyRequestPath("run-a", "bad id!"))).toBe("invalid_request");
    expect(contractReason(() => projectApplyRequestPath(null as never, OBS))).toBe("invalid_request");
  });

  test("the exact POST body carries only the observation", () => {
    expect(projectApplyPostBody(OBS)).toEqual({ schema_version: 1, observation: OBS });
    expect(Object.isFrozen(projectApplyPostBody(OBS))).toBe(true);
    expect(contractReason(() => projectApplyPostBody("nothex"))).toBe("invalid_request");
    expect(contractReason(() => projectApplyPostBody(OBS.toUpperCase()))).toBe("invalid_request");
    expect(contractReason(() => projectApplyPostBody(null as never))).toBe("invalid_request");
  });
});

describe("project apply model: the closed eligibility owner", () => {
  const readyChanges = (overrides: Record<string, unknown> = {}) => ({
    requestState: "ready",
    summary: { total: 3, added: 1, deleted: 0, content_modified: 1, mode_modified: 1, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0, matches_baseline: 2, matches_result: 1, diverged: 0 },
    changes: [{}, {}, {}],
    nextAfter: null,
    ...overrides,
  });

  test("the fully eligible shape passes", () => {
    expect(
      projectApplyEligibility({
        terminalStatus: "succeeded",
        capability: true,
        changesState: readyChanges(),
        applyState: "idle",
        mutationInFlight: false,
      }),
    ).toEqual({ eligible: true, reason: null });
    expect(
      projectApplyEligibility({
        terminalStatus: "failed",
        capability: true,
        changesState: readyChanges(),
        applyState: "idle",
        mutationInFlight: false,
      }).eligible,
    ).toBe(true);
  });

  test("every closed ineligibility reason is reachable and nothing else", () => {
    const base = { terminalStatus: "succeeded", capability: true, changesState: readyChanges(), applyState: "idle", mutationInFlight: false };
    const expectReason = (options: Record<string, unknown>, reason: string) => {
      const result = projectApplyEligibility({ ...base, ...options } as never);
      expect(result).toEqual({ eligible: false, reason });
    };
    expectReason({ terminalStatus: "running" }, "run_not_terminal");
    expectReason({ terminalStatus: null }, "run_not_terminal");
    expectReason({ capability: false }, "capability_disabled");
    expectReason({ capability: undefined }, "capability_disabled");
    expectReason({ changesState: null }, "changes_not_ready");
    expectReason({ changesState: { requestState: "loading", summary: null, changes: [], nextAfter: null } }, "changes_not_ready");
    expectReason({ changesState: readyChanges({ requestState: "stale" }) }, "changes_stale");
    expectReason({ changesState: readyChanges({ nextAfter: 3 }) }, "changes_incomplete");
    expectReason({ changesState: readyChanges({ changes: [{}, {}] }) }, "changes_incomplete");
    expectReason({ changesState: readyChanges({ summary: { ...readyChanges().summary, total: 0 }, changes: [] }) }, "no_changes");
    expectReason({ changesState: readyChanges({ summary: { ...readyChanges().summary, diverged: 1 } }) }, "diverged_present");
    expectReason({ changesState: readyChanges({ summary: { ...readyChanges().summary, matches_baseline: 0 } }) }, "no_pending_changes");
    expectReason({ applyState: "confirming" }, "operation_active");
    expectReason({ applyState: "applying" }, "operation_active");
    expectReason({ applyState: "applied" }, "operation_active");
    expectReason({ applyState: "unavailable" }, "operation_active");
    expectReason({ mutationInFlight: true }, "mutation_in_flight");
    expect(contractReason(() => projectApplyEligibility(null as never))).toBe("invalid_request");
  });

  test("a partial page out of a longer history never enables the apply", () => {
    const result = projectApplyEligibility({
      terminalStatus: "succeeded",
      capability: true,
      changesState: readyChanges({ nextAfter: 100 }),
      applyState: "idle",
      mutationInFlight: false,
    });
    expect(result.reason).toBe("changes_incomplete");
  });

  test("the ineligibility vocabulary is closed and frozen", () => {
    expect(Object.isFrozen(PROJECT_APPLY_INELIGIBILITY_REASONS)).toBe(true);
    expect(PROJECT_APPLY_INELIGIBILITY_REASONS).toEqual([
      "run_not_terminal",
      "capability_disabled",
      "changes_not_ready",
      "changes_stale",
      "changes_incomplete",
      "no_changes",
      "diverged_present",
      "no_pending_changes",
      "operation_active",
      "mutation_in_flight",
    ]);
  });
});

describe("project apply model: the recovery-record storage owner", () => {
  function mapStorage(map: Map<string, string>): Record<string, unknown> {
    return {
      getItem: (key: string): string | null => (map.has(key) ? (map.get(key) as string) : null),
      setItem: (key: string, value: string): void => {
        map.set(key, value);
      },
      removeItem: (key: string): void => {
        map.delete(key);
      },
    };
  }

  class ThrowingStorage {
    getItem(): string | null {
      throw new Error("boom");
    }
    setItem(): void {
      throw new Error("boom");
    }
    removeItem(): void {
      throw new Error("boom");
    }
  }

  test("the exact record round-trips under the closed namespace", () => {
    const map = new Map<string, string>();
    const store = createApplyRecoveryStore(mapStorage(map));
    expect(store.save("run-a", OBS)).toEqual({ saved: true, error: null });
    expect(map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    expect(JSON.parse(map.get(`${APPLY_RECORD_STORAGE_PREFIX}run-a`) as string)).toEqual({
      schema_version: 1,
      run_id: "run-a",
      apply_id: OBS,
      observation: OBS,
    });
    expect(store.load("run-a")).toEqual({ record: { runId: "run-a", applyId: OBS, observation: OBS }, error: null });
    expect(store.remove("run-a")).toBe(true);
    expect(store.load("run-a")).toEqual({ record: null, error: null });
  });

  test("a second save while a record exists is refused — the identity is pinned", () => {
    const map = new Map<string, string>();
    const store = createApplyRecoveryStore(mapStorage(map));
    expect(store.save("run-a", OBS).saved).toBe(true);
    expect(store.save("run-a", OBS2)).toEqual({ saved: false, error: "record_exists" });
    expect(JSON.parse(map.get(`${APPLY_RECORD_STORAGE_PREFIX}run-a`) as string).observation).toBe(OBS);
  });

  test("a corrupted record is surfaced, never silently deleted", () => {
    const map = new Map<string, string>();
    const store = createApplyRecoveryStore(mapStorage(map));
    map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, "{not json");
    expect(store.load("run-a")).toEqual({ record: null, error: "corrupt_record" });
    expect(map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-b`, JSON.stringify({ schema_version: 1, run_id: "run-b", apply_id: OBS }));
    expect(store.load("run-b")).toEqual({ record: null, error: "corrupt_record" });
    expect(map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-b`)).toBe(true);
  });

  test("an unavailable storage refuses every write and never starts an apply", () => {
    const store = createApplyRecoveryStore(null);
    expect(store.load("run-a")).toEqual({ record: null, error: "storage_unavailable" });
    expect(store.save("run-a", OBS)).toEqual({ saved: false, error: "storage_unavailable" });
    expect(store.remove("run-a")).toBe(false);
    const throwing = createApplyRecoveryStore(new ThrowingStorage());
    expect(throwing.load("run-a")).toEqual({ record: null, error: "storage_unavailable" });
    expect(throwing.save("run-a", OBS)).toEqual({ saved: false, error: "record_unwritable" });
  });

  test("a read-back mismatch refuses the write", () => {
    let corruptReads = 0;
    const map = new Map<string, string>();
    const storage = {
      getItem: (key: string): string | null => {
        const value = map.get(key);
        if (value === undefined) {
          return null;
        }
        if (corruptReads > 0) {
          corruptReads -= 1;
          return value.replace("1", "9");
        }
        return value;
      },
      setItem: (key: string, value: string): void => {
        map.set(key, value);
      },
      removeItem: (key: string): void => {
        map.delete(key);
      },
    };
    const store = createApplyRecoveryStore(storage);
    corruptReads = 1;
    expect(store.save("run-a", OBS)).toEqual({ saved: false, error: "record_unwritable" });
    corruptReads = 0;
    expect(store.save("run-a", OBS)).toEqual({ saved: false, error: "record_exists" });
  });

  test("the record parser is exact and hostile shapes fail closed", () => {
    expect(parseApplyRecoveryRecord({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS })).toEqual({
      runId: "run-a",
      applyId: OBS,
      observation: OBS,
    });
    expect(contractReason(() => parseApplyRecoveryRecord({ schema_version: 2, run_id: "run-a", apply_id: OBS, observation: OBS }))).toBe("unknown_schema_version");
    expect(contractReason(() => parseApplyRecoveryRecord({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS, extra: 1 }))).toBe("invalid_shape");
    expect(contractReason(() => parseApplyRecoveryRecord({ schema_version: 1, run_id: "run-a", apply_id: OBS }))).toBe("invalid_shape");
    expect(contractReason(() => parseApplyRecoveryRecord({ schema_version: 1, run_id: "bad/id", apply_id: OBS, observation: OBS }))).toBe("invalid_shape");
    expect(contractReason(() => parseApplyRecoveryRecord({ schema_version: 1, run_id: "run-a", apply_id: "nothex", observation: OBS }))).toBe("invalid_shape");
    expect(contractReason(() => parseApplyRecoveryRecord("nope"))).toBe("not_a_document");
    expect(contractReason(() => parseApplyRecoveryRecord(null))).toBe("not_a_document");
  });

  test("invalid ids and observations are refused before any storage access", () => {
    const store = createApplyRecoveryStore(mapStorage(new Map<string, string>()));
    expect(contractReason(() => store.save("bad/id", OBS))).toBe("invalid_run_id");
    expect(contractReason(() => store.save("run-a", "nothex"))).toBe("invalid_request");
    expect(contractReason(() => store.load("bad/id"))).toBe("invalid_run_id");
    expect(contractReason(() => createApplyRecoveryStore({ getItem: 1 }))).toBe("invalid_storage");
  });

  test("the stored record carries no path, content or credential canary", () => {
    const map = new Map<string, string>();
    const store = createApplyRecoveryStore(mapStorage(map));
    store.save("run-a", OBS);
    const raw = map.get(`${APPLY_RECORD_STORAGE_PREFIX}run-a`) as string;
    for (const banned of ["/opt/", "/home/", "path", "payload", "content", "token", "credential", "sha256", "run_root"]) {
      expect(raw.toLowerCase().includes(banned)).toBe(false);
    }
  });
});

describe("project apply model: source scans", () => {
  const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");

  test("the status vocabulary is defined exactly once in the UI module", () => {
    expect(appSource.split('const PROJECT_APPLY_OPERATION_STATUSES = Object.freeze([').length - 1).toBe(1);
    expect(appSource.split('const PROJECT_APPLY_UI_STATES = Object.freeze([').length - 1).toBe(1);
    expect(appSource.split("const PROJECT_APPLY_OPERATION_KEYS = [").length - 1).toBe(1);
  });

  test("no raw paths, apply-store roots or temp identities in the browser module", () => {
    for (const banned of ["/opt/", "/home/", "applyStoreRoot", ".apply-tmp", "state.json", "payload_base64", "dev, ino"]) {
      expect(appSource.includes(banned)).toBe(false);
    }
  });

  test("the apply region owns no timer, no polling and no automatic retry", () => {
    const start = appSource.indexOf("Project apply (v8)");
    const end = appSource.indexOf("Project change content (v7)");
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    const region = appSource.slice(start, end);
    for (const banned of ["setTimeout", "setInterval", "EventSource", "WebSocket", "backoff", "innerHTML", "document.write"]) {
      expect(region.includes(banned)).toBe(false);
    }
  });
});
