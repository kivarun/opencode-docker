/**
 * The state-machine battery of the Project Apply viewer: recovery on
 * load, eligibility-gated confirmation, the single POST through the
 * injected submit capability, the closed error behavior, the record
 * removal rules and the selection-change isolation. The transport and
 * the storage are fakes; the submit capability is a deferred gate.
 */
import { describe, expect, test } from "bun:test";
import {
  APPLY_RECORD_STORAGE_PREFIX,
  createApplyRecoveryStore,
  createProjectApplyViewer,
  UiContractError,
} from "../ui/app.js";

const OBS = "a".repeat(64);
const OBS2 = "b".repeat(64);

function mapStorage(): { storage: Record<string, unknown>; map: Map<string, string> } {
  const map = new Map<string, string>();
  return {
    map,
    storage: {
      getItem: (key: string): string | null => (map.has(key) ? (map.get(key) as string) : null),
      setItem: (key: string, value: string): void => {
        map.set(key, value);
      },
      removeItem: (key: string): void => {
        map.delete(key);
      },
    },
  };
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

type RawResult =
  | { ok: true; model: Record<string, unknown> }
  | { ok: false; lost: boolean; failure: { layer: string; reason: string } };

/**
 * The PARSED model shape the endpoint client produces (camelCase); the
 * viewer never sees raw wire documents.
 */
function operationModel(status: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    kind: "project_apply_operation",
    runId: "run-a",
    applyId: OBS,
    projectId: "project-a",
    observation: OBS,
    revision: 7,
    status,
    completedStepCount: 0,
    totalStepCount: 3,
    ...overrides,
  };
}

/** A scripted fake transport: records every desire, resolves them on
 * demand, and supports one pending desire like the real single-flight
 * owner (a new desire supersedes the active one). */
function fakeTransport() {
  const desires: Array<Record<string, unknown> & { superseded: boolean }> = [];
  return {
    desires,
    request(desire: Record<string, unknown>) {
      const last = desires[desires.length - 1];
      if (last !== undefined && !last.settled) {
        last.superseded = true;
      }
      desires.push({ ...desire, superseded: false, settled: false });
    },
    invalidate() {
      for (const desire of desires) {
        if (!desire.settled) {
          desire.superseded = true;
        }
      }
    },
    settle(index: number, result: RawResult) {
      const desire = desires[index];
      if (desire === undefined || desire.settled) {
        return;
      }
      desire.settled = true;
      const settled = desire.onSettled as ((s: unknown) => void) | undefined;
      settled?.({ fulfilled: true, superseded: desire.superseded === true, result });
    },
    reject(index: number) {
      const desire = desires[index];
      if (desire === undefined || desire.settled) {
        return;
      }
      desire.settled = true;
      const settled = desire.onSettled as ((s: unknown) => void) | undefined;
      settled?.({ fulfilled: false, superseded: desire.superseded === true, error: new Error("wire") });
    },
    supersededCount: () => desires.filter((desire) => desire.superseded).length,
  };
}

function fullyLoadedChanges(runId: string, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    runId,
    requestState: "ready",
    summary: { total: 3, added: 1, deleted: 0, content_modified: 1, mode_modified: 1, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0, matches_baseline: 2, matches_result: 1, diverged: 0 },
    changes: [{}, {}, {}],
    nextAfter: null,
    ...overrides,
  };
}

interface Harness {
  viewer: ReturnType<typeof createProjectApplyViewer>;
  transport: ReturnType<typeof fakeTransport>;
  store: ReturnType<typeof createApplyRecoveryStore>;
  map: Map<string, string>;
  states: unknown[];
  submissions: Array<{ identity: unknown; deferred: Deferred<RawResult> }>;
  appliedIdentities: unknown[];
  state(): Record<string, unknown>;
}

function firstSubmission(h: Harness): { identity: unknown; deferred: Deferred<RawResult> } {
  const entry = h.submissions[0];
  if (entry === undefined) throw new Error("missing submission");
  return entry;
}

function secondSubmission(h: Harness): { identity: unknown; deferred: Deferred<RawResult> } {
  const entry = h.submissions[1];
  if (entry === undefined) throw new Error("missing second submission");
  return entry;
}

function createHarness(options: { storage?: Map<string, string> | null } = {}): Harness {
  const transport = fakeTransport();
  const map = options.storage ?? new Map<string, string>();
  const storage = options.storage === null
    ? null
    : {
        getItem: (key: string): string | null => (map.has(key) ? (map.get(key) as string) : null),
        setItem: (key: string, value: string): void => {
          map.set(key, value);
        },
        removeItem: (key: string): void => {
          map.delete(key);
        },
      };
  const store = createApplyRecoveryStore(storage);
  const states: unknown[] = [];
  const submissions: Array<{ identity: unknown; deferred: Deferred<RawResult> }> = [];
  const appliedIdentities: unknown[] = [];
  const viewer = createProjectApplyViewer({
    recoveryStore: store,
    transport: transport as unknown as never,
    submit: (identity: unknown) => {
      const gate = deferred<RawResult>();
      submissions.push({ identity, deferred: gate });
      return gate.promise;
    },
    onState: (state: unknown) => {
      states.push(state);
    },
    onApplied: (identity: unknown) => {
      appliedIdentities.push(identity);
    },
  });
  return {
    viewer: viewer as unknown as Harness["viewer"],
    transport,
    store,
    map,
    states,
    submissions,
    appliedIdentities,
    state: () => viewer.currentState() as unknown as Record<string, unknown>,
  };
}

describe("project apply viewer: selection and the recovery record", () => {
  test("the panel starts idle with no record and no wire desire", () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    expect(h.state()).toMatchObject({ runId: "run-a", uiState: "idle", applyId: null, observation: null, recordError: null });
    expect(h.transport.desires).toHaveLength(0);
    expect(h.submissions).toHaveLength(0);
  });

  test("an existing record is loaded without any GET before the changes are ready", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    expect(h.transport.desires).toHaveLength(0);
    expect(h.submissions).toHaveLength(0);
    // the ready, fully loaded snapshot triggers exactly one recovery GET
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.transport.desires).toHaveLength(1);
    expect(h.transport.desires[0]).toMatchObject({ runId: "run-a", applyId: OBS, observation: OBS });
    expect(h.submissions).toHaveLength(0);
    // a second ready snapshot never re-issues the GET
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.transport.desires).toHaveLength(1);
  });

  test("a missing record never issues a GET", () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.transport.desires).toHaveLength(0);
    expect(h.state().recoveryIssued).toBe(true);
  });

  test("a corrupted record surfaces the error, is never deleted and issues no GET", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, "{not json");
    h.viewer.transitionToRun("run-a");
    expect(h.state().recordError).toBe("corrupt_record");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.transport.desires).toHaveLength(0);
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
  });

  test("an unavailable storage blocks the recovery and every apply", () => {
    const h = createHarness({ storage: null });
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.state().recordError).toBe("storage_unavailable");
    h.viewer.review();
    h.viewer.confirm(OBS);
    expect(h.submissions).toHaveLength(0);
    expect(h.state().uiState).toBe("unavailable");
    expect(h.state().recordError).toBe("storage_unavailable");
  });

  test("the recovery GET applies the authoritative status and keeps the record", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    h.transport.settle(0, { ok: true, model: operationModel("recovery_required") });
    expect(h.state()).toMatchObject({ uiState: "recovery_required", applyId: OBS, observation: OBS, revision: 7, totalSteps: 3 });
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    expect(h.appliedIdentities).toHaveLength(0);
  });

  test("a recovery GET returning applied triggers exactly one explicit reload hook", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    h.transport.settle(0, { ok: true, model: operationModel("applied") });
    expect(h.state().uiState).toBe("applied");
    expect(h.appliedIdentities).toEqual([{ runId: "run-a", applyId: OBS, observation: OBS }]);
  });

  test("a recovery GET 404 shows the missing operation and never erases the record", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    h.transport.settle(0, { ok: false, lost: false, failure: { layer: "request", reason: "project_apply_not_found" } });
    expect(h.state()).toMatchObject({ uiState: "unavailable", error: { layer: "request", reason: "project_apply_not_found" } });
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
  });

  test("a selection change preserves the record, resets the panel and drops the wire", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    h.viewer.transitionToRun("run-b");
    expect(h.state()).toMatchObject({ runId: "run-b", uiState: "idle", applyId: null });
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    expect(h.transport.supersededCount()).toBe(1);
    // returning to the run re-issues exactly one fresh recovery GET
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.transport.desires).toHaveLength(2);
  });
});

describe("project apply viewer: eligibility, confirmation and the single POST", () => {
  test("the first review press confirms without any POST or record", () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    expect(h.state().uiState).toBe("confirming");
    expect(h.submissions).toHaveLength(0);
    expect(h.map.size).toBe(0);
  });

  test("review is refused while an operation identity is pinned", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    expect(h.state().uiState).toBe("idle");
    expect(h.submissions).toHaveLength(0);
  });

  test("cancel returns to idle with zero effects", () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.cancel();
    expect(h.state().uiState).toBe("idle");
    expect(h.map.size).toBe(0);
    expect(h.submissions).toHaveLength(0);
  });

  test("confirm writes the recovery record before exactly one POST", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    expect(h.state().uiState).toBe("submitting");
    expect(h.map.get(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(
      JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }),
    );
    expect(h.submissions).toHaveLength(1);
    expect(firstSubmission(h).identity).toEqual({ runId: "run-a", applyId: OBS, observation: OBS });
    expect(h.transport.desires).toHaveLength(0);
    firstSubmission(h).deferred.resolve({ ok: true, model: operationModel("preparing") });
    await Promise.resolve();
    await Promise.resolve();
    expect(h.state()).toMatchObject({ uiState: "preparing", applyId: OBS, observation: OBS });
  });

  test("a pinned record refuses both the review and the confirm paths", () => {
    const map = new Map<string, string>();
    map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    const h = createHarness({ storage: map });
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    expect(h.state().uiState).toBe("idle");
    h.viewer.confirm(OBS);
    expect(h.submissions).toHaveLength(0);
    expect(h.state().uiState).toBe("idle");
    expect(h.map.size).toBe(1);
  });

  test("a double confirm creates exactly one POST", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    h.viewer.confirm(OBS);
    expect(h.submissions).toHaveLength(1);
    firstSubmission(h).deferred.resolve({ ok: true, model: operationModel("applying") });
    await Promise.resolve();
    await Promise.resolve();
  });

  test("the confirm identity is bound to the passed observation", () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    expect(() => h.viewer.confirm("nothex")).toThrow(UiContractError);
    expect(h.submissions).toHaveLength(0);
    expect(h.map.size).toBe(0);
  });

  test("an applied POST result triggers the explicit reload hook and keeps the record", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    firstSubmission(h).deferred.resolve({ ok: true, model: operationModel("applied") });
    await Promise.resolve();
    await Promise.resolve();
    expect(h.state().uiState).toBe("applied");
    expect(h.appliedIdentities).toEqual([{ runId: "run-a", applyId: OBS, observation: OBS }]);
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
  });

  test("typed POST failures surface without masking and keep the record", async () => {
    for (const reason of ["project_busy", "preparation_busy", "identity_conflict", "project_conflict", "project_read_only", "unsupported_filesystem", "artifact_missing", "artifact_modified"]) {
      const h = createHarness();
      h.viewer.transitionToRun("run-a");
      h.viewer.review();
      h.viewer.confirm(OBS);
      firstSubmission(h).deferred.resolve({ ok: false, lost: false, failure: { layer: "orchestrator", reason } });
      await Promise.resolve();
      await Promise.resolve();
      expect(h.state()).toMatchObject({ uiState: "unavailable", error: { layer: "orchestrator", reason } });
      expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    }
  });

  test("a stale_observation refusal clears the provably uncreated operation record explicitly", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    firstSubmission(h).deferred.resolve({ ok: false, lost: false, failure: { layer: "request", reason: "stale_observation" } });
    await Promise.resolve();
    await Promise.resolve();
    expect(h.state()).toMatchObject({
      uiState: "idle",
      error: { layer: "request", reason: "stale_observation" },
      applyId: null,
      observation: null,
    });
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(false);
  });

  test("a lost POST response is network_unknown with the record kept", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    firstSubmission(h).deferred.resolve({ ok: false, lost: true, failure: { layer: "transport", reason: "network_error" } });
    await Promise.resolve();
    await Promise.resolve();
    expect(h.state()).toMatchObject({ uiState: "network_unknown", applyId: OBS, observation: OBS, error: null });
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
  });

  test("the retry uses the same identity without writing a second record", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    firstSubmission(h).deferred.resolve({ ok: false, lost: true, failure: { layer: "transport", reason: "network_error" } });
    await Promise.resolve();
    await Promise.resolve();
    h.viewer.retry();
    expect(h.submissions).toHaveLength(2);
    expect(secondSubmission(h).identity).toEqual({ runId: "run-a", applyId: OBS, observation: OBS });
    expect(h.map.size).toBe(1);
    secondSubmission(h).deferred.resolve({ ok: true, model: operationModel("applied") });
    await Promise.resolve();
    await Promise.resolve();
    expect(h.state().uiState).toBe("applied");
  });

  test("the continuation POSTs the same identity from a durable in-progress status", async () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    h.transport.settle(0, { ok: true, model: operationModel("recovery_required") });
    h.viewer.continueRecovery();
    expect(h.submissions).toHaveLength(1);
    expect(firstSubmission(h).identity).toEqual({ runId: "run-a", applyId: OBS, observation: OBS });
    firstSubmission(h).deferred.resolve({ ok: true, model: operationModel("applying", { completedStepCount: 1 }) });
    await Promise.resolve();
    await Promise.resolve();
    expect(h.state()).toMatchObject({ uiState: "applying", completedSteps: 1 });
  });

  test("check status re-issues one GET and applies the authoritative status", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    void firstSubmission(h).deferred.promise;
    firstSubmission(h).deferred.resolve({ ok: false, lost: true, failure: { layer: "transport", reason: "network_error" } });
    await Promise.resolve();
    await Promise.resolve();
    h.viewer.checkStatus();
    expect(h.transport.desires).toHaveLength(1);
    h.transport.settle(0, { ok: true, model: operationModel("conflict") });
    expect(h.state()).toMatchObject({ uiState: "conflict", applyId: OBS, observation: OBS });
  });

  test("a failed recovery GET never erases the record and stays retryable by check", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    h.transport.reject(0);
    expect(h.state()).toMatchObject({ uiState: "unavailable", error: { layer: "transport", reason: "network_error" } });
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    h.viewer.checkStatus();
    expect(h.transport.desires).toHaveLength(2);
  });
});

describe("project apply viewer: the record removal after a confirming reload", () => {
  test("the removal waits for a fully loaded reload without pending or diverged entries", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    firstSubmission(h).deferred.resolve({ ok: true, model: operationModel("applied") });
    await Promise.resolve();
    await Promise.resolve();
    // a snapshot that still carries pending changes never removes
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    expect(h.state().uiState).toBe("applied");
    // the confirming snapshot removes exactly once
    h.viewer.changesSnapshotReady(
      fullyLoadedChanges("run-a", {
        summary: { total: 2, added: 0, deleted: 0, content_modified: 0, mode_modified: 2, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0, matches_baseline: 0, matches_result: 2, diverged: 0 },
        changes: [{}, {}],
      }),
    );
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(false);
    expect(h.state()).toMatchObject({ uiState: "idle", applyId: null, observation: null, recordError: null });
    // a later snapshot never re-runs a recovery GET
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    expect(h.transport.desires).toHaveLength(0);
  });

  test("a diverged confirming snapshot keeps the record", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    firstSubmission(h).deferred.resolve({ ok: true, model: operationModel("applied") });
    await Promise.resolve();
    await Promise.resolve();
    h.viewer.changesSnapshotReady(
      fullyLoadedChanges("run-a", {
        summary: { total: 2, added: 0, deleted: 0, content_modified: 0, mode_modified: 0, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 2, matches_baseline: 0, matches_result: 0, diverged: 2 },
      }),
    );
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
    expect(h.state().uiState).toBe("applied");
  });

  test("a partially loaded snapshot never removes the record", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    firstSubmission(h).deferred.resolve({ ok: true, model: operationModel("applied") });
    await Promise.resolve();
    await Promise.resolve();
    h.viewer.changesSnapshotReady(
      fullyLoadedChanges("run-a", {
        summary: { total: 3, added: 0, deleted: 0, content_modified: 0, mode_modified: 3, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0, matches_baseline: 0, matches_result: 3, diverged: 0 },
        changes: [{}, {}, {}],
        nextAfter: 3,
      }),
    );
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
  });

  test("a failed record removal surfaces the error and keeps the applied status", async () => {
    const removalFailures: number[] = [];
    const map = new Map<string, string>();
    const storage = {
      getItem: (key: string): string | null => (map.has(key) ? (map.get(key) as string) : null),
      setItem: (key: string, value: string): void => {
        map.set(key, value);
      },
      removeItem: (key: string): void => {
        if (removalFailures.length > 0) {
          removalFailures.pop();
          throw new Error("storage died");
        }
        map.delete(key);
      },
    };
    const store = createApplyRecoveryStore(storage);
    const transport = fakeTransport();
    const states: unknown[] = [];
    const submissions: Array<{ identity: unknown; deferred: Deferred<RawResult> }> = [];
    const viewer = createProjectApplyViewer({
      recoveryStore: store,
      transport: transport as unknown as never,
      submit: (identity: unknown) => {
        const gate = deferred<RawResult>();
        submissions.push({ identity, deferred: gate });
        return gate.promise;
      },
      onState: (state: unknown) => {
        states.push(state);
      },
    });
    viewer.transitionToRun("run-a");
    viewer.review();
    viewer.confirm(OBS);
    const submission = submissions[0];
    if (submission === undefined) throw new Error("missing submission");
    submission.deferred.resolve({ ok: true, model: operationModel("applied") });
    await Promise.resolve();
    await Promise.resolve();
    removalFailures.push(1);
    viewer.changesSnapshotReady(
      fullyLoadedChanges("run-a", {
        summary: { total: 2, added: 0, deleted: 0, content_modified: 0, mode_modified: 2, content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0, matches_baseline: 0, matches_result: 2, diverged: 0 },
        changes: [{}, {}],
      }),
    );
    expect((viewer.currentState() as unknown as Record<string, unknown>)).toMatchObject({ uiState: "applied", recordError: "record_unwritable" });
    expect(map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
  });

  test("a changes snapshot of another run never touches the state", () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-b"));
    expect(h.state().recoveryIssued).toBe(false);
  });
});

describe("project apply viewer: late responses and hostile inputs", () => {
  test("a late POST response of a reset run never applies", async () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    h.viewer.review();
    h.viewer.confirm(OBS);
    const pending = firstSubmission(h).deferred;
    h.viewer.transitionToRun("run-b");
    pending.resolve({ ok: true, model: operationModel("applied") });
    await Promise.resolve();
    await Promise.resolve();
    expect(h.state()).toMatchObject({ runId: "run-b", uiState: "idle" });
    expect(h.appliedIdentities).toHaveLength(0);
    // the record of run-a survives and drives a later recovery
    expect(h.map.has(`${APPLY_RECORD_STORAGE_PREFIX}run-a`)).toBe(true);
  });

  test("a late recovery GET of a superseded desire never applies", () => {
    const h = createHarness();
    h.map.set(`${APPLY_RECORD_STORAGE_PREFIX}run-a`, JSON.stringify({ schema_version: 1, run_id: "run-a", apply_id: OBS, observation: OBS }));
    h.viewer.transitionToRun("run-a");
    h.viewer.changesSnapshotReady(fullyLoadedChanges("run-a"));
    h.viewer.transitionToRun("run-b");
    h.transport.settle(0, { ok: true, model: operationModel("applied") });
    expect(h.state()).toMatchObject({ runId: "run-b", uiState: "idle" });
    expect(h.appliedIdentities).toHaveLength(0);
  });

  test("malformed submit results surface as contract errors, never native exceptions", async () => {
    for (const bad of [undefined, null, 7, "text", {}, { ok: true, model: null }, { ok: true, model: { kind: "other" } }]) {
      const h = createHarness();
      h.viewer.transitionToRun("run-a");
      h.viewer.review();
      h.viewer.confirm(OBS);
      firstSubmission(h).deferred.resolve(bad as RawResult);
      await Promise.resolve();
      await Promise.resolve();
      expect(h.state().uiState).toBe("unavailable");
      expect((h.state().error as { layer: string } | null)?.layer).toBe("contract");
    }
  });

  test("the state machine is deeply frozen and repeated transitions are deterministic", () => {
    const h = createHarness();
    h.viewer.transitionToRun("run-a");
    const first = h.state();
    expect(Object.isFrozen(first)).toBe(true);
    h.viewer.review();
    h.viewer.cancel();
    h.viewer.review();
    h.viewer.cancel();
    expect(h.state()).toEqual(first);
  });

  test("invalid constructor inputs fail closed before any effect", () => {
    expect(() => createProjectApplyViewer(null as never)).toThrow(UiContractError);
    expect(() => createProjectApplyViewer({} as never)).toThrow(UiContractError);
    expect(() => createProjectApplyViewer({ recoveryStore: {}, submit: () => undefined, onState: () => undefined } as never)).toThrow(UiContractError);
  });

  test("transitionToRun refuses unsafe ids", () => {
    const h = createHarness();
    expect(() => h.viewer.transitionToRun("bad/id")).toThrow(UiContractError);
    expect(() => h.viewer.transitionToRun(null as never)).toThrow(UiContractError);
  });
});
