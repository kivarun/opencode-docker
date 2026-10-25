/**
 * Permanent suite of the browser UI v6 project-changes transport: the
 * one single-flight admission owner of every changes GET of the module.
 * The transport is driven directly through its injected io, with an
 * instrumented `fetchChanges` that records the exact wire lifecycle and
 * can hold, reject or abort-ignore each request. Every test proves the
 * arbitration (an initial/reload supersedes and coalesces; a
 * continuation while the wire is busy is declined without a second
 * fetch), the exactly-once settlement, the invalidation discipline and
 * the absence of any timer or retry machinery.
 */
import { describe, expect, test } from "bun:test";
import {
  UiContractError,
  createProjectChangesTransport,
  type UiProjectChangesResult,
} from "../ui/app.js";

interface CallPlan {
  /** The response marker of which call answered. */
  readonly revision: number;
  /** Hold the request on the wire until its gate is released. */
  readonly hold?: boolean;
  /** Reject the fetchChanges promise instead of resolving. */
  readonly reject?: boolean;
  /** Resolve with undefined (a broken transport produced no result). */
  readonly noResult?: boolean;
  /** Ignore the abort signal and settle only when released. */
  readonly ignoreAbort?: boolean;
  /** The stale 409 envelope of the endpoint. */
  readonly stale?: boolean;
}

interface Probe {
  readonly io: {
    readonly fetchChanges: (runId: string, path: string, signal: AbortSignal) => Promise<UiProjectChangesResult | undefined>;
    readonly onStarted: (runId: string) => void;
    readonly onSettled: (runId: string) => void;
  };
  readonly events: Array<{ kind: "started" | "settled"; seq: number }>;
  readonly calls: Array<{ runId: string; path: string; aborted: boolean }>;
  active(): number;
  maxActive(): number;
  release(seq: number): void;
  releaseAll(): void;
}

function changesResult(revision: number): UiProjectChangesResult {
  return {
    ok: true,
    lost: false,
    model: {
      kind: "project_changes",
      runId: "run-a",
      revision,
      observation: "a".repeat(64),
      summary: {
        total: 0, added: 0, deleted: 0, content_modified: 0, mode_modified: 0,
        content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0,
        matches_baseline: 0, matches_result: 0, diverged: 0,
      },
      changes: [],
      nextAfter: null,
    },
  } as UiProjectChangesResult;
}

function staleResult(): UiProjectChangesResult {
  return {
    ok: true,
    lost: false,
    model: { kind: "error", layer: "request", reason: "stale_observation" },
  } as unknown as UiProjectChangesResult;
}

function makeProbe(plans: CallPlan[]): Probe {
  const events: Array<{ kind: "started" | "settled"; seq: number }> = [];
  const calls: Array<{ runId: string; path: string; aborted: boolean }> = [];
  let seq = 0;
  let active = 0;
  let maxActive = 0;
  const gates = new Map<number, () => void>();
  const io = {
    fetchChanges: (runId: string, path: string, signal: AbortSignal) => {
      seq += 1;
      const call = seq;
      calls.push({ runId, path, aborted: false });
      const record = calls[calls.length - 1];
      signal.addEventListener("abort", () => {
        if (record !== undefined) {
          record.aborted = true;
        }
      });
      events.push({ kind: "started", seq: call });
      active += 1;
      if (active > maxActive) maxActive = active;
      const plan = plans[call - 1] ?? { revision: call };
      const settle = async (): Promise<UiProjectChangesResult | undefined> => {
        if (plan.hold === true || plan.ignoreAbort === true) {
          await new Promise<void>((resolve) => {
            gates.set(call, resolve);
          });
        }
        if (plan.reject === true) {
          throw new Error("wire gone");
        }
        if (plan.noResult === true) {
          return undefined;
        }
        return plan.stale === true ? staleResult() : changesResult(plan.revision);
      };
      return settle().finally(() => {
        active -= 1;
        events.push({ kind: "settled", seq: call });
      });
    },
    onStarted: () => undefined,
    onSettled: () => undefined,
  };
  return {
    io,
    events,
    calls,
    active: () => active,
    maxActive: () => maxActive,
    release: (target: number) => {
      const gate = gates.get(target);
      if (gate !== undefined) {
        gates.delete(target);
        gate();
      }
    },
    releaseAll: () => {
      for (const gate of gates.values()) {
        gate();
      }
      gates.clear();
    },
  };
}

interface DesireSpec {
  readonly runId?: string;
  readonly kind: "initial" | "continuation";
  readonly after: number;
  readonly path?: string;
}

function makeTransport(probe: Probe) {
  const applied: Array<UiProjectChangesResult | undefined> = [];
  const transport = createProjectChangesTransport({
    ...probe.io,
    onStarted: probe.io.onStarted,
    onSettled: probe.io.onSettled,
  });
  const desire = (spec: DesireSpec) => {
    const runId = spec.runId ?? "run-a";
    const after = spec.after;
    const path = spec.path ?? `/v1/runs/${runId}/changes?after=${after}&limit=100`;
    return transport.request({
      runId,
      kind: spec.kind,
      after,
      path,
      onApplied: (result) => {
        applied.push(result);
      },
    });
  };
  return { transport, applied, desire };
}

describe("ui project-changes transport: single-flight admission", () => {
  test("t1. an idle admission starts immediately and applies its own result", async () => {
    const probe = makeProbe([{ revision: 5 }]);
    const { transport, applied, desire } = makeTransport(probe);
    const outcome = desire({ kind: "initial", after: 0 });
    expect(probe.active()).toBe(1);
    const result = await outcome;
    expect(probe.active()).toBe(0);
    expect(result).toEqual(changesResult(5));
    expect(applied).toEqual([changesResult(5)]);
    void transport;
  });

  test("t2. a continuation while the wire is busy is declined without a second fetch", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }]);
    const { applied, desire } = makeTransport(probe);
    const first = desire({ kind: "initial", after: 0 });
    const declined = desire({ kind: "continuation", after: 1 });
    expect(probe.active()).toBe(1);
    expect(await declined).toBeUndefined();
    expect(probe.calls.length).toBe(1);
    probe.release(1);
    await first;
    await Bun.sleep(2);
    // the declined desire is never replayed
    expect(probe.calls.length).toBe(1);
    expect(probe.maxActive()).toBe(1);
    expect(applied).toEqual([changesResult(5)]);
  });

  test("t3. an initial/reload desire supersedes the active request and coalesces", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const { applied, desire } = makeTransport(probe);
    const first = desire({ kind: "initial", after: 0 });
    expect(probe.calls[0]?.aborted).toBe(false);
    const second = desire({ kind: "initial", after: 0 });
    expect(probe.calls[0]?.aborted).toBe(true);
    probe.release(1);
    await first;
    await Bun.sleep(2);
    // the superseded response is dropped; the coalesced follow-up runs
    expect(probe.calls.length).toBe(2);
    expect(applied).toEqual([changesResult(6)]);
    expect(await second).toEqual(changesResult(6));
  });

  test("t4. a superseded stale fulfillment is dropped and never applied", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }, { revision: 6 }]);
    const { applied, desire } = makeTransport(probe);
    const first = desire({ kind: "initial", after: 0 });
    const second = desire({ kind: "initial", after: 0 });
    void second;
    probe.release(1);
    await first;
    await Bun.sleep(2);
    expect(applied).toEqual([changesResult(6)]);
    // the late stale rejection of a superseded request is equally dropped
    const rejectProbe = makeProbe([{ revision: 5, hold: true, reject: true }, { revision: 6, hold: true }]);
    const rejectRun = makeTransport(rejectProbe);
    const rejectedFirst = rejectRun.desire({ kind: "initial", after: 0 });
    const rejectedSecond = rejectRun.desire({ kind: "initial", after: 0 });
    void rejectedSecond;
    rejectProbe.release(1);
    await rejectedFirst;
    rejectProbe.release(2);
    await Bun.sleep(2);
    expect(rejectRun.applied).toEqual([changesResult(6)]);
  });

  test("t5. an abort-ignoring fetch still settles the slot exactly once", async () => {
    const probe = makeProbe([{ revision: 5, ignoreAbort: true }, { revision: 6 }]);
    const { applied, desire } = makeTransport(probe);
    const first = desire({ kind: "initial", after: 0 });
    // a superseding desire aborts the ignoring fetch and coalesces
    const second = desire({ kind: "initial", after: 0 });
    expect(probe.calls[0]?.aborted).toBe(true);
    probe.release(1);
    await first;
    await Bun.sleep(2);
    probe.release(2);
    await second;
    expect(probe.maxActive()).toBe(1);
    expect(applied).toEqual([changesResult(6)]);
  });

  test("t6. invalidate withdraws the active and queued desires; a broken transport settles as undefined", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }]);
    const { transport, applied, desire } = makeTransport(probe);
    const first = desire({ kind: "initial", after: 0 });
    transport.invalidate();
    expect(probe.calls[0]?.aborted).toBe(true);
    // the superseded request settles (its gate released) without ever
    // applying; the awaiting caller still receives the raw settlement
    probe.release(1);
    await first;
    await Bun.sleep(2);
    expect(applied).toEqual([]);
    expect(probe.calls.length).toBe(1);
    const broken = makeProbe([{ revision: 1, noResult: true }]);
    const brokenRun = makeTransport(broken);
    const outcome = brokenRun.desire({ kind: "initial", after: 0 });
    expect(await outcome).toBeUndefined();
    expect(brokenRun.applied).toEqual([undefined]);
    // a transport io without functions is a contract failure
    expect(() =>
      createProjectChangesTransport({ fetchChanges: undefined as never, onStarted: () => undefined, onSettled: () => undefined }),
    ).toThrow(UiContractError);
  });

  test("t7. a hundred-request stress keeps one wire slot and exactly the last applied page", async () => {
    const probe = makeProbe([{ revision: 5, hold: true }]);
    const { applied, desire } = makeTransport(probe);
    const pending: Array<Promise<UiProjectChangesResult | undefined>> = [];
    for (let index = 0; index < 100; index++) {
      pending.push(desire({ kind: "initial", after: 0 }));
      await Bun.sleep(0);
    }
    expect(probe.maxActive()).toBe(1);
    probe.releaseAll();
    await Promise.all(pending);
    await Bun.sleep(5);
    expect(probe.maxActive()).toBe(1);
    // exactly one settled request was applied: the last admitted one
    expect(applied.length).toBe(1);
    expect(probe.calls.length).toBe(2);
  });
});
