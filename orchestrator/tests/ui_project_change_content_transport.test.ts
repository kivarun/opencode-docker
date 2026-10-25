/**
 * Permanent suite of the browser UI v7 Project Change Content
 * transport: the one single-flight admission owner of every content
 * GET of the module. An explicit Review supersedes the active request
 * and coalesces as the one follow-up; a chunk continuation while the
 * wire is busy is declined without a second fetch; invalidate and
 * dispose withdraw the active and queued desires; superseded
 * fulfillments and rejections are dropped. There is no timer, no
 * polling and no retry loop of any kind.
 */
import { describe, expect, test } from "bun:test";
import {
  UiContractError,
  createProjectChangeContentTransport,
  type UiProjectChangeContentResult,
} from "../ui/app.js";

interface CallPlan {
  readonly marker: number;
  readonly hold?: boolean;
  readonly reject?: boolean;
  readonly ignoreAbort?: boolean;
}

interface Probe {
  readonly io: {
    readonly fetchContent: (runId: string, path: string, signal: AbortSignal, expected: unknown) => Promise<UiProjectChangeContentResult | undefined>;
    readonly onStarted: (runId: string) => void;
    readonly onSettled: (runId: string) => void;
  };
  readonly events: Array<{ kind: "started" | "settled"; seq: number }>;
  readonly calls: Array<{ runId: string; path: string; side: unknown; aborted: boolean }>;
  active(): number;
  maxActive(): number;
  release(seq: number): void;
  releaseAll(): void;
}

const OBS = "a".repeat(64);
const EXPECTED = { runId: "run-a", observation: OBS, changeIndex: 1, side: "result" };

function contentResult(marker: number): UiProjectChangeContentResult {
  return {
    ok: true,
    lost: false,
    model: {
      kind: "project_change_content",
      runId: "run-a",
      revision: 9,
      observation: OBS,
      changeIndex: 1,
      side: "result",
      status: "available",
      byteCount: marker,
      sha256: "b".repeat(64),
      offset: 0,
      data: new Uint8Array(marker),
      nextOffset: null,
    },
  } as UiProjectChangeContentResult;
}

function makeProbe(plans: CallPlan[]): Probe {
  const events: Array<{ kind: "started" | "settled"; seq: number }> = [];
  const calls: Array<{ runId: string; path: string; side: unknown; aborted: boolean }> = [];
  let seq = 0;
  let active = 0;
  let maxActive = 0;
  const gates = new Map<number, () => void>();
  const io = {
    fetchContent: (runId: string, path: string, signal: AbortSignal, expected: unknown) => {
      seq += 1;
      const call = seq;
      calls.push({ runId, path, side: expected, aborted: false });
      const record = calls[calls.length - 1];
      signal.addEventListener("abort", () => {
        if (record !== undefined) {
          record.aborted = true;
        }
      });
      events.push({ kind: "started", seq: call });
      active += 1;
      if (active > maxActive) maxActive = active;
      const plan = plans[call - 1] ?? { marker: call };
      const settle = async (): Promise<UiProjectChangeContentResult | undefined> => {
        if (plan.hold === true || plan.ignoreAbort === true) {
          await new Promise<void>((resolve) => {
            gates.set(call, resolve);
          });
        }
        if (plan.reject === true) {
          throw new Error("wire gone");
        }
        return contentResult(plan.marker);
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
  readonly side?: string;
  readonly path?: string;
}

function makeTransport(probe: Probe) {
  const applied: Array<UiProjectChangeContentResult | undefined> = [];
  const transport = createProjectChangeContentTransport({
    ...probe.io,
    onStarted: probe.io.onStarted,
    onSettled: probe.io.onSettled,
  });
  const desire = (spec: DesireSpec) => {
    const runId = spec.runId ?? "run-a";
    const path = spec.path ?? `/v1/runs/${runId}/changes/1/content?side=result&observation=${OBS}`;
    return transport.request({
      runId,
      kind: spec.kind,
      side: spec.side ?? "result",
      expected: { ...EXPECTED, side: spec.side ?? "result" },
      path,
      onApplied: (result) => {
        applied.push(result);
      },
    });
  };
  return { transport, applied, desire };
}

describe("v7 content transport: single-flight admission", () => {
  test("t1. an idle admission starts immediately and applies its own result", async () => {
    const probe = makeProbe([{ marker: 7 }]);
    const { transport, applied, desire } = makeTransport(probe);
    const outcome = desire({ kind: "initial" });
    expect(probe.active()).toBe(1);
    const result = await outcome;
    expect(probe.active()).toBe(0);
    expect(result).toEqual(contentResult(7));
    expect(applied).toEqual([contentResult(7)]);
    void transport;
  });

  test("t2. a chunk continuation while the wire is busy is declined without a second fetch", async () => {
    const probe = makeProbe([{ marker: 7, hold: true }]);
    const { applied, desire } = makeTransport(probe);
    const first = desire({ kind: "initial" });
    const declined = desire({ kind: "continuation", side: "baseline" });
    expect(probe.active()).toBe(1);
    expect(await declined).toBeUndefined();
    expect(probe.calls.length).toBe(1);
    probe.release(1);
    await first;
    await Bun.sleep(2);
    expect(probe.calls.length).toBe(1);
    expect(probe.maxActive()).toBe(1);
    expect(applied).toEqual([contentResult(7)]);
  });

  test("t3. a new explicit Review supersedes the active request and coalesces to the last desire", async () => {
    // fetch order: call 1 (held), then the one coalesced follow-up
    // (the replaced queued desire never fetches)
    const probe = makeProbe([{ marker: 7, hold: true }, { marker: 9 }]);
    const { applied, desire } = makeTransport(probe);
    const first = desire({ kind: "initial" });
    expect(probe.calls[0]?.aborted).toBe(false);
    const second = desire({ kind: "initial", side: "baseline" });
    const third = desire({ kind: "initial" });
    expect(probe.calls[0]?.aborted).toBe(true);
    probe.release(1);
    await first;
    await Bun.sleep(2);
    // the superseded response is dropped and the earlier queued desire
    // was replaced by the last one: exactly one follow-up runs
    expect(probe.calls.length).toBe(2);
    expect(applied).toEqual([contentResult(9)]);
    expect(await second).toBeUndefined();
    expect(await third).toEqual(contentResult(9));
  });

  test("t4. a superseded fulfillment and its rejection are both dropped", async () => {
    const rejectProbe = makeProbe([{ marker: 7, hold: true, reject: true }, { marker: 8, hold: true }]);
    const rejectRun = makeTransport(rejectProbe);
    const rejectedFirst = rejectRun.desire({ kind: "initial" });
    const rejectedSecond = rejectRun.desire({ kind: "initial" });
    void rejectedSecond;
    rejectProbe.release(1);
    await rejectedFirst;
    rejectProbe.release(2);
    await Bun.sleep(2);
    expect(rejectRun.applied).toEqual([contentResult(8)]);
  });

  test("t5. invalidate withdraws the active and queued desires; a late response never applies", async () => {
    const probe = makeProbe([{ marker: 7, hold: true, ignoreAbort: true }, { marker: 8 }]);
    const { applied, desire, transport } = makeTransport(probe);
    const first = desire({ kind: "initial" });
    transport.invalidate();
    expect(probe.calls[0]?.aborted).toBe(true);
    const after = desire({ kind: "initial" });
    probe.release(1);
    await first;
    await Bun.sleep(2);
    expect(applied).toEqual([contentResult(8)]);
    expect(await after).toEqual(contentResult(8));
    probe.releaseAll();
  });

  test("t6. a late response after invalidation is superseded and never applied", async () => {
    const probe = makeProbe([{ marker: 7, hold: true, ignoreAbort: true }, { marker: 8 }]);
    const { applied, desire, transport } = makeTransport(probe);
    const first = desire({ kind: "initial" });
    const second = desire({ kind: "initial" });
    transport.invalidate();
    probe.release(1);
    await first;
    probe.release(2);
    await second;
    await Bun.sleep(2);
    expect(applied).toEqual([]);
    void transport;
  });

  test("t7. dispose withdraws everything; later requests resolve undefined", async () => {
    const probe = makeProbe([{ marker: 7, hold: true, ignoreAbort: true }, { marker: 8 }]);
    const { applied, desire, transport } = makeTransport(probe);
    const first = desire({ kind: "initial" });
    transport.dispose();
    expect(probe.calls[0]?.aborted).toBe(true);
    probe.release(1);
    await first;
    expect(await desire({ kind: "initial" })).toBeUndefined();
    probe.releaseAll();
    await Bun.sleep(2);
    expect(applied).toEqual([]);
  });

  test("t8. the wire lifecycle is exactly once per request and the abort never frees the slot itself", async () => {
    const probe = makeProbe([{ marker: 7, hold: true, ignoreAbort: true }, { marker: 8 }]);
    const { desire } = makeTransport(probe);
    const first = desire({ kind: "initial" });
    const second = desire({ kind: "initial" });
    expect(probe.calls[0]?.aborted).toBe(true);
    expect(probe.active()).toBe(1);
    probe.release(1);
    await first;
    await second;
    await Bun.sleep(2);
    const started = probe.events.filter((event) => event.kind === "started").length;
    const settled = probe.events.filter((event) => event.kind === "settled").length;
    expect(started).toBe(settled);
    expect(started).toBe(2);
    expect(probe.maxActive()).toBe(1);
  });

  test("t9. malformed desires resolve undefined without touching the wire", async () => {
    const probe = makeProbe([]);
    const { transport } = makeTransport(probe);
    expect(await transport.request({ kind: "initial" } as never)).toBeUndefined();
    expect(await transport.request(null as never)).toBeUndefined();
    expect(probe.calls.length).toBe(0);
    expect(() =>
      createProjectChangeContentTransport({ fetchContent: 1, onStarted: () => undefined, onSettled: () => undefined } as never),
    ).toThrow(UiContractError);
  });

  test("t10. the 100-sequence stress keeps exactly one active GET at any instant", async () => {
    const probe = makeProbe([]);
    const { applied, desire } = makeTransport(probe);
    for (let index = 0; index < 100; index += 1) {
      void desire({ kind: "initial" });
      await Bun.sleep(0);
    }
    await Bun.sleep(20);
    expect(probe.calls.length).toBeLessThanOrEqual(100);
    expect(probe.maxActive()).toBe(1);
    expect(applied.length).toBeGreaterThan(0);
    probe.releaseAll();
  });
});
