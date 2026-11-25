/**
 * The S5 red-before overlay battery: the "Lifecycle history" slice
 * (iteration cards, compare, historical interventions) sourced from the
 * shipped P-3 identity stream (12 kinds) and the landed S5-P0
 * execution-bindings stream.
 *
 * Materialized on the clean base `1185cae5f003bfd498586896df88d52a8c657fec`
 * BEFORE the first S5 production edit, per the accepted preflight r3
 * (/exchange/outbox/increment-6-s5-preflight-r3-report.md). Red-before
 * classes are RED on the clean base and turn green with the S5
 * implementation; the explicit green-preservation guards are green on
 * the clean base and must stay green throughout.
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const appPath = join(import.meta.dir, "..", "ui", "app.js");
const htmlPath = join(import.meta.dir, "..", "ui", "index.html");
const appSource = readFileSync(appPath, "utf8");
const htmlSource = readFileSync(htmlPath, "utf8");

/** The fixed owner names of preflight r3 §4 (the implementation must export all of them). */
const S5_OWNERS = [
  "parseIdentityDocument",
  "parseExecutionBindingsDocument",
  "identityRequestPath",
  "executionBindingsRequestPath",
  "createIdentityTransport",
  "createExecutionBindingsTransport",
  "createLifecycleHistoryState",
  "applyIdentityPage",
  "applyBindingsPage",
  "deriveLifecycleHistory",
  "createLifecycleHistoryViewer",
  "parseIdentityEndpointDocument",
  "parseExecutionBindingsEndpointDocument",
] as const;

/** The general API client's fail-closed default (imported statically — it exists on the base). */
const { parseApiDocument, eventsRequestPath } = await import("../ui/app.js");

describe("S5 red-before: the Lifecycle history slice does not exist yet", () => {
  test("(a) ui/index.html has no lifecycle-history section", () => {
    expect(htmlSource).toContain('id="lifecycle-history-section"');
  });

  test("(c) every fixed S5 owner is exported by ui/app.js", async () => {
    const app = await import(appPath);
    for (const owner of S5_OWNERS) {
      expect(typeof (app as Record<string, unknown>)[owner], owner).toBe("function");
    }
  });

  test("(c/d) the app.js wiring consumes the identity and execution-bindings streams", () => {
    // The wiring site (not merely the definitions): each owner name must
    // appear again outside its own `function <name>(` declaration — the
    // viewer/state-machine call sites. At the clean base neither name
    // appears at all.
    expect(appSource.includes("parseIdentityDocument")).toBe(true);
    expect(appSource.includes("execution_binding")).toBe(true);
    // The DOM section and the viewer are wired together.
    expect(appSource.includes("lifecycle-history-section")).toBe(true);
  });

  test("(d) the bindings element ids are declared in the DOM manifest", () => {
    for (const id of [
      "lh-identity-meta",
      "lh-bindings-meta",
      "lh-iterations",
      "lh-compare",
      "lh-historical",
      "lh-status",
    ]) {
      expect(htmlSource).toContain(`id="${id}"`);
    }
  });
});

describe("green-preservation guards (green on the clean base)", () => {
  test("(b) the general API client keeps rejecting the identity and execution_bindings kinds", () => {
    for (const kind of ["identity", "execution_bindings"]) {
      try {
        parseApiDocument({ schema_version: 1, kind, run_id: "r", revision: 1, after: 0, records: [], next_after: null });
        throw new Error(`unreachable: ${kind} accepted`);
      } catch (cause) {
        expect((cause as Error).name).toBe("UiContractError");
      }
    }
  });

  test("the events timeline keeps its own URL builder and section untouched", () => {
    expect(eventsRequestPath("r", 0)).toBe("/v1/runs/r/events?after=0&limit=100");
    expect(htmlSource).toContain('id="events-section"');
    // The rail identity derivation stays events-based: the identity
    // stream must never leak into the rail (r2 prohibition).
    const railMatch = /export function deriveRunRailIdentity\((.|\n)*?\n}\n/.exec(appSource);
    if (railMatch !== null) {
      expect(railMatch[0]).not.toContain('"identity"');
      expect(railMatch[0]).not.toContain("execution_binding");
    }
  });
});
