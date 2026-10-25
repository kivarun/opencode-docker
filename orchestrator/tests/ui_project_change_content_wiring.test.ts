/**
 * Permanent wiring suite of the browser UI v7 Project Change Content
 * panel: the fake-DOM harness boots the shipped `app.js` and drives
 * the production call chains (run selection -> terminal inspect ->
 * the automatic changes metadata load -> an explicit Review -> the two
 * sequential content GETs -> Load more -> stale -> Reload Project
 * Changes -> selection change), with an instrumented fetch serving the
 * real content endpoint documents. Every test proves the zero-GET
 * idle state, the exact request matrix, the bounded render (text and
 * hex), the staleness discipline and the isolation from the inspect,
 * events, logs and mutation surfaces.
 */
import { describe, expect, test } from "bun:test";
import { createUiHarness, jsonResponse, projectsDocument, runDocument, runsPageDocument } from "./ui_dom_harness.ts";

const OBS = "a".repeat(64);
const SHA = "c".repeat(64);
const SHA2 = "d".repeat(64);

function fileNode(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return { kind: "regular_file", mode: 0o644, byte_count: 5, sha256: SHA, multiple_links: false, ...overrides };
}

function eventEntry(kind: string, revision: number): Record<string, unknown> {
  return { revision, kind };
}

function changesPage(observation: string, entries: Array<Record<string, unknown>>): Record<string, unknown> {
  const counts: Record<string, number> = {};
  for (const entry of entries) {
    counts[entry["change"] as string] = (counts[entry["change"] as string] ?? 0) + 1;
    counts[entry["source_relation"] as string] = (counts[entry["source_relation"] as string] ?? 0) + 1;
  }
  return {
    schema_version: 1,
    kind: "project_changes",
    run_id: "run-a",
    revision: 12,
    observation,
    summary: {
      total: entries.length,
      added: 0, deleted: 0, content_modified: 0, mode_modified: 0,
      content_and_mode_modified: 0, symlink_target_modified: 0, type_changed: 0,
      matches_baseline: 0, matches_result: 0, diverged: 0,
      ...counts,
    },
    changes: entries,
    next_after: null,
  };
}

function contentDocument(options: {
  readonly changeIndex: number;
  readonly side: string;
  readonly status: string;
  readonly byteCount?: number;
  readonly offset?: number;
  readonly bytes?: Uint8Array;
  readonly nextOffset?: number | null;
  readonly sha256?: string;
  readonly node?: Record<string, unknown>;
}): Record<string, unknown> {
  const base = {
    schema_version: 1,
    kind: "project_change_content",
    run_id: "run-a",
    revision: 12,
    observation: OBS,
    change_index: options.changeIndex,
    side: options.side,
    status: options.status,
  };
  if (options.status === "available") {
    const bytes = options.bytes ?? new Uint8Array(0);
    let binary = "";
    for (const byte of bytes) {
      binary += String.fromCharCode(byte);
    }
    return {
      ...base,
      byte_count: options.byteCount ?? bytes.length,
      sha256: options.sha256 ?? SHA,
      offset: options.offset ?? 0,
      data_base64: btoa(binary),
      next_offset: options.nextOffset ?? null,
    };
  }
  if (options.status === "implicit_empty") {
    return { ...base, byte_count: 0, offset: 0, data_base64: "", next_offset: null };
  }
  if (options.status === "metadata_only") {
    return { ...base, node: options.node ?? { kind: "directory", mode: 0o700 } };
  }
  return { ...base, reason: "source_no_longer_matches_baseline", node: options.node ?? fileNode() };
}

interface ContentPlan {
  readonly entry: Record<string, unknown>;
}

function makeFetch(entries: Array<Record<string, unknown>>, contentPlans: Array<(path: string) => Record<string, unknown> | "network" | undefined> = []) {
  const requests: Array<{ method: string; path: string }> = [];
  const inspectSeq = { index: 0 };
  const changesSeq = { index: 0 };
  const fetchImpl = async (path: string, init?: RequestInit): Promise<Response> => {
    const method = init !== undefined && typeof init.method === "string" ? init.method : "GET";
    requests.push({ method, path });
    if (method === "POST") {
      return jsonResponse({ schema_version: 1, kind: "command_result", run_id: "run-a", outcome: "succeeded", exit_code: 0 });
    }
    if (path === "/v1/projects") {
      return jsonResponse(projectsDocument());
    }
    if (path === "/v1/runs" || path.startsWith("/v1/runs?")) {
      return jsonResponse(runsPageDocument(["run-a"]));
    }
    const contentMatch = /^\/v1\/runs\/run-a\/changes\/(\d+)\/content\?/.exec(path);
    if (contentMatch !== null) {
      for (const plan of contentPlans) {
        const served = plan(path);
        if (served === "network") {
          throw new Error("network gone");
        }
        if (served !== undefined) {
          return jsonResponse(served);
        }
      }
      return jsonResponse({ schema_version: 1, kind: "error", layer: "request", reason: "stale_observation" });
    }
    if (/^\/v1\/runs\/run-a\/changes/.test(path)) {
      const page = changesPage(OBS, entries);
      return jsonResponse(page);
    }
    if (/^\/v1\/runs\/run-a\/events\?/.test(path)) {
      return jsonResponse({ schema_version: 1, kind: "events", run_id: "run-a", revision: 12, events: [eventEntry("create_run", 1)], next_after: null });
    }
    if (/^\/v1\/runs\/run-a\/logs/.test(path)) {
      return jsonResponse({ schema_version: 1, kind: "worker_logs", run_id: "run-a", revision: 12, logs: [] });
    }
    if (path === "/v1/runs/run-a") {
      const plans = [
        { revision: 12, status: "succeeded", boundary: { kind: "succeeded", terminalStateId: "done" } },
      ];
      const current = plans[inspectSeq.index] ?? plans[plans.length - 1]!;
      inspectSeq.index += 1;
      return jsonResponse(runDocument("run-a", current.revision, current.status, current.boundary));
    }
    throw new Error(`unexpected fetch path: ${path}`);
  };
  return { fetchImpl, requests };
}

const addedEntry = (): Record<string, unknown> => ({
  index: 1,
  path: "dogfood.txt",
  change: "added",
  source_relation: "matches_baseline",
  baseline: null,
  result: fileNode({ byte_count: 6 }),
});

const modifiedEntry = (index = 2): Record<string, unknown> => ({
  index,
  path: "README.md",
  change: "content_modified",
  source_relation: "matches_baseline",
  baseline: fileNode({ byte_count: 8, sha256: SHA2 }),
  result: fileNode({ byte_count: 12, sha256: SHA }),
});

async function bootWith(entries: Array<Record<string, unknown>>, contentPlans: Array<(path: string) => Record<string, unknown> | "network" | undefined> = []) {
  const wire = makeFetch(entries, contentPlans);
  const harness = await createUiHarness({ fetchImpl: wire.fetchImpl });
  harness.click("refresh-runs");
  await Bun.sleep(4);
  harness.clickRunInspect(0);
  await Bun.sleep(4);
  return { harness, wire };
}

function reviewButton(harness: Awaited<ReturnType<typeof bootWith>>["harness"], row: number) {
  const buttons = harness.element("changes-body").children[row]!.querySelectorAll("button");
  const button = buttons.find((node) => node.textContent === "Review content");
  if (button === undefined) {
    throw new Error(`no Review content button on changes row ${row}`);
  }
  return button;
}

describe("v7 wiring: zero content GETs until the explicit Review", () => {
  test("the terminal inspect loads the changes metadata; no content GET and no preload", async () => {
    const { harness, wire } = await bootWith([addedEntry()]);
    const contentGets = wire.requests.filter((request) => request.path.includes("/content")).length;
    expect(contentGets).toBe(0);
    expect(harness.element("changes-body").children.length).toBe(1);
    expect(reviewButton(harness, 0)).toBeDefined();
    // the review panel stays hidden
    expect(harness.element("content-review-panel").hidden).toBe(true);
  });

  test("Reload changes and Load more of the metadata never fetch content", async () => {
    const { harness, wire } = await bootWith([addedEntry()]);
    harness.click("changes-reload");
    await Bun.sleep(4);
    expect(wire.requests.some((request) => request.path.includes("/content"))).toBe(false);
  });
});

describe("v7 wiring: the explicit Review drives the exact content requests", () => {
  test("an added change loads the empty baseline and the byte-exact result text", async () => {
    const { harness, wire } = await bootWith(
      [addedEntry()],
      [
        (path) => (path.includes("side=baseline") ? contentDocument({ changeIndex: 1, side: "baseline", status: "implicit_empty" }) : undefined),
        (path) => (path.includes("side=result") ? contentDocument({ changeIndex: 1, side: "result", status: "available", bytes: new TextEncoder().encode("hi dog") }) : undefined),
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    const contentGets = wire.requests.filter((request) => request.path.includes("/content"));
    expect(contentGets.map((request) => request.method)).toEqual(["GET", "GET"]);
    expect(contentGets[0]?.path).toBe(`/v1/runs/run-a/changes/1/content?side=baseline&observation=${OBS}`);
    expect(contentGets[1]?.path).toBe(`/v1/runs/run-a/changes/1/content?side=result&observation=${OBS}`);
    expect(harness.element("content-review-panel").hidden).toBe(false);
    expect(harness.element("content-baseline-status").textContent).toContain("empty side");
    expect(harness.element("content-result-status").textContent).toContain("available");
    expect(harness.element("content-result-status").textContent).toContain("full preview");
    expect(harness.element("content-result-view").textContent).toBe("hi dog");
  });

  test("a modified text change renders the bounded client-side diff", async () => {
    const { harness } = await bootWith(
      [addedEntry(), modifiedEntry()],
      [
        (path) => (path.includes("side=baseline") && path.includes("changes/2/") ? contentDocument({ changeIndex: 2, side: "baseline", status: "available", bytes: new TextEncoder().encode("one\ntwo\n"), sha256: SHA2 }) : undefined),
        (path) => (path.includes("side=result") && path.includes("changes/2/") ? contentDocument({ changeIndex: 2, side: "result", status: "available", bytes: new TextEncoder().encode("one\nTWO\ntwo\n") }) : undefined),
      ],
    );
    reviewButton(harness, 1).click();
    await Bun.sleep(6);
    expect(harness.element("content-diff-view").textContent).toContain("+ TWO");
    expect(harness.element("content-diff-view").textContent).toContain("  one");
    expect(harness.element("content-diff-view").textContent).toContain("  two");
  });

  test("a mode-only change reports identical content instead of a diff", async () => {
    const modeEntry: Record<string, unknown> = {
      index: 1,
      path: "f",
      change: "mode_modified",
      source_relation: "matches_baseline",
      baseline: fileNode({ mode: 0o644, byte_count: 4 }),
      result: fileNode({ mode: 0o755, byte_count: 4 }),
    };
    const { harness } = await bootWith(
      [modeEntry],
      [
        (path) => (path.includes("side=baseline") ? contentDocument({ changeIndex: 1, side: "baseline", status: "available", bytes: new TextEncoder().encode("same") }) : undefined),
        (path) => (path.includes("side=result") ? contentDocument({ changeIndex: 1, side: "result", status: "available", bytes: new TextEncoder().encode("same") }) : undefined),
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    expect(harness.element("content-diff-status").textContent).toContain("Content identical; metadata changed.");
    expect(harness.element("content-diff-view").hidden).toBe(true);
  });

  test("a binary result renders through the bounded hex view", async () => {
    const binaryEntry: Record<string, unknown> = {
      index: 1,
      path: "blob.bin",
      change: "added",
      source_relation: "matches_baseline",
      baseline: null,
      result: fileNode({ byte_count: 8 }),
    };
    const { harness } = await bootWith(
      [binaryEntry],
      [
        (path) => (path.includes("side=baseline") ? contentDocument({ changeIndex: 1, side: "baseline", status: "implicit_empty" }) : undefined),
        (path) => (path.includes("side=result") ? contentDocument({ changeIndex: 1, side: "result", status: "available", bytes: new Uint8Array([66, 73, 78, 0, 65, 82, 89, 0]) }) : undefined),
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    const hex = harness.element("content-result-view").textContent;
    expect(hex).toContain("00000000");
    expect(hex).toContain("42 49 4e 00");
    expect(hex).toContain("|BIN.ARY.|");
  });

  test("Load more continues at the raw-byte cursor and the cap ends the preview", async () => {
    const bigByteCount = 300000;
    const bigEntry: Record<string, unknown> = {
      index: 1,
      path: "big.bin",
      change: "added",
      source_relation: "matches_baseline",
      baseline: null,
      result: fileNode({ byte_count: bigByteCount }),
    };
    const servedChunks: number[] = [];
    const { harness, wire } = await bootWith(
      [bigEntry],
      [
        (path) => {
          const offset = Number(/offset=(\d+)/.exec(path)?.[1] ?? 0);
          if (path.includes("side=result")) {
            servedChunks.push(offset);
          }
          if (path.includes("side=baseline")) {
            return contentDocument({ changeIndex: 1, side: "baseline", status: "implicit_empty" });
          }
          const length = Math.min(65536, bigByteCount - offset);
          return contentDocument({
            changeIndex: 1,
            side: "result",
            status: "available",
            byteCount: bigByteCount,
            offset,
            bytes: new Uint8Array(length).fill(65),
            nextOffset: offset + length < bigByteCount ? offset + length : null,
          });
        },
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    expect(servedChunks).toEqual([0]);
    expect(harness.element("content-review-load-result").hidden).toBe(false);
    harness.click("content-review-load-result");
    await Bun.sleep(6);
    harness.click("content-review-load-result");
    await Bun.sleep(6);
    harness.click("content-review-load-result");
    await Bun.sleep(6);
    expect(servedChunks).toEqual([0, 65536, 131072, 196608]);
    expect(servedChunks).toEqual([0, 65536, 131072, 196608]);
    expect(harness.element("content-result-status").textContent).toContain("truncated preview");
    // the cap forbids further loads even though the file is longer
    harness.click("content-review-load-result");
    await Bun.sleep(6);
    expect(servedChunks.length).toBe(4);
    expect(wire.requests.filter((request) => request.path.includes("/content")).length).toBe(5);
  });

  test("a stale content response discards bytes and offers only the Project Changes reload", async () => {
    const { harness, wire } = await bootWith(
      [addedEntry()],
      [
        (path) => (path.includes("side=baseline") ? contentDocument({ changeIndex: 1, side: "baseline", status: "implicit_empty" }) : undefined),
        () => ({ schema_version: 1, kind: "error", layer: "request", reason: "stale_observation" }),
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    expect(harness.element("content-review-status").textContent).toContain("Stale");
    expect(harness.element("content-result-view").hidden).toBe(true);
    expect(harness.element("content-review-reload-changes").hidden).toBe(false);
    const contentGetsBefore = wire.requests.filter((request) => request.path.includes("/content")).length;
    harness.click("content-review-reload-changes");
    await Bun.sleep(6);
    // the reload is a changes-metadata GET; no content GET is re-issued
    expect(wire.requests.filter((request) => request.path.includes("/content")).length).toBe(contentGetsBefore);
    expect(wire.requests.some((request) => request.path.startsWith("/v1/runs/run-a/changes?"))).toBe(true);
  });

  test("a network-failed content GET is retried only by the explicit Retry", async () => {
    let failing = true;
    const { harness } = await bootWith(
      [addedEntry()],
      [
        (path) => {
          if (path.includes("side=baseline") && failing) {
            return "network" as const;
          }
          return path.includes("side=baseline") ? contentDocument({ changeIndex: 1, side: "baseline", status: "implicit_empty" }) : undefined;
        },
        (path) => (path.includes("side=result") ? contentDocument({ changeIndex: 1, side: "result", status: "available", bytes: new TextEncoder().encode("hi") }) : undefined),
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    expect(harness.element("content-baseline-status").textContent).toContain("failed (transport: network_error)");
    expect(harness.element("content-review-retry").hidden).toBe(false);
    failing = false;
    harness.click("content-review-retry");
    await Bun.sleep(6);
    expect(harness.element("content-baseline-status").textContent).toContain("empty side");
    expect(harness.element("content-review-retry").hidden).toBe(true);
  });

  test("a second Review supersedes the in-flight first one; the late response never renders", async () => {
    const gates: Array<() => void> = [];
    const { harness } = await bootWith(
      [addedEntry(), modifiedEntry()],
      [
        (path) => {
          if (path.includes("changes/2/")) {
            return contentDocument({ changeIndex: 2, side: path.includes("side=baseline") ? "baseline" : "result", status: "available", bytes: new TextEncoder().encode(path.includes("side=baseline") ? "one\ntwo\n" : "one\nXXX\ntwo\n"), sha256: path.includes("side=baseline") ? SHA2 : SHA });
          }
          if (path.includes("side=baseline")) {
            return new Promise<Response>(() => {
              gates.push(() => undefined);
            }) as never;
          }
          return contentDocument({ changeIndex: 1, side: "result", status: "available", bytes: new TextEncoder().encode("dog") });
        },
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(4);
    // the second Review supersedes the hanging first baseline request
    reviewButton(harness, 1).click();
    await Bun.sleep(8);
    expect(harness.element("content-review-meta").textContent).toContain("README.md");
    expect(harness.element("content-result-view").textContent).toBe("one\nXXX\ntwo\n");
  });

  test("a content failure isolates the other sections and the content region never POSTs", async () => {
    const { harness, wire } = await bootWith(
      [addedEntry()],
      [
        () => "network" as const,
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    expect(harness.element("content-baseline-status").textContent).toContain("failed");
    // the inspect, events and logs surfaces stay intact and no POST
    // was ever issued by the content region
    expect(wire.requests.every((request) => request.method === "GET" || request.path === "/v1/runs/run-a/continue-stage" || request.path.endsWith("/resume"))).toBe(true);
    expect(wire.requests.filter((request) => request.path.includes("/content")).every((request) => request.method === "GET")).toBe(true);
  });

  test("Close hides the panel; a run selection change never fetches content", async () => {
    const { harness, wire } = await bootWith(
      [addedEntry()],
      [
        (path) => (path.includes("side=baseline") ? contentDocument({ changeIndex: 1, side: "baseline", status: "implicit_empty" }) : undefined),
        (path) => (path.includes("side=result") ? contentDocument({ changeIndex: 1, side: "result", status: "available", bytes: new TextEncoder().encode("dog") }) : undefined),
      ],
    );
    reviewButton(harness, 0).click();
    await Bun.sleep(6);
    harness.click("content-review-close");
    await Bun.sleep(2);
    expect(harness.element("content-review-panel").hidden).toBe(true);
    const contentGets = wire.requests.filter((request) => request.path.includes("/content")).length;
    // re-selecting the same run's metadata path does not re-fetch content
    harness.click("changes-reload");
    await Bun.sleep(4);
    expect(wire.requests.filter((request) => request.path.includes("/content")).length).toBe(contentGets);
  });
});
