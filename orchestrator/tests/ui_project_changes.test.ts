/**
 * Permanent model suite of the browser UI v6 read-only Project changes
 * panel: the dedicated fail-closed document parser, the exact node and
 * entry shapes with the per-variant coherence, the summary coherence,
 * the presentation derivation, the pure page merge (initial and
 * continuation), the URL builder and the source restrictions (no
 * content, no POST, no download, no Apply anywhere in the changes
 * owner).
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  applyProjectChangesPage,
  createProjectChangesState,
  parseApiDocument,
  parseProjectChangesDocument,
  projectChangesRequestPath,
  UiContractError,
} from "../ui/app.js";

const OBS = "a".repeat(64);

const summaryOf = (overrides: Record<string, number> = {}): Record<string, number> => ({
  total: 0,
  added: 0,
  deleted: 0,
  content_modified: 0,
  mode_modified: 0,
  content_and_mode_modified: 0,
  symlink_target_modified: 0,
  type_changed: 0,
  matches_baseline: 0,
  matches_result: 0,
  diverged: 0,
  ...overrides,
});

const fileNode = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  kind: "regular_file",
  mode: 0o644,
  byte_count: 8,
  sha256: "b".repeat(64),
  multiple_links: false,
  ...overrides,
});

const entryOf = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  index: 1,
  path: "dogfood.txt",
  change: "added",
  source_relation: "matches_baseline",
  baseline: null,
  result: fileNode(),
  ...overrides,
});

const changesDocument = (overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  schema_version: 1,
  kind: "project_changes",
  run_id: "run-a",
  revision: 12,
  observation: OBS,
  summary: summaryOf(),
  changes: [],
  next_after: null,
  ...overrides,
});

function expectContractError(run: () => unknown, reason?: string): void {
  try {
    run();
    throw new Error("expected the call to throw");
  } catch (error) {
    expect(error instanceof UiContractError).toBe(true);
    if (reason !== undefined) {
      expect((error as UiContractError).reason).toBe(reason);
    }
  }
}

describe("project changes model: the dedicated parser", () => {
  test("the exact document shape parses into the frozen model", () => {
    const model = parseProjectChangesDocument(changesDocument({
      summary: summaryOf({ total: 1, added: 1, matches_baseline: 1 }),
      changes: [entryOf()],
    }));
    expect(model).toEqual({
      kind: "project_changes",
      runId: "run-a",
      revision: 12,
      observation: OBS,
      summary: summaryOf({ total: 1, added: 1, matches_baseline: 1 }),
      changes: [{ index: 1, path: "dogfood.txt", change: "added", source_relation: "matches_baseline", baseline: null, result: fileNode() }] as never,
      nextAfter: null,
    } as never);
    expect(Object.isFrozen(model)).toBe(true);
    expect(Object.isFrozen(model.changes)).toBe(true);
    expect(Object.isFrozen(model.summary)).toBe(true);
    expect(Object.isFrozen(model.changes[0])).toBe(true);
    expect(Object.isFrozen(model.changes[0]?.result)).toBe(true);
  });

  test("unknown schema versions, kinds and extra or missing keys fail closed", () => {
    expectContractError(() => parseProjectChangesDocument({ ...changesDocument(), schema_version: 2 }));
    expectContractError(() => parseProjectChangesDocument({ ...changesDocument(), kind: "project_change" }));
    expectContractError(() => parseProjectChangesDocument({ ...changesDocument(), extra: 1 }));
    const { revision: _drop, ...missing } = changesDocument();
    expectContractError(() => parseProjectChangesDocument(missing));
    expectContractError(() => parseProjectChangesDocument("not a document"));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ run_id: "../escape" })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ run_id: "run-b" }), "run-a"));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ observation: "ZZ" })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ observation: "a".repeat(63) })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ revision: 0 })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ next_after: -1 })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ next_after: 1.5 })));
    // the general client keeps rejecting the document kind fail-closed
    expectContractError(() => parseApiDocument(changesDocument()));
  });

  test("the node union accepts exactly the three variants with exact hardlink metadata", () => {
    const model = parseProjectChangesDocument(changesDocument({
      summary: summaryOf({ total: 3, added: 2, content_modified: 1, matches_baseline: 3 }),
      changes: [
        entryOf({ result: fileNode({ multiple_links: true, hardlink_group: 2, sha256: "c".repeat(64) }) }),
        entryOf({ index: 2, path: "src", change: "added", result: { kind: "directory", mode: 0o700 } }),
        entryOf({ index: 3, path: "latest", change: "added", result: { kind: "symlink", target: "README.md" } }),
      ],
    }));
    expect(model.changes[0]?.result).toEqual(fileNode({ multiple_links: true, hardlink_group: 2, sha256: "c".repeat(64) }) as never);
    expect(model.changes[1]?.result).toEqual({ kind: "directory", mode: 0o700 } as never);
    expect(model.changes[2]?.result).toEqual({ kind: "symlink", target: "README.md" } as never);
  });

  test("node variants reject unknown kinds, out-of-range modes, bad digests and hardlink incoherence", () => {
    const cases: Array<Record<string, unknown>> = [
      { kind: "socket", mode: 0o600 },
      fileNode({ mode: 0o1000 }),
      fileNode({ mode: -1 }),
      fileNode({ byte_count: -1 }),
      fileNode({ sha256: "zz" }),
      fileNode({ multiple_links: "yes" }),
      // the hardlink biconditional of the actual API: the group travels
      // exactly with multiple_links
      fileNode({ multiple_links: true }),
      fileNode({ multiple_links: false, hardlink_group: 1 }),
      fileNode({ hardlink_group: 0 }),
      fileNode({ extra: 1 }),
      { kind: "directory", mode: 0o700, extra: 1 },
      { kind: "symlink" },
    ];
    for (const node of cases) {
      expectContractError(() =>
        parseProjectChangesDocument(changesDocument({
          summary: summaryOf({ total: 1, added: 1, matches_baseline: 1 }),
          changes: [entryOf({ result: node })],
        })),
      );
    }
    // an empty symlink target is verbatim data and stays valid
    const empty = parseProjectChangesDocument(changesDocument({
      summary: summaryOf({ total: 1, added: 1, matches_baseline: 1 }),
      changes: [entryOf({ path: "latest", result: { kind: "symlink", target: "" } })],
    }));
    expect(empty.changes[0]?.result).toEqual({ kind: "symlink", target: "" } as never);
  });

  test("the per-variant coherence rejects every mismatched baseline/result pair", () => {
    const baselineFile = fileNode();
    const cases: Array<[string, unknown, unknown]> = [
      // added needs a null baseline and a non-null result
      ["added", baselineFile, fileNode()],
      ["added", null, null],
      // deleted needs a non-null baseline and a null result
      ["deleted", null, fileNode()],
      ["deleted", baselineFile, baselineFile],
      // content kinds are regular_file on both sides
      ["content_modified", null, fileNode()],
      ["content_modified", baselineFile, null],
      ["content_modified", { kind: "directory", mode: 0o700 }, { kind: "directory", mode: 0o700 }],
      ["content_and_mode_modified", baselineFile, { kind: "symlink", target: "x" }],
      // mode_modified is mode-bearing and kind-stable
      ["mode_modified", baselineFile, { kind: "symlink", target: "x" }],
      ["mode_modified", { kind: "directory", mode: 0o700 }, fileNode()],
      ["mode_modified", baselineFile, null],
      // symlink_target_modified is symlink on both sides
      ["symlink_target_modified", baselineFile, baselineFile],
      ["symlink_target_modified", { kind: "symlink", target: "a" }, fileNode()],
      // type_changed needs different kinds
      ["type_changed", baselineFile, fileNode({ sha256: "d".repeat(64) })],
      ["type_changed", null, fileNode()],
    ];
    for (const [change, baseline, result] of cases) {
      expectContractError(() =>
        parseProjectChangesDocument(changesDocument({
          summary: summaryOf({ total: 1, [change]: 1, matches_baseline: 1 }),
          changes: [entryOf({ change, baseline, result })],
        })),
      );
    }
  });

  test("paths must be relative project paths; indexes strictly increase and paths stay unique", () => {
    for (const path of ["", "/abs.txt", "a/../b.txt", "./x", "a//b", "a/", ".", "..", "a\0b"]) {
      expectContractError(() =>
        parseProjectChangesDocument(changesDocument({
          summary: summaryOf({ total: 1, added: 1, matches_baseline: 1 }),
          changes: [entryOf({ path })],
        })),
      );
    }
    expectContractError(() =>
      parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 2, added: 2, matches_baseline: 2 }),
        changes: [entryOf({ index: 2 }), entryOf({ index: 2, path: "second.txt" })],
      })),
    );
    expectContractError(() =>
      parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 2, added: 2, matches_baseline: 2 }),
        changes: [entryOf(), entryOf({ path: "dogfood.txt", index: 2 })],
      })),
    );
    expectContractError(() =>
      parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 2, added: 2, matches_baseline: 2 }),
        changes: [entryOf(), entryOf({ path: "second.txt", index: 1 })],
      })),
    );
  });

  test("the summary counts are non-negative, kind-coherent and relation-coherent", () => {
    expectContractError(() => parseProjectChangesDocument(changesDocument({ summary: summaryOf({ total: -1 }) })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ summary: summaryOf({ total: 1.5 }) })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ summary: { ...summaryOf({ total: 2, added: 1 }) } })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ summary: { ...summaryOf({ total: 2, matches_baseline: 1 }) } })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ summary: { ...summaryOf({ total: 1, added: 1, matches_baseline: 1 }), extra: 1 } })));
    expectContractError(() => parseProjectChangesDocument(changesDocument({ summary: { total: 0, added: 0, deleted: 0, content_modified: 0, mode_modified: 0 } })));
    // a page may be shorter than the whole set, never longer
    expectContractError(() =>
      parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 0 }),
        changes: [entryOf()],
      })),
    );
  });
});

describe("project changes model: presentation and page merge", () => {
  const emptyState = createProjectChangesState("run-a");

  test("the presentation derivation covers every closed state", () => {
    const present = (overrides: Record<string, number>): string => {
      const count = overrides["total"] ?? 0;
      const changes = Array.from({ length: count }, (_unused, index) =>
        entryOf({ index: index + 1, path: `p${index}.txt` }));
      const model = parseProjectChangesDocument(changesDocument({ summary: summaryOf(overrides), changes }));
      return applyProjectChangesPage(emptyState, model, 0).presentationState;
    };
    expect(present({})).toBe("no_changes");
    expect(present({ total: 2, added: 1, content_modified: 1, diverged: 1, matches_baseline: 1 })).toBe("diverged");
    expect(present({ total: 2, added: 1, content_modified: 1, matches_baseline: 2 })).toBe("pending");
    expect(present({ total: 2, added: 1, content_modified: 1, matches_result: 2 })).toBe("already_present");
    expect(present({ total: 3, added: 2, content_modified: 1, matches_baseline: 2, matches_result: 1 })).toBe("mixed");
    // a summary never carries a presentation state by itself
    expect(createProjectChangesState("run-a").presentationState).toBe("unavailable");
  });

  test("an initial page replaces the snapshot: dense indexes, coherent totals and cursors", () => {
    const model = parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 2, added: 1, content_modified: 1, matches_baseline: 2 }),
      changes: [entryOf(), entryOf({ index: 2, path: "README.md", change: "content_modified", baseline: fileNode(), result: fileNode({ byte_count: 34, sha256: "e".repeat(64) }) })],
      next_after: null,
    }));
    const state = applyProjectChangesPage(emptyState, model, 0);
    expect(state.changes.length).toBe(2);
    expect(state.inspectRevision).toBe(12);
    expect(state.observation).toBe(OBS);
    expect(state.requestState).toBe("ready");
    expect(state.presentationState).toBe("pending");
    expect(state.nextAfter).toBeNull();
    // a first page that starts above 1 is incoherent
    expectContractError(() =>
      applyProjectChangesPage(emptyState, parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 2, added: 2, matches_baseline: 2 }),
        changes: [entryOf({ index: 2 }), entryOf({ index: 3, path: "b.txt" })],
        next_after: null,
      })), 0),
    );
    // an empty page with a non-empty summary is incoherent
    expectContractError(() =>
      applyProjectChangesPage(emptyState, parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 1, added: 1, matches_baseline: 1 }),
        next_after: null,
      })), 0),
    );
    // a non-null cursor on the initial page must equal the loaded count
    expectContractError(() =>
      applyProjectChangesPage(emptyState, parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 2, added: 2, matches_baseline: 2 }),
        changes: [entryOf(), entryOf({ index: 2, path: "b.txt" })],
        next_after: 5,
      })), 0),
    );
    // a null cursor must have loaded the whole set
    expectContractError(() =>
      applyProjectChangesPage(emptyState, parseProjectChangesDocument(changesDocument({
        summary: summaryOf({ total: 3, added: 3, matches_baseline: 3 }),
        changes: [entryOf(), entryOf({ index: 2, path: "b.txt" })],
        next_after: null,
      })), 0),
    );
  });

  test("a continuation extends append-only under the exact observation, revision and cursor contract", () => {
    const first = parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 3, added: 3, matches_baseline: 3 }),
      changes: [entryOf(), entryOf({ index: 2, path: "b.txt" })],
      next_after: 2,
    }));
    const state = applyProjectChangesPage(emptyState, first, 0);
    const second = parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 3, added: 3, matches_baseline: 3 }),
      changes: [entryOf({ index: 3, path: "c.txt" })],
      next_after: null,
    }));
    const merged = applyProjectChangesPage(state, second, 2);
    expect(merged.changes.length).toBe(3);
    expect(merged.changes.map((entry) => entry.index)).toEqual([1, 2, 3]);
    expect(merged.nextAfter).toBeNull();
    // a foreign observation, revision or cursor is rejected without
    // touching the accepted snapshot
    expectContractError(() => applyProjectChangesPage(state, parseProjectChangesDocument({ ...second, observation: "f".repeat(64) }), 2));
    expectContractError(() => applyProjectChangesPage(state, parseProjectChangesDocument({ ...second, revision: 13 }), 2));
    expectContractError(() => applyProjectChangesPage(state, second, 1));
    expectContractError(() => applyProjectChangesPage(state, second, 3));
    // a page without a continuation cursor cannot continue
    const closed = applyProjectChangesPage(emptyState, parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 1, added: 1, matches_baseline: 1 }),
      changes: [entryOf()],
      next_after: null,
    })), 0);
    expectContractError(() => applyProjectChangesPage(closed, second, 1));
    // duplicate paths and gapped or regressive indexes fail
    expectContractError(() => applyProjectChangesPage(state, parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 3, added: 3, matches_baseline: 3 }),
      changes: [entryOf({ index: 3, path: "dogfood.txt" })],
      next_after: null,
    })), 2));
    expectContractError(() => applyProjectChangesPage(state, parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 3, added: 3, matches_baseline: 3 }),
      changes: [entryOf({ index: 4, path: "c.txt" })],
      next_after: null,
    })), 2));
    // a changed summary between pages is incoherent
    expectContractError(() => applyProjectChangesPage(state, parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 4, added: 4, matches_baseline: 4 }),
      changes: [entryOf({ index: 3, path: "c.txt" })],
      next_after: null,
    })), 2));
    // loaded entries can never exceed the total
    expectContractError(() => applyProjectChangesPage(state, parseProjectChangesDocument(changesDocument({
      revision: 12,
      summary: summaryOf({ total: 3, added: 3, matches_baseline: 3 }),
      changes: [entryOf({ index: 3, path: "c.txt" }), entryOf({ index: 4, path: "d.txt" })],
      next_after: 4,
    })), 2));
    // a regressing cursor is incoherent
    expectContractError(() => {
      const growing = applyProjectChangesPage(state, parseProjectChangesDocument(changesDocument({
        revision: 12,
        summary: summaryOf({ total: 4, added: 4, matches_baseline: 4 }),
        changes: [entryOf({ index: 3, path: "c.txt" })],
        next_after: 3,
      })), 2);
      applyProjectChangesPage(growing, parseProjectChangesDocument(changesDocument({
        revision: 12,
        summary: summaryOf({ total: 4, added: 4, matches_baseline: 4 }),
        changes: [entryOf({ index: 4, path: "d.txt" })],
        next_after: 3,
      })), 3);
    });
    // the input state is never mutated by a rejected merge
    expect(state.changes.length).toBe(2);
    expect(state.requestState).toBe("ready");
  });

  test("a foreign run or a hostile state is refused without any effect", () => {
    expectContractError(() => applyProjectChangesPage(emptyState, parseProjectChangesDocument(changesDocument({ run_id: "run-b" })), 0));
    expectContractError(() => applyProjectChangesPage(emptyState, null as never, 0));
    expectContractError(() => applyProjectChangesPage(null as never, parseProjectChangesDocument(changesDocument()), 0));
    expectContractError(() => applyProjectChangesPage(emptyState, parseProjectChangesDocument(changesDocument()), -1));
  });
});

describe("project changes model: the URL builder", () => {
  test("the exact initial and continuation forms with nothing else in the URL", () => {
    expect(projectChangesRequestPath({ runId: "run-a", after: 0, limit: 100 })).toBe("/v1/runs/run-a/changes?after=0&limit=100");
    expect(projectChangesRequestPath({ runId: "run-a", after: 100, limit: 100, observation: OBS })).toBe(`/v1/runs/run-a/changes?after=100&limit=100&observation=${OBS}`);
    expectContractError(() => projectChangesRequestPath({ runId: "../escape", after: 0, limit: 100 }));
    expectContractError(() => projectChangesRequestPath({ runId: "run-a", after: -1, limit: 100 }));
    expectContractError(() => projectChangesRequestPath({ runId: "run-a", after: 0, limit: 0 }));
    expectContractError(() => projectChangesRequestPath({ runId: "run-a", after: 0, limit: 201 }));
    expectContractError(() => projectChangesRequestPath({ runId: "run-a", after: 0, limit: 100, observation: OBS }));
    expectContractError(() => projectChangesRequestPath({ runId: "run-a", after: 1, limit: 100 }));
    expectContractError(() => projectChangesRequestPath({ runId: "run-a", after: 1, limit: 100, observation: "zz" }));
  });
});

describe("project changes model: source restrictions", () => {
  const appSource = readFileSync(join(import.meta.dir, "..", "ui", "app.js"), "utf8");

  test("one owner per changes concern beside the fail-closed general client", () => {
    for (const owner of [
      "export function parseProjectChangesDocument(",
      "export function createProjectChangesState(",
      "export function applyProjectChangesPage(",
      "export function projectChangesRequestPath(",
      "export async function requestProjectChangesJson(",
      "export function createProjectChangesTransport(",
      "export function createProjectChangesViewer(",
    ]) {
      expect(appSource.split(owner).length - 1).toBe(1);
    }
  });

  test("the changes owner carries no content, download, apply or write capability", () => {
    const start = appSource.indexOf("Project changes (v6)");
    // the v6 changes owner ends where the v7 content preview region begins
    const end = appSource.indexOf("Project change content (v7)");
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    const region = appSource.slice(start, end);
    for (const banned of [
      "data_base64",
      "/diff",
      "/apply",
      "showSaveFilePicker",
      "showOpenFilePicker",
      "navigator.clipboard",
      "download",
      "innerHTML",
      "document.write",
      "method: \"POST\"",
      "setTimeout",
      "setInterval",
      "localStorage",
      "sessionStorage",
      "inspectTransport",
      "eventsTransport",
      "logsTransport",
      "mutationGate",
      "postMutation",
    ]) {
      expect(region.includes(banned)).toBe(false);
    }
  });

  test("the run workspace is described relative to the durable baseline, never as worker proof", () => {
    expect(appSource).toContain("run workspace changes relative to the durable project baseline");
    expect(appSource).not.toContain("worker-authored");
    expect(appSource).not.toContain("proven worker");
  });
});
