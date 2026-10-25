/**
 * Permanent suite of the browser UI v7 Project Change Content model:
 * the dedicated document parser (exact own keys per status, the closed
 * vocabularies, base64/cursor coherence, the exact request binding),
 * the bounded line-diff owner (limits and refusals) and the source
 * scans pinning the one-parser/one-builder/one-transport/one-viewer/
 * one-diff ownership and the read-only boundary of the content region.
 */
import { describe, expect, test } from "bun:test";
import {
  CONTENT_PREVIEW_CHUNK_LIMIT,
  CONTENT_PREVIEW_MAX_BYTES,
  DIFF_MAX_LINES_PER_SIDE,
  DIFF_MAX_WORK_CELLS,
  computeBoundedLineDiff,
  decodePreviewText,
  hexPreviewRows,
  parseApiDocument,
  parseProjectChangeContentDocument,
  projectChangeContentRequestPath,
} from "../ui/app.js";

const OBS = "a".repeat(64);
const SHA = "b".repeat(64);
const BINDING = { runId: "run_x", observation: OBS, changeIndex: 2, side: "result" };

/** Narrows a parsed model to the `available` variant (typed access in tests). */
function expectAvailable(model: ReturnType<typeof parseProjectChangeContentDocument>) {
  if (model.status !== "available") {
    throw new Error(`expected available, got ${model.status}`);
  }
  return model;
}

/** Narrows a parsed model to the `metadata_only` variant. */
function expectMetadataOnly(model: ReturnType<typeof parseProjectChangeContentDocument>) {
  if (model.status !== "metadata_only") {
    throw new Error(`expected metadata_only, got ${model.status}`);
  }
  return model;
}

/** Narrows a parsed model to the `unavailable` variant. */
function expectUnavailable(model: ReturnType<typeof parseProjectChangeContentDocument>) {
  if (model.status !== "unavailable") {
    throw new Error(`expected unavailable, got ${model.status}`);
  }
  return model;
}

function availableDocument(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    kind: "project_change_content",
    run_id: "run_x",
    revision: 12,
    observation: OBS,
    change_index: 2,
    side: "result",
    status: "available",
    byte_count: 2,
    sha256: SHA,
    offset: 0,
    data_base64: "aGk=",
    next_offset: null,
    ...overrides,
  };
}

describe("v7 model: the dedicated content parser", () => {
  test("the available status parses to the exact frozen model with decoded bytes", () => {
    const parsed = parseProjectChangeContentDocument(availableDocument(), BINDING);
    expect(Object.keys(parsed).sort()).toEqual(
      ["changeIndex", "data", "kind", "nextOffset", "observation", "offset", "revision", "runId", "sha256", "side", "status", "byteCount"].sort(),
    );
    const model = expectAvailable(parsed);
    expect(model.status).toBe("available");
    expect(model.byteCount).toBe(2);
    expect(model.sha256).toBe(SHA);
    expect(model.offset).toBe(0);
    expect(model.nextOffset).toBeNull();
    expect(Array.from(model.data)).toEqual([104, 105]);
    expect(Object.isFrozen(model)).toBe(true);
  });

  test("the exact own keys of every status variant; unknown keys are rejected at every level", () => {
    expect(Object.keys(parseProjectChangeContentDocument(availableDocument(), BINDING)).sort()).toEqual(
      ["byteCount", "changeIndex", "data", "kind", "nextOffset", "observation", "offset", "revision", "runId", "sha256", "side", "status"].sort(),
    );
    const implicit = parseProjectChangeContentDocument({
      schema_version: 1, kind: "project_change_content", run_id: "run_x", revision: 12, observation: OBS,
      change_index: 2, side: "baseline", status: "implicit_empty", byte_count: 0, offset: 0, data_base64: "", next_offset: null,
    }, { ...BINDING, side: "baseline" });
    expect(Object.keys(implicit).sort()).toEqual(["byteCount", "changeIndex", "data", "kind", "nextOffset", "observation", "offset", "revision", "runId", "side", "status"].sort());
    const metadata = parseProjectChangeContentDocument({
      schema_version: 1, kind: "project_change_content", run_id: "run_x", revision: 12, observation: OBS,
      change_index: 2, side: "baseline", status: "metadata_only", node: { kind: "directory", mode: 0o700 },
    }, { ...BINDING, side: "baseline" });
    expect(expectMetadataOnly(metadata).node).toEqual({ kind: "directory", mode: 0o700 });
    const unavailable = parseProjectChangeContentDocument({
      schema_version: 1, kind: "project_change_content", run_id: "run_x", revision: 12, observation: OBS,
      change_index: 2, side: "baseline", status: "unavailable", reason: "source_no_longer_matches_baseline",
      node: { kind: "regular_file", mode: 0o600, byte_count: 9, sha256: SHA, multiple_links: false },
    }, { ...BINDING, side: "baseline" });
    expect(expectUnavailable(unavailable).reason).toBe("source_no_longer_matches_baseline");
    for (const extra of [
      availableDocument({ bogus: 1 }),
      availableDocument({ node: { kind: "directory", mode: 0 } }),
    ]) {
      expect(() => parseProjectChangeContentDocument(extra, BINDING)).toThrow();
    }
  });

  test("unknown version, kind, status, side and reason fail closed; no value is echoed", () => {
    expect(() => parseProjectChangeContentDocument(availableDocument({ schema_version: 2 }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ kind: "project_changes" }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ status: "complete" }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ side: "middle" }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(
      availableDocument({ status: "unavailable", reason: "node_has_no_file_content", node: { kind: "regular_file", mode: 0, byte_count: 1, sha256: SHA, multiple_links: false } }),
      { ...BINDING, side: "baseline" },
    )).toThrow();
  });

  test("the response binds exactly to run id, observation, change index and side", () => {
    expect(() => parseProjectChangeContentDocument(availableDocument({ run_id: "other" }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ observation: "b".repeat(64) }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ change_index: 3 }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ side: "baseline" }), BINDING)).toThrow();
    expect(parseProjectChangeContentDocument(availableDocument({ run_id: "other" }), null).runId).toBe("other");
  });

  test("cursor/base64/byte-count coherence: the decoded length, the offsets and the terminal chunk must agree", () => {
    // decoded length 2 but next_offset says 3
    expect(() => parseProjectChangeContentDocument(availableDocument({ next_offset: 3 }), BINDING)).toThrow();
    // non-terminal slice must end exactly at next_offset
    expect(() => parseProjectChangeContentDocument(availableDocument({ next_offset: 1 }), BINDING)).toThrow();
    // offset beyond the byte count
    expect(() => parseProjectChangeContentDocument(availableDocument({ offset: 3 }), BINDING)).toThrow();
    // a terminal empty chunk is only legal at the exact end
    expect(() => parseProjectChangeContentDocument(availableDocument({ data_base64: "" }), BINDING)).toThrow();
    expect(expectAvailable(parseProjectChangeContentDocument(availableDocument({ byte_count: 2, data_base64: "aGk=", offset: 0, next_offset: null }), BINDING)).data.length).toBe(2);
    // a mid-file terminal empty chunk is invalid
    expect(() => parseProjectChangeContentDocument(availableDocument({ byte_count: 5, data_base64: "" }), BINDING)).toThrow();
    // a non-empty next_offset page
    const page = expectAvailable(parseProjectChangeContentDocument(availableDocument({ byte_count: 5, data_base64: "aGk=", next_offset: 2 }), BINDING));
    expect(page.nextOffset).toBe(2);
  });

  test("malformed base64 and non-lowercase digests fail closed", () => {
    expect(() => parseProjectChangeContentDocument(availableDocument({ data_base64: "aGk" }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ data_base64: "aGk==" }), null)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ data_base64: "!!!=" }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument(availableDocument({ sha256: SHA.toUpperCase() }), BINDING)).toThrow();
  });

  test("implicit_empty carries no synthetic digest; metadata_only and unavailable carry no content or cursor keys", () => {
    const implicit = parseProjectChangeContentDocument({
      schema_version: 1, kind: "project_change_content", run_id: "run_x", revision: 12, observation: OBS,
      change_index: 2, side: "result", status: "implicit_empty", byte_count: 0, offset: 0, data_base64: "", next_offset: null,
    }, BINDING);
    expect("sha256" in implicit).toBe(false);
    // a fabricated digest key is an unknown key and fails
    expect(() => parseProjectChangeContentDocument({
      schema_version: 1, kind: "project_change_content", run_id: "run_x", revision: 12, observation: OBS,
      change_index: 2, side: "result", status: "implicit_empty", byte_count: 0, offset: 0, data_base64: "", next_offset: null, sha256: SHA,
    }, BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument({
      schema_version: 1, kind: "project_change_content", run_id: "run_x", revision: 12, observation: OBS,
      change_index: 2, side: "baseline", status: "metadata_only", node: { kind: "symlink", target: "x" }, data_base64: "",
    }, { ...BINDING, side: "baseline" })).toThrow();
    expect(() => parseProjectChangeContentDocument({
      schema_version: 1, kind: "project_change_content", run_id: "run_x", revision: 12, observation: OBS,
      change_index: 2, side: "baseline", status: "unavailable", reason: "source_no_longer_matches_baseline",
      node: { kind: "regular_file", mode: 0o600, byte_count: 1, sha256: SHA, multiple_links: false }, byte_count: 1,
    }, { ...BINDING, side: "baseline" })).toThrow();
  });

  test("hostile strings never become diagnostics: the errors carry typed reasons only", () => {
    const hostile = availableDocument({ data_base64: "<script>alert(1)</script>" });
    try {
      parseProjectChangeContentDocument(hostile, BINDING);
      throw new Error("expected a throw");
    } catch (error) {
      expect(String(error)).not.toContain("script");
    }
    expect(() => parseProjectChangeContentDocument(availableDocument({ observation: "ZZ" }), BINDING)).toThrow();
    expect(() => parseProjectChangeContentDocument("not a record", BINDING)).toThrow();
  });

  test("the general API client keeps rejecting the content kind; only the dedicated parser accepts it", () => {
    expect(() => parseApiDocument(availableDocument())).toThrow();
    const model = parseProjectChangeContentDocument(availableDocument(), BINDING);
    expect(model.kind).toBe("project_change_content");
  });
});

describe("v7 model: the URL builder", () => {
  test("the exact paths: defaults omitted, cursor and limit included when set", () => {
    expect(projectChangeContentRequestPath({ runId: "run_x", changeIndex: 3, side: "baseline", observation: OBS }))
      .toBe(`/v1/runs/run_x/changes/3/content?side=baseline&observation=${OBS}`);
    expect(projectChangeContentRequestPath({ runId: "run_x", changeIndex: 3, side: "result", observation: OBS, offset: 65536 }))
      .toBe(`/v1/runs/run_x/changes/3/content?side=result&observation=${OBS}&offset=65536`);
    expect(projectChangeContentRequestPath({ runId: "run_x", changeIndex: 3, side: "result", observation: OBS, limit: 100 }))
      .toBe(`/v1/runs/run_x/changes/3/content?side=result&observation=${OBS}&limit=100`);
  });

  test("no path, digest, node metadata or run root can enter a content URL", () => {
    for (const hostile of [
      { runId: "../etc", changeIndex: 1, side: "result", observation: OBS },
      { runId: "run_x", changeIndex: 0, side: "result", observation: OBS },
      { runId: "run_x", changeIndex: 1, side: "/workspace/secret", observation: OBS },
      { runId: "run_x", changeIndex: 1, side: "result", observation: "ZZ" },
      { runId: "run_x", changeIndex: 1, side: "result", observation: OBS, limit: 262145 },
      { runId: "run_x", changeIndex: 1, side: "result", observation: OBS, offset: -1 },
    ]) {
      expect(() => projectChangeContentRequestPath(hostile as never)).toThrow();
    }
  });
});

describe("v7 model: text/binary classification and the hex view", () => {
  test("valid UTF-8 decodes; a NUL byte or invalid UTF-8 makes the side binary", () => {
    expect(decodePreviewText(new TextEncoder().encode("héllo"), {})).toEqual({ kind: "text", text: "héllo" });
    expect(decodePreviewText(new Uint8Array([66, 73, 78, 0, 65]), {})).toEqual({ kind: "binary" });
    expect(decodePreviewText(new Uint8Array([0x61, 0xff]), {})).toEqual({ kind: "binary" });
    expect(decodePreviewText(new Uint8Array([0x61, 0xc3]), {})).toEqual({ kind: "binary" });
    // an incomplete trailing sequence in an unfinished preview is not
    // finally invalid: it is excluded from the text instead
    const unfinished = decodePreviewText(new Uint8Array([0x61, 0xc3]), { incompleteTail: true });
    expect(unfinished.kind).toBe("text");
    expect(unfinished.kind === "text" ? unfinished.text : "").toBe("a");
  });

  test("a code point split across chunk boundaries decodes when the pieces are joined", () => {
    const full = new TextEncoder().encode("aéz");
    expect(decodePreviewText(full, {})).toEqual({ kind: "text", text: "aéz" });
    const first = decodePreviewText(full.subarray(0, 2), { incompleteTail: true });
    const second = full.subarray(2);
    expect(first.kind).toBe("text");
    expect(decodePreviewText(new Uint8Array([...full.subarray(0, 2).filter((_, i) => i < 2), ...second]), {})).toEqual({ kind: "text", text: "aéz" });
  });

  test("ANSI escapes and markup-looking text stay plain text", () => {
    const hostile = new TextEncoder().encode("\u001b[31m<b>bold</b>\u001b[0m");
    const decoded = decodePreviewText(hostile, {});
    expect(decoded.kind).toBe("text");
    expect(decoded.kind === "text" && decoded.text.includes("<b>")).toBe(true);
  });

  test("the hex view renders fixed offsets, hex pairs and a safe ASCII column", () => {
    const rows = hexPreviewRows(new Uint8Array([66, 73, 78, 0, 255]), 4096);
    expect(rows.length).toBe(1);
    expect(rows[0]!.offset).toBe("00000000");
    expect(rows[0]!.hex.startsWith("42 49 4e 00 ff")).toBe(true);
    expect(rows[0]!.ascii).toBe("BIN..");
    const wide = hexPreviewRows(new Uint8Array(17), 4096);
    expect(wide.length).toBe(2);
    expect(wide[1]!.offset).toBe("00000010");
    expect(hexPreviewRows(new Uint8Array(3), 2).length).toBe(1);
  });
});

describe("v7 diff: the bounded line-diff owner", () => {
  const enc = (text: string) => new TextEncoder().encode(text);

  test("add, delete, context and the no-final-newline distinction", () => {
    const diff = computeBoundedLineDiff(enc("a\nb\nc\n"), enc("a\nx\nc\n"));
    expect(diff.kind).toBe("diff");
    if (diff.kind !== "diff") return;
    expect(diff.identical).toBe(false);
    expect(diff.rows.map((row) => `${row.type}:${row.text}`)).toEqual(["context:a", "delete:b", "add:x", "context:c"]);
    const noEol = computeBoundedLineDiff(enc("a\nb"), enc("a\nb\n"));
    if (noEol.kind !== "diff") return;
    expect(noEol.rows.map((row) => `${row.type}:${row.text}:${row.noEol}`)).toEqual(["context:a:false", "delete:b:true", "add:b:false"]);
  });

  test("an empty side participates as the honest empty side (added/deleted)", () => {
    const added = computeBoundedLineDiff(new Uint8Array(0), enc("one\ntwo\n"));
    expect(added.kind).toBe("diff");
    if (added.kind !== "diff") return;
    expect(added.rows.map((row) => row.type)).toEqual(["add", "add"]);
    const deleted = computeBoundedLineDiff(enc("one\ntwo\n"), new Uint8Array(0));
    expect(deleted.kind).toBe("diff");
    if (deleted.kind !== "diff") return;
    expect(deleted.rows.map((row) => row.type)).toEqual(["delete", "delete"]);
    const bothEmpty = computeBoundedLineDiff(new Uint8Array(0), new Uint8Array(0));
    expect(bothEmpty.kind).toBe("diff");
    if (bothEmpty.kind !== "diff") return;
    expect(bothEmpty.identical).toBe(true);
  });

  test("identical content short-circuits; mode-only changes diff to identical", () => {
    const diff = computeBoundedLineDiff(enc("same\n"), enc("same\n"));
    expect(diff.kind).toBe("diff");
    expect(diff.kind === "diff" && diff.identical).toBe(true);
    expect(diff.kind === "diff" && diff.rows.length).toBe(0);
  });

  test("binary and NUL-bearing sides refuse as not_text", () => {
    expect(computeBoundedLineDiff(enc("a"), new Uint8Array([0x61, 0x00]))).toEqual({ kind: "refusal", reason: "not_text" });
    expect(computeBoundedLineDiff(enc("a"), new Uint8Array([0x61, 0xff]))).toEqual({ kind: "refusal", reason: "not_text" });
  });

  test("the line limit and the work budget refuse without hanging", () => {
    const big = enc(Array.from({ length: DIFF_MAX_LINES_PER_SIDE + 1 }, (_, i) => `line ${i}`).join("\n"));
    expect(computeBoundedLineDiff(big, enc("x"))).toEqual({ kind: "refusal", reason: "too_many_lines" });
    // two sides under the line cap but over the work budget
    const wideA = enc(`x${"y".repeat(150)}\n`.repeat(1600).slice(0, -1));
    const wideB = enc(`z${"y".repeat(150)}\n`.repeat(1600).slice(0, -1));
    expect(computeBoundedLineDiff(wideA, wideB)).toEqual({ kind: "refusal", reason: "work_budget" });
    // exactly at the budget boundary succeeds
    const edgeA = enc("a\n".repeat(500).slice(0, -1));
    const edgeB = enc("b\n".repeat(500).slice(0, -1));
    const edge = computeBoundedLineDiff(edgeA, edgeB);
    expect(edge.kind).toBe("diff");
    expect(DIFF_MAX_WORK_CELLS).toBe(250000);
  });

  test("hostile HTML content diffs as plain text only", () => {
    const diff = computeBoundedLineDiff(enc("<b>old</b>\n"), enc("<b>new</b>\n"));
    expect(diff.kind).toBe("diff");
    if (diff.kind !== "diff") return;
    expect(diff.rows.some((row) => row.text.includes("<b>new</b>"))).toBe(true);
  });
});

describe("v7 constants", () => {
  test("the UI preview and diff bounds are pinned", () => {
    expect(CONTENT_PREVIEW_CHUNK_LIMIT).toBe(65536);
    expect(CONTENT_PREVIEW_MAX_BYTES).toBe(262144);
    expect(DIFF_MAX_LINES_PER_SIDE).toBe(2000);
    expect(DIFF_MAX_WORK_CELLS).toBe(250000);
  });
});

describe("v7 source scans", () => {
  const appSource = async () => await Bun.file(new URL("../ui/app.js", import.meta.url)).text();

  test("exactly one content parser, one URL builder, one transport, one viewer and one diff owner", async () => {
    const source = await appSource();
    expect(source.match(/function parseProjectChangeContentDocument/g)?.length).toBe(1);
    expect(source.match(/function projectChangeContentRequestPath/g)?.length).toBe(1);
    expect(source.match(/function createProjectChangeContentTransport/g)?.length).toBe(1);
    expect(source.match(/function createProjectChangeContentViewer/g)?.length).toBe(1);
    expect(source.match(/function computeBoundedLineDiff/g)?.length).toBe(1);
  });

  test("the content region never POSTs, never uses timers, storage, innerHTML or Apply", async () => {
    const source = await appSource();
    const contentRegion = source.slice(source.indexOf("Project change content (v7)"), source.indexOf("Live observation (v3)"));
    expect(contentRegion).toContain("createProjectChangeContentViewer");
    expect(contentRegion.includes("method:")).toBe(false);
    expect(contentRegion.includes("body:")).toBe(false);
    expect(contentRegion.includes("FormData")).toBe(false);
    expect(contentRegion.includes("setTimeout")).toBe(false);
    expect(contentRegion.includes("setInterval")).toBe(false);
    expect(contentRegion.includes("localStorage")).toBe(false);
    expect(contentRegion.includes("sessionStorage")).toBe(false);
    expect(contentRegion.includes("innerHTML")).toBe(false);
    expect(/\bApply\b/.test(contentRegion)).toBe(false);
  });

  test("no payload shortcut outside the dedicated state/render path; digests and paths never enter URLs", async () => {
    const source = await appSource();
    // the data field of the content model only flows through the
    // viewer's side state and the two render helpers
    expect(source.match(/model\.data\b/g)?.length).toBe(1);
    // the URL builder never embeds a path, a digest or a node value
    const builder = source.slice(source.indexOf("export function projectChangeContentRequestPath"), source.indexOf("export function createProjectChangeContentTransport"));
    expect(builder.includes("node")).toBe(false);
    expect(builder.includes("sha")).toBe(false);
  });
});
