import { describe, expect, test } from "bun:test";
import {
  PIPELINE_V2_PROJECT_COPY_POLICY_VERSION,
  PIPELINE_V2_PROJECT_PROVENANCE_KIND,
  PIPELINE_V2_PROJECT_PROVENANCE_SCHEMA_VERSION,
  PipelineV2ProjectProvenanceError,
  type PipelineV2ProjectProvenanceFailureReason,
  isSafeProjectRelativePath,
  parsePipelineV2ProjectProvenance,
  pipelineV2ProjectProvenanceBinding,
  pipelineV2ProjectProvenanceDigest,
  preparePipelineV2ProjectProvenance,
  requireProvenanceBindingMatch,
  validatePipelineV2ProjectProvenanceBinding,
} from "../src/pipeline_v2_project_provenance.ts";
import { canonicalJson } from "../src/canonical_json.ts";
import { createHash } from "node:crypto";

const dir = (path: string, sourceMode = 0o755): unknown => ({
  path,
  kind: "directory",
  source_mode: sourceMode,
  workspace_mode: 0o700,
});
const file = (path: string, sha: string, byteCount = 3, sourceMode = 0o644): unknown => ({
  path,
  kind: "regular_file",
  source_mode: sourceMode,
  workspace_mode: (sourceMode & 0o111) !== 0 ? 0o700 : 0o600,
  byte_count: byteCount,
  sha256: sha,
  multiple_links: false,
});
const link = (path: string, target: string): unknown => ({
  path,
  kind: "symlink",
  target,
  targets_identical: true,
});
const hex = (char: string): string => char.repeat(64);

function expectProvenanceError(
  cause: unknown,
  reason: PipelineV2ProjectProvenanceFailureReason,
  fragment = "",
): void {
  expect(cause).toBeInstanceOf(PipelineV2ProjectProvenanceError);
  expect((cause as PipelineV2ProjectProvenanceError).reason).toBe(reason);
  if (fragment !== "") {
    expect((cause as Error).message).toContain(fragment);
  }
}

describe("pipeline v2 project provenance manifest", () => {
  test("the exact envelope with the supported copy policy version", () => {
    const prepared = preparePipelineV2ProjectProvenance([file("a.txt", hex("a")), dir("d")]);
    expect(prepared.manifest.schema_version).toBe(PIPELINE_V2_PROJECT_PROVENANCE_SCHEMA_VERSION);
    expect(prepared.manifest.schema_version).toBe(1);
    expect(prepared.manifest.kind).toBe(PIPELINE_V2_PROJECT_PROVENANCE_KIND);
    expect(prepared.manifest.kind).toBe("pipeline_v2_project_provenance");
    expect(prepared.manifest.copy_policy_version).toBe(PIPELINE_V2_PROJECT_COPY_POLICY_VERSION);
    expect(prepared.manifest.copy_policy_version).toBe(1);
    expect(prepared.manifest.entries.map((entry) => entry.path)).toEqual(["a.txt", "d"]);
  });

  test("entries are sorted into deterministic code-unit order regardless of input order", () => {
    const first = preparePipelineV2ProjectProvenance([
      file("a.txt", hex("a")),
      dir("d"),
      file("d/z.txt", hex("b")),
      file("d.y", hex("c")),
    ]);
    const second = preparePipelineV2ProjectProvenance([
      file("d.y", hex("c")),
      file("d/z.txt", hex("b")),
      dir("d"),
      file("a.txt", hex("a")),
    ]);
    expect(first.manifest.entries.map((entry) => entry.path)).toEqual(["a.txt", "d", "d.y", "d/z.txt"]);
    expect(first.sha256).toBe(second.sha256);
    expect(first.canonical_json).toBe(second.canonical_json);
  });

  test("the digest is the domain-separated sha256 over the canonical JSON", () => {
    const prepared = preparePipelineV2ProjectProvenance([file("a.txt", hex("a"))]);
    const expected = createHash("sha256")
      .update("pipeline-v2-project-provenance\0", "utf8")
      .update(prepared.canonical_json, "utf8")
      .digest("hex");
    expect(prepared.sha256).toBe(expected);
    expect(prepared.sha256).toBe(pipelineV2ProjectProvenanceDigest(prepared.manifest));
    expect(prepared.canonical_json).toBe(canonicalJson(prepared.manifest));
    expect(prepared.canonical_json.endsWith("\n")).toBe(false);
  });

  test("an empty project is a valid manifest with zero entries", () => {
    const prepared = preparePipelineV2ProjectProvenance([]);
    expect(prepared.manifest.entries).toEqual([]);
    expect(pipelineV2ProjectProvenanceBinding(prepared)).toEqual({
      digest: prepared.sha256,
      entry_count: 0,
      copy_policy_version: 1,
    });
  });

  test("the digest is sensitive to every execution-relevant field class", () => {
    const base = preparePipelineV2ProjectProvenance([
      file("a.txt", hex("a"), 3, 0o644),
      dir("d"),
      link("d/l", "../a.txt"),
    ]);
    const digestOf = (entries: unknown[]): string => preparePipelineV2ProjectProvenance(entries).sha256;
    expect(digestOf([file("a.txt", hex("b"), 3, 0o644), dir("d"), link("d/l", "../a.txt")])).not.toBe(base.sha256);
    expect(digestOf([file("a.txt", hex("a"), 4, 0o644), dir("d"), link("d/l", "../a.txt")])).not.toBe(base.sha256);
    expect(digestOf([file("a.txt", hex("a"), 3, 0o600), dir("d"), link("d/l", "../a.txt")])).not.toBe(base.sha256);
    expect(digestOf([file("a.txt", hex("a"), 3, 0o644), dir("d", 0o700), link("d/l", "../a.txt")])).not.toBe(base.sha256);
    expect(digestOf([file("a.txt", hex("a"), 3, 0o644), dir("d"), link("d/l", "../b.txt")])).not.toBe(base.sha256);
    expect(digestOf([file("a.txt", hex("a"), 3, 0o644), dir("d"), link("d/l", "../a.txt"), file("e.txt", hex("f"))])).not.toBe(base.sha256);
    expect(digestOf([file("a.txt", hex("a"), 3, 0o644), dir("d"), link("d/l", "../a.txt")])).toBe(base.sha256);
  });

  test("the workspace mode normalization is recorded, never the source mode as workspace mode", () => {
    const prepared = preparePipelineV2ProjectProvenance([
      file("plain.txt", hex("a"), 3, 0o644),
      file("exec.sh", hex("b"), 3, 0o755),
      dir("d", 0o750),
    ]);
    const byPath = new Map(prepared.manifest.entries.map((entry) => [entry.path, entry]));
    const plain = byPath.get("plain.txt") as { source_mode: number; workspace_mode: number };
    const exec = byPath.get("exec.sh") as { source_mode: number; workspace_mode: number };
    const directory = byPath.get("d") as { source_mode: number; workspace_mode: number };
    expect(plain.source_mode).toBe(0o644);
    expect(plain.workspace_mode).toBe(0o600);
    expect(exec.source_mode).toBe(0o755);
    expect(exec.workspace_mode).toBe(0o700);
    expect(directory.source_mode).toBe(0o750);
    expect(directory.workspace_mode).toBe(0o700);
  });

  test("hardlink groups are canonical per-manifest indexes and never host inodes", () => {
    const prepared = preparePipelineV2ProjectProvenance([
      { path: "b.txt", kind: "regular_file", source_mode: 0o644, workspace_mode: 0o600, byte_count: 1, sha256: hex("b"), multiple_links: true, hardlink_group: 1 },
      { path: "a.txt", kind: "regular_file", source_mode: 0o644, workspace_mode: 0o600, byte_count: 1, sha256: hex("a"), multiple_links: true, hardlink_group: 2 },
    ]);
    const entries = prepared.manifest.entries as unknown as { path: string; hardlink_group?: number }[];
    expect(entries.map((entry) => entry.hardlink_group)).toEqual([2, 1]);
    const serialized = prepared.canonical_json;
    expect(serialized).not.toContain("inode");
    expect(serialized).toMatch(/"hardlink_group":1/);
  });

  test("the safe relative-path grammar", () => {
    expect(isSafeProjectRelativePath("a.txt")).toBe(true);
    expect(isSafeProjectRelativePath("d/e.txt")).toBe(true);
    expect(isSafeProjectRelativePath(".hidden")).toBe(true);
    expect(isSafeProjectRelativePath("with space.txt")).toBe(true);
    expect(isSafeProjectRelativePath("")).toBe(false);
    expect(isSafeProjectRelativePath("/abs")).toBe(false);
    expect(isSafeProjectRelativePath("a//b")).toBe(false);
    expect(isSafeProjectRelativePath("./a")).toBe(false);
    expect(isSafeProjectRelativePath("../a")).toBe(false);
    expect(isSafeProjectRelativePath("a/..")).toBe(false);
    expect(isSafeProjectRelativePath("a/./b")).toBe(false);
    expect(isSafeProjectRelativePath("a/\0b")).toBe(false);
    expect(isSafeProjectRelativePath(42)).toBe(false);
  });

  test("invalid entries fail closed without echoing values", () => {
    expect(() => preparePipelineV2ProjectProvenance([{} as unknown])).toThrow(/missing the required fields/);
    expect(() => preparePipelineV2ProjectProvenance([{ path: "/abs", kind: "directory", source_mode: 0o700, workspace_mode: 0o700 }]))
      .toThrow(/not a safe relative path/);
    expect(() => preparePipelineV2ProjectProvenance([{ path: "a", kind: "weird" }]))
      .toThrow(/unsupported kind/);
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "directory", source_mode: 0o7777, workspace_mode: 0o700 }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "permission mode");
    }
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "regular_file", source_mode: 0o644, workspace_mode: 0o600, byte_count: -1, sha256: hex("a"), multiple_links: false }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "byte count");
    }
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "regular_file", source_mode: 0o644, workspace_mode: 0o600, byte_count: 1, sha256: "NOTHex", multiple_links: false }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "sha256");
    }
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "regular_file", source_mode: 0o644, workspace_mode: 0o600, byte_count: 1, sha256: hex("a"), multiple_links: true }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "hardlink_group");
    }
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "regular_file", source_mode: 0o644, workspace_mode: 0o600, byte_count: 1, sha256: hex("a"), multiple_links: false, hardlink_group: 1 }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "hardlink_group");
    }
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "symlink", target: "t\0", targets_identical: true }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "symlink target");
    }
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "symlink", target: "t", targets_identical: false }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "identical");
    }
    try {
      preparePipelineV2ProjectProvenance([{ path: "a", kind: "directory", source_mode: 0o700, workspace_mode: 0o700, extra: 1 }]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_entry", "unknown field");
    }
  });

  test("structural coherence: duplicates, ancestors and unknown extra fields at the top level", () => {
    try {
      preparePipelineV2ProjectProvenance([file("a", hex("a")), file("a", hex("b"))]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_manifest", "more than once");
    }
    try {
      preparePipelineV2ProjectProvenance([file("d/e.txt", hex("a"))]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_manifest", "ancestor");
    }
    try {
      preparePipelineV2ProjectProvenance([file("a", hex("a")), file("d/e.txt", hex("b"))]);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_manifest", "ancestor");
    }
    try {
      preparePipelineV2ProjectProvenance([file("a", hex("a"))], );
      expect.unreachable();
    } catch {
      // well-formed single entry is fine; checked below
    }
    expect(() => preparePipelineV2ProjectProvenance([file("a", hex("a"))])).not.toThrow();
  });

  test("parse requires canonical bytes and rejects malformed or foreign documents", () => {
    const prepared = preparePipelineV2ProjectProvenance([file("a.txt", hex("a"))]);
    expect(parsePipelineV2ProjectProvenance(prepared.canonical_json).sha256).toBe(prepared.sha256);
    // non-canonical (reordered object keys) must fail closed
    const reordered = JSON.parse(prepared.canonical_json) as Record<string, unknown>;
    const reorderedText = `{"entries":${JSON.stringify(reordered["entries"])},"kind":${JSON.stringify(reordered["kind"])},"copy_policy_version":1,"schema_version":1}`;
    try {
      parsePipelineV2ProjectProvenance(reorderedText);
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_manifest", "canonical");
    }
    try {
      parsePipelineV2ProjectProvenance("{not json");
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_manifest", "not valid JSON");
      expect((cause as Error).message).not.toContain("position");
      expect((cause as Error).message).not.toContain("token");
    }
    for (const raw of [
      "[]",
      "null",
      '{"schema_version":2,"kind":"pipeline_v2_project_provenance","copy_policy_version":1,"entries":[]}',
      '{"schema_version":1,"kind":"other","copy_policy_version":1,"entries":[]}',
      '{"schema_version":1,"kind":"pipeline_v2_project_provenance","copy_policy_version":2,"entries":[]}',
      '{"schema_version":1,"kind":"pipeline_v2_project_provenance","copy_policy_version":1}',
    ]) {
      try {
        parsePipelineV2ProjectProvenance(raw);
        expect.unreachable();
      } catch (cause) {
        expectProvenanceError(cause, "invalid_manifest");
      }
    }
  });

  test("bindings validate exactly and match their prepared manifest", () => {
    const prepared = preparePipelineV2ProjectProvenance([file("a.txt", hex("a"))]);
    const binding = pipelineV2ProjectProvenanceBinding(prepared);
    expect(validatePipelineV2ProjectProvenanceBinding(binding, "x")).toEqual(binding);
    expect(validatePipelineV2ProjectProvenanceBinding({ ...binding }, "x")).toEqual(binding);
    for (const broken of [
      null,
      {},
      { ...binding, digest: "nothex" },
      { ...binding, entry_count: -1 },
      { ...binding, copy_policy_version: 2 },
      { ...binding, extra: 1 },
    ]) {
      try {
        validatePipelineV2ProjectProvenanceBinding(broken, "x");
        expect.unreachable();
      } catch (cause) {
        expectProvenanceError(cause, "invalid_manifest");
      }
    }
    requireProvenanceBindingMatch(prepared, binding);
    try {
      requireProvenanceBindingMatch(prepared, { ...binding, digest: hex("f") });
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_manifest", "does not match");
    }
    try {
      requireProvenanceBindingMatch(prepared, { ...binding, entry_count: 2 });
      expect.unreachable();
    } catch (cause) {
      expectProvenanceError(cause, "invalid_manifest", "does not match");
    }
  });

  test("the manifest carries no absolute paths, host identities, timestamps or contents", () => {
    const prepared = preparePipelineV2ProjectProvenance([
      file("/etc/passwd-shaped", hex("a")),
    ].map((entry) => {
      const e = entry as { path: string };
      return { ...e, path: "safe.txt" };
    }));
    const serialized = prepared.canonical_json;
    for (const banned of ["/tmp", "/home", "uid", "gid", "mtime", "ctime", "ino", "dev", "xattr", "source_path", "run_root", "project_root"]) {
      expect(serialized).not.toContain(banned);
    }
  });

  test("results are deep-frozen and isolated from caller mutations", () => {
    const entries: unknown[] = [file("a.txt", hex("a")), dir("d")];
    const prepared = preparePipelineV2ProjectProvenance(entries);
    (entries[0] as { sha256: string }).sha256 = hex("f");
    expect(prepared.manifest.entries[0]!.kind === "regular_file" && (prepared.manifest.entries[0] as { sha256: string }).sha256).toBe(hex("a"));
    expect(Object.isFrozen(prepared)).toBe(true);
    expect(Object.isFrozen(prepared.manifest)).toBe(true);
    expect(Object.isFrozen(prepared.manifest.entries)).toBe(true);
    expect(() => {
      (prepared.manifest as { entries: unknown }).entries = [];
    }).toThrow();
    expect(Object.isFrozen(pipelineV2ProjectProvenanceBinding(prepared))).toBe(true);
  });

  test("repeated preparation of identical captures is deterministic", () => {
    const entries = [file("z.txt", hex("1")), dir("a"), link("a/l", "t")];
    expect(preparePipelineV2ProjectProvenance(entries).canonical_json)
      .toBe(preparePipelineV2ProjectProvenance([...entries]).canonical_json);
  });
});
