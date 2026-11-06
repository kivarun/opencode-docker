/**
 * The durable run-wide profile epoch: manifest substrate and store battery.
 */
import { chmod, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { statSync, readFileSync, mkdtempSync, rmSync, mkdirSync, writeFileSync } from "node:fs";
import { afterAll, expect, test } from "bun:test";
import {
  PROFILE_EPOCH_KIND,
  PROFILE_EPOCH_SCHEMA_VERSION,
  pipelineV2ProfileEpochDigest,
  preparePipelineV2ProfileEpoch,
  profileEpochConfigDigest,
  profileEpochEnvValueDigest,
  parsePipelineV2ProfileEpoch,
  validatePipelineV2ProfileEpochBinding,
  requireProfileEpochBindingMatch,
  PipelineV2ProfileEpochError,
  isDigestPinnedImageRef,
  type ProfileEpochProfileEntry,
} from "../src/pipeline_v2_profile_epoch.ts";
import {
  PROFILE_EPOCH_FILE_NAME,
  PipelineV2ProfileEpochStoreError,
  loadPipelineV2ProfileEpoch,
  loadVerifiedPipelineV2ProfileEpoch,
  publishPipelineV2ProfileEpoch,
  verifyPipelineV2ProfileEpochForExistingRun,
} from "../src/pipeline_v2_profile_epoch_store.ts";
import { PipelineV2PublicReasonError } from "../src/pipeline_v2_public_reason.ts";

const ROOTS: string[] = [];

afterAll(async () => {
  for (const root of ROOTS.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

const IMAGE_A = `ghcr.io/example/worker@sha256:${"a".repeat(64)}`;
const IMAGE_B = `ghcr.io/example/worker@sha256:${"b".repeat(64)}`;
const CONFIG_A = '{"model":"model-a"}';
const CONFIG_B = '{"model":"model-b"}';

function entry(options?: {
  id?: string;
  image?: string;
  config?: string;
  bindings?: ProfileEpochProfileEntry["env_bindings"];
}): ProfileEpochProfileEntry {
  return {
    id: options?.id ?? "coder",
    image_ref: options?.image ?? IMAGE_A,
    config_policy: "env_only",
    config_sha256: profileEpochConfigDigest(options?.config ?? CONFIG_A),
    env_bindings: options?.bindings ?? [],
  };
}

const BINDING_SERVER = {
  destination: "LLM_SERVER",
  from_env: "LLM_SERVER",
  required: true,
  secret: false,
  value_sha256: profileEpochEnvValueDigest("https://llm.example/v1"),
} as const;
const BINDING_KEY = {
  destination: "LLM_KEY",
  from_env: "LLM_KEY",
  required: true,
  secret: true,
} as const;

function makeRunRootSync(): string {
  const root = mkdtempSync(join(tmpdir(), "profile-epoch-"));
  ROOTS.push(root);
  const runRoot = join(root, "pipeline-runs", "run-1");
  mkdirSync(runRoot, { recursive: true, mode: 0o700 });
  return runRoot;
}

// --- the pure manifest substrate ---------------------------------------------

test("epoch: prepare returns the exact deep-frozen manifest, canonical bytes and digest", () => {
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  expect(prepared.manifest.schema_version).toBe(PROFILE_EPOCH_SCHEMA_VERSION);
  expect(prepared.manifest.kind).toBe(PROFILE_EPOCH_KIND);
  expect(prepared.canonical_json).toBe(
    `{"kind":"pipeline_v2_profile_epoch","profiles":[{"config_policy":"env_only","config_sha256":"${profileEpochConfigDigest(CONFIG_A)}","env_bindings":[{"destination":"LLM_KEY","from_env":"LLM_KEY","required":true,"secret":true},{"destination":"LLM_SERVER","from_env":"LLM_SERVER","required":true,"secret":false,"value_sha256":"${profileEpochEnvValueDigest("https://llm.example/v1")}"}],"id":"coder","image_ref":"${IMAGE_A}"}],"schema_version":1}`,
  );
  expect(prepared.sha256).toBe(pipelineV2ProfileEpochDigest(prepared.manifest));
  expect(prepared.sha256).toMatch(/^[0-9a-f]{64}$/);
  expect(Object.isFrozen(prepared)).toBe(true);
  expect(Object.isFrozen(prepared.manifest)).toBe(true);
  expect(Object.isFrozen(prepared.manifest.profiles[0])).toBe(true);
  const again = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  expect(again.canonical_json).toBe(prepared.canonical_json);
  expect(again.sha256).toBe(prepared.sha256);
});

test("epoch: profile declaration order is preserved verbatim and is digest-relevant", () => {
  const first = preparePipelineV2ProfileEpoch([entry({ id: "alpha" }), entry({ id: "beta", image: IMAGE_B })]);
  const second = preparePipelineV2ProfileEpoch([entry({ id: "beta", image: IMAGE_B }), entry({ id: "alpha" })]);
  expect(first.manifest.profiles.map((p) => p.id)).toEqual(["alpha", "beta"]);
  expect(first.sha256).not.toBe(second.sha256);
});

test("epoch: the three digest domains are separated and length-framed", () => {
  const config = profileEpochConfigDigest(CONFIG_A);
  const value = profileEpochEnvValueDigest(CONFIG_A);
  const manifest = pipelineV2ProfileEpochDigest(preparePipelineV2ProfileEpoch([entry()]).manifest);
  expect(new Set([config, value, manifest]).size).toBe(3);
  expect(profileEpochConfigDigest(CONFIG_A)).not.toBe(profileEpochConfigDigest(CONFIG_B));
  expect(profileEpochEnvValueDigest(CONFIG_A)).not.toBe(profileEpochEnvValueDigest(CONFIG_B));
  expect(profileEpochEnvValueDigest("ab")).not.toBe(profileEpochEnvValueDigest("abc"));
  expect(profileEpochConfigDigest("")).toMatch(/^[0-9a-f]{64}$/);
});

test("epoch: env bindings must be destination-sorted without duplicates", () => {
  expect(() => preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_SERVER, BINDING_KEY] })])).toThrow(
    /sorted by destination/,
  );
  expect(() =>
    preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, { ...BINDING_KEY, from_env: "OTHER" }] })]),
  ).toThrow(/sorted by destination/);
});

test("epoch: secret bindings never carry a value digest; non-secret carry it exactly when resolved", () => {
  expect(() =>
    preparePipelineV2ProfileEpoch([
      entry({ bindings: [{ ...BINDING_KEY, value_sha256: profileEpochEnvValueDigest("x") }] }),
    ]),
  ).toThrow(/unknown field "value_sha256"/);
  const withoutValue = preparePipelineV2ProfileEpoch([
    entry({
      bindings: [{ destination: "LLM_SERVER", from_env: "LLM_SERVER", required: false, secret: false }],
    }),
  ]);
  expect("value_sha256" in withoutValue.manifest.profiles[0]!.env_bindings[0]!).toBe(false);
  const parsed = parsePipelineV2ProfileEpoch(withoutValue.canonical_json);
  expect(parsed.manifest).toEqual(withoutValue.manifest);
});

test("epoch: exact own-key validation at every level rejects unknown and missing fields", () => {
  expect(() => preparePipelineV2ProfileEpoch([{ ...entry(), extra: 1 } as unknown as ProfileEpochProfileEntry])).toThrow(/unknown field "extra"/);
  expect(() =>
    preparePipelineV2ProfileEpoch([entry({ bindings: [{ ...BINDING_KEY, extra: 1 } as never] })]),
  ).toThrow(/unknown field "extra"/);
  const missing = entry() as unknown as Record<string, unknown>;
  delete missing["config_sha256"];
  expect(() => preparePipelineV2ProfileEpoch([missing as never])).toThrow(/missing required field "config_sha256"/);
  expect(() => preparePipelineV2ProfileEpoch([{ ...entry(), config_policy: "embedded" } as never])).toThrow(
    /config_policy/,
  );
  expect(() => preparePipelineV2ProfileEpoch([{ ...entry(), id: "" } as never])).toThrow(/id/);
  expect(() => preparePipelineV2ProfileEpoch([{ ...entry(), image_ref: "ghcr.io/example/worker:1" } as never])).toThrow(
    /image_ref/,
  );
});

test("epoch: parse rejects malformed JSON with a content-free diagnostic", () => {
  expect(() => parsePipelineV2ProfileEpoch("{not json")).toThrow(/is not valid JSON/);
  expect(() => parsePipelineV2ProfileEpoch('{"schema_version":2,"kind":"pipeline_v2_profile_epoch"}')).toThrow(
    /missing required field "profiles"/,
  );
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  expect(() => parsePipelineV2ProfileEpoch(prepared.canonical_json.replace("env_only", "embedded"))).toThrow(
    /config_policy/,
  );
});

test("epoch: the manifest carries no host paths, no config paths and no secret values", () => {
  const prepared = preparePipelineV2ProfileEpoch([
    entry({
      id: "coder",
      bindings: [BINDING_KEY, BINDING_SERVER],
    }),
  ]);
  for (const canary of [
    "/host",
    "/tmp/",
    "opencode/",
    ".json",
    "LLM_KEY",
    "dhc_",
    "secret-value",
    "password",
  ]) {
    if (canary === "LLM_KEY") {
      continue; // the declared destination name is non-secret binding metadata
    }
    expect(prepared.canonical_json, canary).not.toContain(canary);
  }
  expect(prepared.canonical_json).toContain('"destination":"LLM_KEY"');
  expect(prepared.canonical_json).toContain('"destination":"LLM_SERVER"');
  expect(prepared.canonical_json).toContain('"secret":true');
  expect(Object.keys(prepared.manifest.profiles[0]!.env_bindings[0]!).sort()).toEqual([
    "destination",
    "from_env",
    "required",
    "secret",
  ]);
});

// --- the binding validators ---------------------------------------------------

test("epoch: the state binding validates exact shape, digest and count", () => {
  const binding = { digest: "d".repeat(64), profile_count: 1 };
  expect(validatePipelineV2ProfileEpochBinding(binding, "the durable binding")).toEqual(binding);
  expect(() => validatePipelineV2ProfileEpochBinding({ ...binding, extra: 1 } as never, "binding")).toThrow(
    /unknown field "extra"/,
  );
  expect(() => validatePipelineV2ProfileEpochBinding({ digest: "D".repeat(64), profile_count: 1 }, "binding")).toThrow(
    /digest/,
  );
  expect(() => validatePipelineV2ProfileEpochBinding({ digest: "d".repeat(64), profile_count: 1.5 }, "binding")).toThrow(
    /profile_count/,
  );
  expect(() =>
    validatePipelineV2ProfileEpochBinding({ digest: "d".repeat(64), profile_count: -1 }, "binding"),
  ).toThrow(/profile_count/);
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  expect(() => requireProfileEpochBindingMatch(prepared, { digest: "e".repeat(64), profile_count: 1 })).toThrow(
    /does not match the stored profile epoch manifest/,
  );
  expect(() =>
    requireProfileEpochBindingMatch(prepared, { digest: prepared.sha256, profile_count: 2 }),
  ).toThrow(/does not match the stored profile epoch manifest/);
  expect(requireProfileEpochBindingMatch(prepared, { digest: prepared.sha256, profile_count: 1 })).toBeUndefined();
  expect(() => preparePipelineV2ProfileEpoch([])).not.toThrow();
  const empty = preparePipelineV2ProfileEpoch([]);
  expect(empty.manifest.profiles).toHaveLength(0);
  expect(empty.sha256).toMatch(/^[0-9a-f]{64}$/);
});

test("epoch: the digest-pinned image grammar accepts tag-pinned refs and rejects tags as identity", () => {
  expect(isDigestPinnedImageRef("ghcr.io/acme/worker@sha256:" + "0".repeat(64))).toBe(true);
  expect(isDigestPinnedImageRef("registry.example:5000/team/worker:fixed@sha256:" + "0".repeat(64))).toBe(true);
  expect(isDigestPinnedImageRef("ghcr.io/acme/worker:1")).toBe(false);
  expect(isDigestPinnedImageRef("sha256:" + "0".repeat(64))).toBe(false);
  expect(isDigestPinnedImageRef(`ghcr.io/acme/worker@sha256:${"0".repeat(63)}`)).toBe(false);
  expect(isDigestPinnedImageRef(`ghcr.io/acme/worker@sha256:${"0".repeat(64)}` + "\n")).toBe(false);
  expect(isDigestPinnedImageRef("")).toBe(false);
});

// --- the store ----------------------------------------------------------------

test("epoch store: publication writes the canonical bytes at mode 0600 with the fixed name", async () => {
  const runRoot = makeRunRootSync();
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  await publishPipelineV2ProfileEpoch(runRoot, prepared);
  const path = join(runRoot, PROFILE_EPOCH_FILE_NAME);
  const st = statSync(path);
  expect(st.isFile()).toBe(true);
  expect(st.mode & 0o777).toBe(0o600);
  expect(await readFile(path, "utf8")).toBe(prepared.canonical_json);
  const loaded = await loadPipelineV2ProfileEpoch(runRoot);
  expect(loaded!.canonical_json).toBe(prepared.canonical_json);
  const verified = await loadVerifiedPipelineV2ProfileEpoch(runRoot, {
    digest: prepared.sha256,
    profile_count: 1,
  });
  expect(verified.canonical_json).toBe(prepared.canonical_json);
});

test("epoch store: an exact retry adopts the published artifact byte- and inode-identically", async () => {
  const runRoot = makeRunRootSync();
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  await publishPipelineV2ProfileEpoch(runRoot, prepared);
  const path = join(runRoot, PROFILE_EPOCH_FILE_NAME);
  const before = statSync(path);
  await publishPipelineV2ProfileEpoch(runRoot, prepared);
  const after = statSync(path);
  expect(after.ino).toBe(before.ino);
  expect(after.mtimeMs).toBe(before.mtimeMs);
  expect(readFileSync(path, "utf8")).toBe(prepared.canonical_json);
});

test("epoch store: a different manifest at the same run root is a typed conflict with zero overwrite", async () => {
  const runRoot = makeRunRootSync();
  const first = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  const second = preparePipelineV2ProfileEpoch([entry({ image: IMAGE_B })]);
  await publishPipelineV2ProfileEpoch(runRoot, first);
  try {
    await publishPipelineV2ProfileEpoch(runRoot, second);
    throw new Error("expected a conflict");
  } catch (cause) {
    expect(cause).toBeInstanceOf(PipelineV2ProfileEpochStoreError);
    const storeError = cause as PipelineV2ProfileEpochStoreError;
    expect(storeError.outcome).toBe("not_published");
    expect(storeError.reason).toBe("conflict");
  }
  expect(readFileSync(join(runRoot, PROFILE_EPOCH_FILE_NAME), "utf8")).toBe(first.canonical_json);
});

test("epoch store: missing, wrong-mode, symlinked and foreign-kind artifacts are typed refusals", async () => {
  const missingRoot = makeRunRootSync();
  expect(await loadPipelineV2ProfileEpoch(missingRoot)).toBeNull();
  try {
    await loadVerifiedPipelineV2ProfileEpoch(missingRoot, { digest: "d".repeat(64), profile_count: 1 });
    throw new Error("expected a failure");
  } catch (cause) {
    expect((cause as PipelineV2ProfileEpochStoreError).outcome).toBe("not_published");
    expect((cause as PipelineV2ProfileEpochStoreError).reason).toBe("invalid_layout");
  }

  const wrongModeRoot = makeRunRootSync();
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  await writeFile(join(wrongModeRoot, PROFILE_EPOCH_FILE_NAME), prepared.canonical_json, { mode: 0o644 });
  try {
    await loadVerifiedPipelineV2ProfileEpoch(wrongModeRoot, { digest: prepared.sha256, profile_count: 1 });
    throw new Error("expected a failure");
  } catch (cause) {
    expect((cause as PipelineV2ProfileEpochStoreError).reason).toBe("conflict");
  }

  const symlinkRoot = makeRunRootSync();
  const target = join(symlinkRoot, "elsewhere.json");
  await writeFile(target, prepared.canonical_json, { mode: 0o600 });
  await symlink(target, join(symlinkRoot, PROFILE_EPOCH_FILE_NAME));
  try {
    await loadVerifiedPipelineV2ProfileEpoch(symlinkRoot, { digest: prepared.sha256, profile_count: 1 });
    throw new Error("expected a failure");
  } catch (cause) {
    expect((cause as PipelineV2ProfileEpochStoreError).reason).toBe("conflict");
  }
});

test("epoch store: tampered bytes and a foreign canonical document fail the verification", async () => {
  const runRoot = makeRunRootSync();
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  await writeFile(join(runRoot, PROFILE_EPOCH_FILE_NAME), prepared.canonical_json.replace("env_only", "embedded"), {
    mode: 0o600,
  });
  try {
    await loadVerifiedPipelineV2ProfileEpoch(runRoot, { digest: prepared.sha256, profile_count: 1 });
    throw new Error("expected a failure");
  } catch (cause) {
    expect((cause as PipelineV2ProfileEpochStoreError).reason).toBe("conflict");
  }
  // a valid canonical document of a different run still mismatches the binding
  const foreignRoot = makeRunRootSync();
  await publishPipelineV2ProfileEpoch(foreignRoot, prepared);
  try {
    await loadVerifiedPipelineV2ProfileEpoch(foreignRoot, { digest: "e".repeat(64), profile_count: 1 });
    throw new Error("expected a failure");
  } catch (cause) {
    expect((cause as PipelineV2ProfileEpochStoreError).reason).toBe("conflict");
  }
});

// --- the existing-run verifier ------------------------------------------------

test("epoch verifier: exact match succeeds and drift is the typed public refusal", async () => {
  const runRoot = makeRunRootSync();
  const prepared = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  await publishPipelineV2ProfileEpoch(runRoot, prepared);
  const binding = { digest: prepared.sha256, profile_count: 1 };
  await verifyPipelineV2ProfileEpochForExistingRun({ runRoot, binding, profileEntries: [entry({ bindings: [BINDING_KEY, BINDING_SERVER] })] });
  await expect(
    verifyPipelineV2ProfileEpochForExistingRun({
      runRoot,
      binding,
      profileEntries: [entry({ image: IMAGE_B, bindings: [BINDING_KEY, BINDING_SERVER] })],
    }),
  ).rejects.toMatchObject({ reason: "profile_epoch_mismatch" });
  await expect(
    verifyPipelineV2ProfileEpochForExistingRun({
      runRoot,
      binding: { digest: prepared.sha256, profile_count: 2 },
      profileEntries: [entry({ bindings: [BINDING_KEY, BINDING_SERVER] })],
    }),
  ).rejects.toMatchObject({ reason: "artifact_modified" });
  // a recomputed manifest that no longer matches the durable count drifts
  await expect(
    verifyPipelineV2ProfileEpochForExistingRun({
      runRoot,
      binding,
      profileEntries: [entry({ id: "alpha" }), entry({ id: "beta", image: IMAGE_B })],
    }),
  ).rejects.toMatchObject({ reason: "profile_epoch_mismatch" });
  // secret rotation of the declared destination keeps the identity stable:
  // the recomputed manifest is identical because secrets never enter it
  const rotated = preparePipelineV2ProfileEpoch([entry({ bindings: [BINDING_KEY, BINDING_SERVER] })]);
  expect(rotated.sha256).toBe(prepared.sha256);
});
