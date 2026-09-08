import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  ProfileError,
  isControlEnvName,
  loadProfile,
  parseProfileSpec,
  resolveEnvBindings,
  validateProfileName,
} from "../src/profile.ts";

const OPENCODE_CONFIG = '{"$schema":"https://opencode.ai/config.json","model":"test/model"}';

async function withConfigRoot(
  files: { profiles?: Record<string, string>; opencode?: Record<string, string>; extra?: Record<string, string> },
  fn: (root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "profile-test-"));
  try {
    await mkdir(join(root, "profiles"), { recursive: true });
    await mkdir(join(root, "opencode"), { recursive: true });
    for (const [name, content] of Object.entries(files.profiles ?? {})) {
      await writeFile(join(root, "profiles", `${name}.json`), content);
    }
    for (const [name, content] of Object.entries(files.opencode ?? {})) {
      await writeFile(join(root, "opencode", name), content);
    }
    for (const [name, content] of Object.entries(files.extra ?? {})) {
      await writeFile(join(root, name), content);
    }
    await fn(root);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

function profileBody(overrides: Record<string, unknown> = {}): string {
  return JSON.stringify({
    schema_version: 1,
    image: "ghcr.io/kivarun/opencode-docker/base:latest",
    opencode_config: "opencode/default.json",
    env: {
      LLM_SERVER: { from_env: "LLM_SERVER", required: true },
      LLM_KEY: { from_env: "LLM_KEY", required: true },
      OPENCODE_ENABLE_EXA: { from_env: "OPENCODE_ENABLE_EXA", required: false },
    },
    ...overrides,
  });
}

const BASE_ENV = {
  LLM_SERVER: "https://llm.example/v1",
  LLM_KEY: "sk-supersecret-value-42",
  OPENCODE_ENABLE_EXA: "1",
};

test("profile: valid named profile loads with resolved bindings", async () => {
  await withConfigRoot(
    { profiles: { default: profileBody() }, opencode: { "default.json": OPENCODE_CONFIG } },
    async (root) => {
      const profile = await loadProfile(root, "default", BASE_ENV);
      expect(profile.profileName).toBe("default");
      expect(profile.image).toBe("ghcr.io/kivarun/opencode-docker/base:latest");
      expect(profile.opencodeConfigContent).toBe(OPENCODE_CONFIG);
      expect(profile.opencodeConfigPath.startsWith(root)).toBe(true);
      expect(profile.env).toEqual({
        LLM_SERVER: "https://llm.example/v1",
        LLM_KEY: "sk-supersecret-value-42",
        OPENCODE_ENABLE_EXA: "1",
      });
    },
  );
});

test("profile: empty env bindings are allowed", async () => {
  await withConfigRoot(
    {
      profiles: {
        minimal: JSON.stringify({
          schema_version: 1,
          image: "alpine:3.22",
          opencode_config: "opencode/default.json",
          env: {},
        }),
      },
      opencode: { "default.json": OPENCODE_CONFIG },
    },
    async (root) => {
      const profile = await loadProfile(root, "minimal", {});
      expect(profile.env).toEqual({});
      expect(profile.image).toBe("alpine:3.22");
    },
  );
});

test("profile: unknown profile is rejected", async () => {
  await withConfigRoot({ profiles: { default: profileBody() }, opencode: { "default.json": OPENCODE_CONFIG } }, async (root) => {
    await expect(loadProfile(root, "missing", BASE_ENV)).rejects.toThrow(/is not accessible/);
  });
});

test("profile: missing profile file is rejected", async () => {
  await withConfigRoot({ profiles: {}, opencode: {} }, async (root) => {
    await expect(loadProfile(root, "default", BASE_ENV)).rejects.toThrow(ProfileError);
  });
});

test("profile: profile name traversal and separators are rejected", () => {
  for (const bad of ["", "..", "a/b", "a\\b", "../x", "./x", ".hidden", "a..b", "x/y", "a\0b"]) {
    expect(() => validateProfileName(bad)).toThrow(ProfileError);
  }
  expect(validateProfileName("default")).toBe("default");
  expect(validateProfileName("agent-base-2")).toBe("agent-base-2");
});

test("profile: malformed JSON is rejected", async () => {
  await withConfigRoot({ profiles: { broken: "{ not json" } }, async (root) => {
    await expect(loadProfile(root, "broken", {})).rejects.toThrow(/not valid JSON/);
  });
});

test("profile: unknown and missing fields are rejected", async () => {
  expect(() => parseProfileSpec(profileBody({ extra: 1 }))).toThrow(/unknown field "extra"/);
  expect(() => parseProfileSpec(JSON.stringify({ schema_version: 1, image: "x", env: {} }))).toThrow(
    /missing required field "opencode_config"/,
  );
  expect(() => parseProfileSpec('{"schema_version":2,"image":"x","opencode_config":"a","env":{}}')).toThrow(
    /schema_version 2, expected 1/,
  );
  expect(() => parseProfileSpec('{"schema_version":1,"env":{}}')).toThrow(/missing required field/);
  expect(() => parseProfileSpec('{"schema_version":1,"image":"x","opencode_config":"a"}')).toThrow(/env/);
});

test("profile: schema_version, image and opencode_config validation", () => {
  expect(() => parseProfileSpec(profileBody({ schema_version: "1" }))).toThrow(/schema_version/);
  expect(() => parseProfileSpec(profileBody({ image: "" }))).toThrow(/image/);
  expect(() => parseProfileSpec(profileBody({ image: 5 }))).toThrow(/image/);
  expect(() => parseProfileSpec(profileBody({ opencode_config: "" }))).toThrow(/opencode_config/);
  expect(() => parseProfileSpec(profileBody({ opencode_config: "/etc/external.json" }))).toThrow(
    /relative to the configuration root/,
  );
});

test("profile: env binding shape is strict", () => {
  expect(() =>
    parseProfileSpec(profileBody({ env: { LLM_SERVER: { from_env: "LLM_SERVER" } } })),
  ).toThrow(/missing required field "required"/);
  expect(() =>
    parseProfileSpec(profileBody({ env: { LLM_SERVER: { from_env: "LLM_SERVER", required: true, prefix: "LLM" } } })),
  ).toThrow(/unknown field "prefix"/);
  expect(() => parseProfileSpec(profileBody({ env: { "BAD-NAME": { from_env: "A", required: false } } }))).toThrow(
    /not a valid environment variable name/,
  );
  expect(() => parseProfileSpec(profileBody({ env: { OK: { from_env: "1BAD", required: false } } }))).toThrow(
    /not a valid environment variable name/,
  );
  expect(() => parseProfileSpec(profileBody({ env: { OK: { from_env: "OK", required: "yes" } } }))).toThrow(
    /required must be a boolean/,
  );
  expect(() => parseProfileSpec(profileBody({ env: { OK: ["LLM_SERVER", true] } }))).toThrow(
    /is not a JSON object/,
  );
  expect(() => parseProfileSpec(profileBody({ env: { OK: { from_env: "", required: true } } }))).toThrow(
    /non-empty string/,
  );
});

test("profile: control variables cannot be destinations or sources", () => {
  for (const name of [
    "OPENCODE_CONFIG_CONTENT",
    "DOCKER_HELPER_SESSION_TOKEN",
    "DOCKER_HELPER_CREDENTIAL_TOKEN",
    "DOCKER_HELPER_ADMIN_TOKEN",
    "DOCKER_HELPER_STATE_PATH",
    "AGENT_SMOKE_RUN_ID",
    "ORCHESTRATOR_FUTURE",
  ]) {
    expect(isControlEnvName(name)).toBe(true);
    expect(() => parseProfileSpec(profileBody({ env: { [name]: { from_env: "LLM_SERVER", required: false } } }))).toThrow(
      /orchestrator-owned control variable/,
    );
    expect(() =>
      parseProfileSpec(profileBody({ env: { LLM_SERVER: { from_env: name, required: false } } })),
    ).toThrow(/cannot be used as a source/);
  }
});

test("profile: opencode_config escape attempts are rejected", async () => {
  const outsideRoot = await mkdtemp(join(tmpdir(), "profile-outside-"));
  try {
    const secretFile = join(outsideRoot, "secret.json");
    await writeFile(secretFile, OPENCODE_CONFIG);
    const escapeRelative = `../../${outsideRoot.replace(/^\/+/, "")}/secret.json`;
    await withConfigRoot(
      { profiles: { escape: profileBody({ opencode_config: escapeRelative }) } },
      async (root) => {
        await expect(loadProfile(root, "escape", {})).rejects.toThrow(
          /resolves outside the configuration root/,
        );
      },
    );
  } finally {
    await rm(outsideRoot, { recursive: true, force: true });
  }
  expect(() => parseProfileSpec(profileBody({ opencode_config: "/etc/passwd" }))).toThrow(/relative/);
});

test("profile: symlink escape of the profile file is rejected", async () => {
  await withConfigRoot({ profiles: { linked: profileBody() }, opencode: { "default.json": OPENCODE_CONFIG } }, async (root) => {
    const rootParent = await mkdtemp(join(tmpdir(), "profile-outside-"));
    try {
      const outsideFile = join(rootParent, "outside-profile.json");
      await writeFile(outsideFile, profileBody());
      await rm(join(root, "profiles", "linked.json"), { force: true });
      await symlink(outsideFile, join(root, "profiles", "linked.json"));
      await expect(loadProfile(root, "linked", {})).rejects.toThrow(
        /resolves outside the configuration root/,
      );
    } finally {
      await rm(rootParent, { recursive: true, force: true });
    }
  });
});

test("profile: symlink escape of the opencode config is rejected", async () => {
  await withConfigRoot({ profiles: { default: profileBody() } }, async (root) => {
    const rootParent = await mkdtemp(join(tmpdir(), "profile-outside-"));
    try {
      const outsideFile = join(rootParent, "opencode.json");
      await writeFile(outsideFile, OPENCODE_CONFIG);
      await symlink(outsideFile, join(root, "opencode", "default.json"));
      await expect(loadProfile(root, "default", {})).rejects.toThrow(
        /resolves outside the configuration root/,
      );
    } finally {
      await rm(rootParent, { recursive: true, force: true });
    }
  });
});

test("profile: config root must exist", async () => {
  await expect(loadProfile("/nonexistent/profile-root-xyz", "default", {})).rejects.toThrow(
    /cannot be canonicalized/,
  );
});

test("profile: missing required source env is an error; missing optional is skipped", () => {
  expect(() => resolveEnvBindings({ LLM_KEY: { from_env: "LLM_KEY", required: true } }, {})).toThrow(
    /profile requires environment variable LLM_KEY/,
  );
  expect(resolveEnvBindings({ EXTRA: { from_env: "EXTRA", required: false } }, {})).toEqual({});
  expect(resolveEnvBindings({ EXTRA: { from_env: "EXTRA", required: false } }, { EXTRA: "v" })).toEqual({ EXTRA: "v" });
});

test("profile: errors contain names and paths but never secret values", async () => {
  await withConfigRoot({ profiles: { default: profileBody() }, opencode: { "default.json": OPENCODE_CONFIG } }, async (root) => {
    try {
      await loadProfile(root, "default", {});
      throw new Error("expected loadProfile to fail");
    } catch (cause) {
      expect(cause instanceof ProfileError).toBe(true);
      const message = (cause as Error).message;
      expect(message).toContain("LLM_SERVER");
      expect(message).not.toContain("sk-supersecret-value-42");
      expect(message).not.toContain("https://llm.example/v1");
    }
  });
});

test("profile: launcher/admin/state variables are never usable as sources", () => {
  const env = {
    LLM_KEY: "sk-supersecret-value-42",
    DOCKER_HELPER_CREDENTIAL_TOKEN: "dhc_launcher_token",
    DOCKER_HELPER_ADMIN_TOKEN: "dha_admin_secret",
    DOCKER_HELPER_STATE_PATH: "/host/orchestrator-state",
  };
  const resolved = resolveEnvBindings(
    { LLM_KEY: { from_env: "LLM_KEY", required: true } },
    env,
  );
  expect(resolved).toEqual({ LLM_KEY: "sk-supersecret-value-42" });
  expect(Object.keys(resolved)).not.toContain("DOCKER_HELPER_CREDENTIAL_TOKEN");
  expect(Object.keys(resolved)).not.toContain("DOCKER_HELPER_ADMIN_TOKEN");
  expect(Object.keys(resolved)).not.toContain("DOCKER_HELPER_STATE_PATH");
});

test("profile: empty required source value is treated as missing", () => {
  expect(() => resolveEnvBindings({ LLM_KEY: { from_env: "LLM_KEY", required: true } }, { LLM_KEY: "" })).toThrow(
    /profile requires environment variable LLM_KEY/,
  );
});

test("profile: parse rejects non-object payloads", () => {
  expect(() => parseProfileSpec("[]")).toThrow(/not a JSON object/);
  expect(() => parseProfileSpec("null")).toThrow(/not a JSON object/);
  expect(() => parseProfileSpec('"str"')).toThrow(/not a JSON object/);
});
