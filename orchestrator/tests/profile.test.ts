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
import {
  profileEpochConfigDigest,
  profileEpochEnvValueDigest,
} from "../src/pipeline_v2_profile_epoch.ts";

const OPENCODE_CONFIG = '{"$schema":"https://opencode.ai/config.json","model":"test/model"}';
const DIGEST = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
const IMAGE = `ghcr.io/kivarun/opencode-docker/base@sha256:${DIGEST}`;

function profileYaml(
  image = IMAGE,
  opencodeConfig = "  path: opencode/default.json\n  secrets: env_only",
  env = `  LLM_SERVER:
    from_env: LLM_SERVER
    required: true
    secret: false
  LLM_KEY:
    from_env: LLM_KEY
    required: true
    secret: true
  OPENCODE_ENABLE_EXA:
    from_env: OPENCODE_ENABLE_EXA
    required: false
    secret: false`,
): string {
  return [
    "schema_version: 2",
    `image: ${image}`,
    "opencode_config:",
    opencodeConfig,
    "env:",
    env,
    "",
  ].join("\n");
}

async function withConfigRoot(
  files: { profiles?: Record<string, string>; opencode?: Record<string, string>; extra?: Record<string, string> },
  fn: (root: string) => Promise<void>,
): Promise<void> {
  const root = await mkdtemp(join(tmpdir(), "profile-test-"));
  try {
    await mkdir(join(root, "profiles"), { recursive: true });
    await mkdir(join(root, "opencode"), { recursive: true });
    for (const [name, content] of Object.entries(files.profiles ?? {})) {
      await writeFile(join(root, "profiles", `${name}.yaml`), content);
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

const BASE_ENV = {
  LLM_SERVER: "https://llm.example/v1",
  LLM_KEY: "sk-supersecret-value-42",
  OPENCODE_ENABLE_EXA: "1",
};

test("profile: valid named profile loads with resolved bindings", async () => {
  await withConfigRoot(
    { profiles: { default: profileYaml() }, opencode: { "default.json": OPENCODE_CONFIG } },
    async (root) => {
      const profile = await loadProfile(root, "default", BASE_ENV);
      expect(profile.profileName).toBe("default");
      expect(profile.image).toBe(IMAGE);
      expect(profile.opencodeConfigContent).toBe(OPENCODE_CONFIG);
      expect(profile.env).toEqual({
        LLM_SERVER: "https://llm.example/v1",
        LLM_KEY: "sk-supersecret-value-42",
        OPENCODE_ENABLE_EXA: "1",
      });
      // the absolute canonical config path never enters the resolved object
      const serialized = JSON.stringify(profile);
      expect(serialized).not.toContain(root);
    },
  );
});

test("profile: schema v1 is rejected without migration", async () => {
  const v1 = profileYaml().replace("schema_version: 2", "schema_version: 1");
  await withConfigRoot(
    { profiles: { legacy: v1 }, opencode: { "default.json": OPENCODE_CONFIG } },
    async (root) => {
      await expect(loadProfile(root, "legacy", BASE_ENV)).rejects.toThrow(
        /schema_version 1, expected 2 \(no schema v1 migration exists\)/,
      );
    },
  );
});

test("profile: opencode_config must be the exact two-key mapping with secrets: env_only", () => {
  expect(() => parseProfileSpec(profileYaml().replace(/opencode_config:[\s\S]*?env:/, "opencode_config: opencode/default.json\nenv:"))).toThrow(
    /opencode_config/,
  );
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: embedded_ok"),
    ),
  ).toThrow(/is not the supported config policy/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only\n  extra: 1"),
    ),
  ).toThrow(/unknown field "extra"/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  secrets: env_only"),
    ),
  ).toThrow(/missing required field "path"/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: /etc/external.json\n  secrets: env_only"),
    ),
  ).toThrow(/relative to the configuration root/);
});

test("profile: every env binding requires the exact secret boolean", () => {
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  LLM_SERVER:
    from_env: LLM_SERVER
    required: true`),
    ),
  ).toThrow(/missing required field "secret"/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  LLM_SERVER:
    from_env: LLM_SERVER
    required: true
    secret: "no"`),
    ),
  ).toThrow(/secret must be a boolean/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  LLM_SERVER:
    from_env: LLM_SERVER
    required: true
    secret: false
    prefix: X`),
    ),
  ).toThrow(/unknown field "prefix"/);
});

test("profile: digest-pinned image references are accepted and preserved verbatim", async () => {
  await withConfigRoot(
    {
      profiles: { pinned: profileYaml(`registry.example:5000/team/opencode:fixed@sha256:${DIGEST}`) },
      opencode: { "default.json": OPENCODE_CONFIG },
    },
    async (root) => {
      const profile = await loadProfile(root, "pinned", BASE_ENV);
      expect(profile.image).toBe(`registry.example:5000/team/opencode:fixed@sha256:${DIGEST}`);
      expect(profile.identity.image_ref).toBe(profile.image);
    },
  );
});

test("profile: tag-only, bare-id and malformed image references are rejected", () => {
  for (const bad of [
    "ghcr.io/kivarun/opencode-docker/base:latest",
    "alpine:3.22",
    "alpine",
    `ghcr.io/example/worker@sha256:${"A".repeat(64)}`,
    `ghcr.io/example/worker@sha256:${"a".repeat(63)}`,
    `ghcr.io/example/worker@sha256:${"a".repeat(65)}`,
    `ghcr.io/example/worker@sha256:${"g".repeat(64)}`,
  ]) {
    expect(() => parseProfileSpec(profileYaml(bad))).toThrow(/digest-pinned image reference/);
  }
  // whitespace variants fail YAML parsing first — still hard rejections
  expect(() => parseProfileSpec(profileYaml(`ghcr.io/example/wo rker@sha256:${DIGEST}`))).toThrow(
    ProfileError,
  );
  expect(() => parseProfileSpec(profileYaml(`ghcr.io/example/worker @sha256:${DIGEST}`))).toThrow(
    ProfileError,
  );
  expect(() => parseProfileSpec(profileYaml(""))).toThrow(/image/);
  // a bare digest without a repository fails YAML shape or the grammar
  expect(() => parseProfileSpec(profileYaml(`"sha256:${DIGEST}"`))).toThrow(ProfileError);
  expect(() => parseProfileSpec(profileYaml(`"@sha256:${DIGEST}"`))).toThrow(/digest-pinned image reference/);
  // a trailing space inside a quoted scalar is rejected by the grammar
  expect(() =>
    parseProfileSpec(profileYaml(`"ghcr.io/example/worker@sha256:${DIGEST} "`)),
  ).toThrow(/digest-pinned image reference/);
});

test("profile: empty env bindings are allowed", async () => {
  await withConfigRoot(
    {
      profiles: {
        minimal: [
          "schema_version: 2",
          `image: alpine@sha256:${DIGEST}`,
          "opencode_config:",
          "  path: opencode/default.json",
          "  secrets: env_only",
          "env: {}",
          "",
        ].join("\n"),
      },
      opencode: { "default.json": OPENCODE_CONFIG },
    },
    async (root) => {
      const profile = await loadProfile(root, "minimal", {});
      expect(profile.env).toEqual({});
      expect(profile.image).toBe(`alpine@sha256:${DIGEST}`);
      expect(profile.identity.env_bindings).toEqual([]);
    },
  );
});

test("profile: unknown profile is rejected", async () => {
  await withConfigRoot({ profiles: { default: profileYaml() }, opencode: { "default.json": OPENCODE_CONFIG } }, async (root) => {
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

test("profile: malformed YAML is rejected", async () => {
  await withConfigRoot({ profiles: { broken: "schema_version: [unclosed" } }, async (root) => {
    await expect(loadProfile(root, "broken", {})).rejects.toThrow(/not valid YAML/);
  });
});

test("profile: unknown and missing fields are rejected", () => {
  expect(() => parseProfileSpec(`${profileYaml()}extra: 1\n`)).toThrow(/unknown field "extra"/);
  expect(() => parseProfileSpec("schema_version: 2\nimage: x\nenv: {}\n")).toThrow(
    /missing required field "opencode_config"/,
  );
  expect(() => parseProfileSpec("schema_version: 2\nenv: {}\n")).toThrow(/missing required field/);
  expect(() => parseProfileSpec("schema_version: 2\nimage: x\n")).toThrow(
    /missing required field "opencode_config"/,
  );
});

test("profile: schema_version and image validation", () => {
  expect(() => parseProfileSpec(profileYaml().replace("schema_version: 2", 'schema_version: "2"'))).toThrow(
    /schema_version/,
  );
  expect(() => parseProfileSpec(profileYaml().replace(/image: .*/, 'image: ""'))).toThrow(/image/);
  expect(() => parseProfileSpec(profileYaml().replace(/image: .*\n/, ""))).toThrow(
    /missing required field "image"/,
  );
});

test("profile: env binding shape is strict", () => {
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  LLM_SERVER:
    from_env: LLM_SERVER
    required: true
    secret: false
    extra: 1`),
    ),
  ).toThrow(/unknown field "extra"/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  "BAD-NAME":
    from_env: A
    required: false
    secret: false`),
    ),
  ).toThrow(/not a valid environment variable name/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  OK:
    from_env: 1BAD
    required: false
    secret: false`),
    ),
  ).toThrow(/not a valid environment variable name/);
  expect(() =>
    parseProfileSpec(
      profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  OK:
    from_env: ""
    required: true
    secret: false`),
    ),
  ).toThrow(/non-empty string/);
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
    "HOME",
    "XDG_CONFIG_HOME",
    "XDG_STATE_HOME",
    "XDG_RUNTIME_DIR",
  ]) {
    expect(isControlEnvName(name)).toBe(true);
    expect(() =>
      parseProfileSpec(
        profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  ${name}:
    from_env: LLM_SERVER
    required: false
    secret: false`),
      ),
    ).toThrow(/orchestrator-owned control or operator-path variable/);
    expect(() =>
      parseProfileSpec(
        profileYaml(IMAGE, "  path: opencode/default.json\n  secrets: env_only", `  LLM_SERVER:
    from_env: ${name}
    required: false
    secret: false`),
      ),
    ).toThrow(/cannot be used as a source/);
  }
});

test("profile: operator path variables stay usable by the orchestrator but not as bindings", () => {
  // the orchestrator's own minimal CLI env still resolves these names
  const env = {
    HOME: "/home/opencode",
    XDG_CONFIG_HOME: "/uat-cred",
    XDG_STATE_HOME: "/xdg-state",
    XDG_RUNTIME_DIR: "/run/user/1000",
  };
  const resolved = resolveEnvBindings(
    { LLM_KEY: { from_env: "LLM_KEY", required: true, secret: true } },
    { ...env, LLM_KEY: "sk-supersecret-value-42" },
  );
  expect(resolved).toEqual({ LLM_KEY: "sk-supersecret-value-42" });
  for (const name of Object.keys(env)) {
    expect(isControlEnvName(name)).toBe(true);
  }
});

test("profile: opencode_config escape attempts are rejected", async () => {
  const outsideRoot = await mkdtemp(join(tmpdir(), "profile-outside-"));
  try {
    const secretFile = join(outsideRoot, "secret.jsonc");
    await writeFile(secretFile, OPENCODE_CONFIG);
    const escapeRelative = `../../${outsideRoot.replace(/^\/+/, "")}/secret.jsonc`;
    await withConfigRoot(
      {
        profiles: {
          escape: profileYaml(IMAGE, `  path: ${escapeRelative}\n  secrets: env_only`),
        },
      },
      async (root) => {
        await expect(loadProfile(root, "escape", {})).rejects.toThrow(
          /resolves outside the configuration root/,
        );
      },
    );
  } finally {
    await rm(outsideRoot, { recursive: true, force: true });
  }
  expect(() => parseProfileSpec(profileYaml(IMAGE, "  path: /etc/passwd\n  secrets: env_only"))).toThrow(/relative/);
});

test("profile: symlink escape of the profile file is rejected", async () => {
  await withConfigRoot({ profiles: { linked: profileYaml() }, opencode: { "default.json": OPENCODE_CONFIG } }, async (root) => {
    const rootParent = await mkdtemp(join(tmpdir(), "profile-outside-"));
    try {
      const outsideFile = join(rootParent, "outside-profile.yaml");
      await writeFile(outsideFile, profileYaml());
      await rm(join(root, "profiles", "linked.yaml"), { force: true });
      await symlink(outsideFile, join(root, "profiles", "linked.yaml"));
      await expect(loadProfile(root, "linked", {})).rejects.toThrow(
        /resolves outside the configuration root/,
      );
    } finally {
      await rm(rootParent, { recursive: true, force: true });
    }
  });
});

test("profile: symlink escape of the opencode config is rejected", async () => {
  await withConfigRoot({ profiles: { default: profileYaml() } }, async (root) => {
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

test("profile: only .yaml profiles are supported", async () => {
  await withConfigRoot({}, async (root) => {
    await writeFile(
      join(root, "profiles", "default.json"),
      JSON.stringify({ schema_version: 2, image: "x", opencode_config: "a", env: {} }),
    );
    await writeFile(join(root, "profiles", "legacy.yml"), profileYaml());
    await expect(loadProfile(root, "default", {})).rejects.toThrow(/is not accessible/);
    await expect(loadProfile(root, "legacy", {})).rejects.toThrow(/is not accessible/);
  });
});

test("profile: config root must exist", async () => {
  await expect(loadProfile("/nonexistent/profile-root-xyz", "default", {})).rejects.toThrow(
    /cannot be canonicalized/,
  );
});

test("profile: missing required source env is an error; missing optional is skipped", () => {
  expect(() =>
    resolveEnvBindings({ LLM_KEY: { from_env: "LLM_KEY", required: true, secret: true } }, {}),
  ).toThrow(/profile requires environment variable LLM_KEY/);
  expect(resolveEnvBindings({ EXTRA: { from_env: "EXTRA", required: false, secret: false } }, {})).toEqual({});
  expect(resolveEnvBindings({ EXTRA: { from_env: "EXTRA", required: false, secret: false } }, { EXTRA: "v" })).toEqual({
    EXTRA: "v",
  });
});

test("profile: multiple declared bindings all resolve", async () => {
  await withConfigRoot(
    {
      profiles: {
        multi: profileYaml(
          `ghcr.io/example/multi@sha256:${DIGEST}`,
          "  path: opencode/default.json\n  secrets: env_only",
          `  FIRST:
    from_env: FIRST_SRC
    required: true
    secret: false
  SECOND:
    from_env: SECOND_SRC
    required: false
    secret: false
  THIRD:
    from_env: THIRD
    required: true
    secret: true`,
        ),
      },
      opencode: { "default.json": OPENCODE_CONFIG },
    },
    async (root) => {
      const profile = await loadProfile(root, "multi", { FIRST_SRC: "f", THIRD: "t" });
      expect(profile.env).toEqual({ FIRST: "f", THIRD: "t" });
      expect(Object.keys(profile.env)).not.toContain("SECOND");
    },
  );
});

test("profile: the identity projection binds declarations and non-secret value digests only", async () => {
  await withConfigRoot(
    { profiles: { default: profileYaml() }, opencode: { "default.json": OPENCODE_CONFIG } },
    async (root) => {
      const profile = await loadProfile(root, "default", BASE_ENV);
      expect(profile.identity).toEqual({
        id: "default",
        image_ref: IMAGE,
        config_policy: "env_only",
        config_sha256: profileEpochConfigDigest(OPENCODE_CONFIG),
        env_bindings: [
          {
            destination: "LLM_KEY",
            from_env: "LLM_KEY",
            required: true,
            secret: true,
          },
          {
            destination: "LLM_SERVER",
            from_env: "LLM_SERVER",
            required: true,
            secret: false,
            value_sha256: profileEpochEnvValueDigest("https://llm.example/v1"),
          },
          {
            destination: "OPENCODE_ENABLE_EXA",
            from_env: "OPENCODE_ENABLE_EXA",
            required: false,
            secret: false,
            value_sha256: profileEpochEnvValueDigest("1"),
          },
        ],
      });
      // the identity projection is content-free: no host path, no secret
      // value, digest, presence or length marker
      const serialized = JSON.stringify(profile.identity);
      expect(serialized).not.toContain(root);
      expect(serialized).not.toContain("sk-supersecret-value-42");
      expect(serialized).not.toContain("https://llm.example/v1");
    },
  );
});

test("profile: an optional non-secret binding that did not resolve carries no value digest", async () => {
  await withConfigRoot(
    { profiles: { default: profileYaml() }, opencode: { "default.json": OPENCODE_CONFIG } },
    async (root) => {
      const profile = await loadProfile(root, "default", { LLM_SERVER: "https://llm.example/v1", LLM_KEY: "k" });
      const exa = profile.identity.env_bindings.find((binding) => binding.destination === "OPENCODE_ENABLE_EXA");
      expect(exa !== undefined && "value_sha256" in exa).toBe(false);
      const server = profile.identity.env_bindings.find((binding) => binding.destination === "LLM_SERVER");
      expect(server !== undefined && typeof server.value_sha256 === "string").toBe(true);
    },
  );
});

test("profile: the resolved profile is deep-frozen and mutation-proof", async () => {
  await withConfigRoot(
    { profiles: { default: profileYaml() }, opencode: { "default.json": OPENCODE_CONFIG } },
    async (root) => {
      const profile = await loadProfile(root, "default", BASE_ENV);
      expect(Object.isFrozen(profile)).toBe(true);
      expect(Object.isFrozen(profile.identity)).toBe(true);
      expect(Object.isFrozen(profile.env)).toBe(true);
      expect(() => {
        (profile as { image: string }).image = "changed";
      }).toThrow();
      expect(profile.identity.config_sha256).toBe(profileEpochConfigDigest(OPENCODE_CONFIG));
    },
  );
});

test("profile: errors contain names and paths but never secret values", async () => {
  await withConfigRoot({ profiles: { default: profileYaml() }, opencode: { "default.json": OPENCODE_CONFIG } }, async (root) => {
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
    { LLM_KEY: { from_env: "LLM_KEY", required: true, secret: true } },
    env,
  );
  expect(resolved).toEqual({ LLM_KEY: "sk-supersecret-value-42" });
  expect(Object.keys(resolved)).not.toContain("DOCKER_HELPER_CREDENTIAL_TOKEN");
  expect(Object.keys(resolved)).not.toContain("DOCKER_HELPER_ADMIN_TOKEN");
  expect(Object.keys(resolved)).not.toContain("DOCKER_HELPER_STATE_PATH");
});

test("profile: empty required source value is treated as missing", () => {
  expect(() =>
    resolveEnvBindings({ LLM_KEY: { from_env: "LLM_KEY", required: true, secret: true } }, { LLM_KEY: "" }),
  ).toThrow(/profile requires environment variable LLM_KEY/);
});

test("profile: parse rejects non-mapping payloads", () => {
  expect(() => parseProfileSpec("- a\n- b\n")).toThrow(/not a YAML mapping/);
  expect(() => parseProfileSpec("null")).toThrow(/not a YAML mapping/);
  expect(() => parseProfileSpec('"str"')).toThrow(/not a YAML mapping/);
});
