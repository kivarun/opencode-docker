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

function profileYaml(
  image = "ghcr.io/kivarun/opencode-docker/base:latest",
  opencodeConfig = "opencode/default.json",
  env = `  LLM_SERVER:
    from_env: LLM_SERVER
    required: true
  LLM_KEY:
    from_env: LLM_KEY
    required: true
  OPENCODE_ENABLE_EXA:
    from_env: OPENCODE_ENABLE_EXA
    required: false`,
): string {
  return [
    "schema_version: 1",
    `image: ${image}`,
    `opencode_config: ${opencodeConfig}`,
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

test("profile: image comes from the profile, not the CLI", async () => {
  await withConfigRoot(
    {
      profiles: { pinned: profileYaml("registry.example/team/opencode:fixed-tag") },
      opencode: { "default.json": OPENCODE_CONFIG },
    },
    async (root) => {
      const profile = await loadProfile(root, "pinned", BASE_ENV);
      expect(profile.image).toBe("registry.example/team/opencode:fixed-tag");
    },
  );
});

test("profile: empty env bindings are allowed", async () => {
  await withConfigRoot(
    {
      profiles: {
        minimal: [
          "schema_version: 1",
          "image: alpine:3.22",
          "opencode_config: opencode/default.json",
          "env: {}",
          "",
        ].join("\n"),
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
  expect(() => parseProfileSpec("schema_version: 1\nimage: x\nenv: {}\n")).toThrow(
    /missing required field "opencode_config"/,
  );
  expect(() =>
    parseProfileSpec('{"schema_version":2,"image":"x","opencode_config":"a","env":{}}'),
  ).toThrow(/schema_version "2"|schema_version 2, expected 1/);
  expect(() => parseProfileSpec("schema_version: 1\nenv: {}\n")).toThrow(/missing required field/);
  expect(() => parseProfileSpec("schema_version: 1\nimage: x\n")).toThrow(
    /missing required field "opencode_config"/,
  );
});

test("profile: schema_version, image and opencode_config validation", () => {
  expect(() => parseProfileSpec(profileYaml().replace("schema_version: 1", 'schema_version: "1"'))).toThrow(
    /schema_version/,
  );
  expect(() => parseProfileSpec(profileYaml().replace(/image: .*/, 'image: ""'))).toThrow(/image/);
  expect(() => parseProfileSpec(profileYaml().replace(/image: .*\n/, ""))).toThrow(
    /missing required field "image"/,
  );
  expect(() => parseProfileSpec(profileYaml().replace(/opencode_config: .*/, 'opencode_config: ""'))).toThrow(
    /opencode_config/,
  );
  expect(() =>
    parseProfileSpec(profileYaml().replace(/opencode_config: .*/, "opencode_config: /etc/external.json")),
  ).toThrow(/relative to the configuration root/);
});

test("profile: env binding shape is strict", () => {
  expect(() =>
    parseProfileSpec(profileYaml("img:1", "a", `  LLM_SERVER:
    from_env: LLM_SERVER`)),
  ).toThrow(/missing required field "required"/);
  expect(() =>
    parseProfileSpec(
      profileYaml("img:1", "a", `  LLM_SERVER:
    from_env: LLM_SERVER
    required: true
    prefix: LLM`),
    ),
  ).toThrow(/unknown field "prefix"/);
  expect(() =>
    parseProfileSpec(
      profileYaml("img:1", "a", `  "BAD-NAME":
    from_env: A
    required: false`),
    ),
  ).toThrow(/not a valid environment variable name/);
  expect(() =>
    parseProfileSpec(
      profileYaml("img:1", "a", `  OK:
    from_env: 1BAD
    required: false`),
    ),
  ).toThrow(/not a valid environment variable name/);
  expect(() =>
    parseProfileSpec(profileYaml("img:1", "a", `  OK:
    from_env: OK
    required: "yes"`)),
  ).toThrow(/required must be a boolean/);
  expect(() =>
    parseProfileSpec(
      profileYaml("img:1", "a", `  OK:
    from_env: ""
    required: true`),
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
  ]) {
    expect(isControlEnvName(name)).toBe(true);
    expect(() =>
      parseProfileSpec(
        profileYaml("img:1", "a", `  ${name}:
    from_env: LLM_SERVER
    required: false`),
      ),
    ).toThrow(/orchestrator-owned control variable/);
    expect(() =>
      parseProfileSpec(
        profileYaml("img:1", "a", `  LLM_SERVER:
    from_env: ${name}
    required: false`),
      ),
    ).toThrow(/cannot be used as a source/);
  }
});

test("profile: opencode_config escape attempts are rejected", async () => {
  const outsideRoot = await mkdtemp(join(tmpdir(), "profile-outside-"));
  try {
    const secretFile = join(outsideRoot, "secret.jsonc");
    await writeFile(secretFile, OPENCODE_CONFIG);
    const escapeRelative = `../../${outsideRoot.replace(/^\/+/, "")}/secret.jsonc`;
    await withConfigRoot(
      { profiles: { escape: profileYaml("img:1", escapeRelative) } },
      async (root) => {
        await expect(loadProfile(root, "escape", {})).rejects.toThrow(
          /resolves outside the configuration root/,
        );
      },
    );
  } finally {
    await rm(outsideRoot, { recursive: true, force: true });
  }
  expect(() => parseProfileSpec(profileYaml("img:1", "/etc/passwd"))).toThrow(/relative/);
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
      JSON.stringify({ schema_version: 1, image: "x", opencode_config: "a", env: {} }),
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
  expect(() => resolveEnvBindings({ LLM_KEY: { from_env: "LLM_KEY", required: true } }, {})).toThrow(
    /profile requires environment variable LLM_KEY/,
  );
  expect(resolveEnvBindings({ EXTRA: { from_env: "EXTRA", required: false } }, {})).toEqual({});
  expect(resolveEnvBindings({ EXTRA: { from_env: "EXTRA", required: false } }, { EXTRA: "v" })).toEqual({ EXTRA: "v" });
});

test("profile: multiple declared bindings all resolve", async () => {
  await withConfigRoot(
    {
      profiles: {
        multi: profileYaml(
          "img:2",
          "opencode/default.json",
          `  FIRST:
    from_env: FIRST_SRC
    required: true
  SECOND:
    from_env: SECOND_SRC
    required: false
  THIRD:
    from_env: THIRD
    required: true`,
        ),
      },
      opencode: { "default.json": OPENCODE_CONFIG },
    },
    async (root) => {
      const profile = await loadProfile(
        root,
        "multi",
        { FIRST_SRC: "f", THIRD: "t" },
      );
      expect(profile.env).toEqual({ FIRST: "f", THIRD: "t" });
      expect(Object.keys(profile.env)).not.toContain("SECOND");
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

test("profile: parse rejects non-mapping payloads", () => {
  expect(() => parseProfileSpec("- a\n- b\n")).toThrow(/not a YAML mapping/);
  expect(() => parseProfileSpec("null")).toThrow(/not a YAML mapping/);
  expect(() => parseProfileSpec('"str"')).toThrow(/not a YAML mapping/);
});
