import { expect, test } from "bun:test";
import {
  createChildSession,
  type ChildSessionFilesystemEntry,
} from "../src/launcher.ts";
import type { CliRunner, CliStdio } from "../src/docker_helper.ts";

const SOCKET = "/run/docker-helper/docker-helper.sock";
const CREDENTIAL_FILE = "/home/op/.config/docker-helper/credential.token";
const CONFIG = { socketPath: SOCKET, credentialFile: CREDENTIAL_FILE };
const OPERATOR_ENV = { HOME: "/home/op", XDG_CONFIG_HOME: "/cfg" };
const WORKSPACE = "/work/run";

interface RecordedCall {
  args: string[];
  env: Record<string, string>;
  stdio: CliStdio;
}

function makeFakeCli(): { cli: CliRunner; calls: RecordedCall[] } {
  const calls: RecordedCall[] = [];
  const cli: CliRunner = async (args, env, stdio) => {
    calls.push({ args: [...args], env: { ...env }, stdio });
    return {
      code: 0,
      stdout: JSON.stringify({
        ok: true,
        session: { id: "dhs_1", launcher_id: "dhl_launcher" },
        token: "dhc_1",
      }),
    };
  };
  return { cli, calls };
}

function entry(path: string, access: string): ChildSessionFilesystemEntry {
  return { path, access } as ChildSessionFilesystemEntry;
}

test("1. a v1 call without entries keeps the exact previous argv", async () => {
  const { cli, calls } = makeFakeCli();
  const session = await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV });
  expect(session.sessionId).toBe("dhs_1");
  expect(session.token).toBe("dhc_1");
  expect(calls.length).toBe(1);
  expect(calls[0]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--workspace",
    WORKSPACE,
  ]);
  expect(calls[0]!.env).toEqual(OPERATOR_ENV);
  expect(calls[0]!.stdio).toBe("capture");
});

test("1a. an empty entries list also keeps the exact previous argv", async () => {
  const { cli, calls } = makeFakeCli();
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemEntries: [],
  });
  expect(calls[0]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--workspace",
    WORKSPACE,
  ]);
});

test("2. entries produce exact repeatable flags in the given order after --workspace", async () => {
  const { cli, calls } = makeFakeCli();
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemEntries: [
      entry("project", "read_write"),
      entry(".", "read_only"),
      entry("activations/1-coder/data/inputs", "read_only"),
      entry("activations/1-coder/data/outputs", "read_write"),
    ],
  });
  expect(calls[0]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--workspace",
    WORKSPACE,
    "--filesystem-entry",
    "project=read_write",
    "--filesystem-entry",
    ".=read_only",
    "--filesystem-entry",
    "activations/1-coder/data/inputs=read_only",
    "--filesystem-entry",
    "activations/1-coder/data/outputs=read_write",
  ]);
});

test("3. mutating the caller list and entry objects after the call cannot change the argv", async () => {
  const { cli, calls } = makeFakeCli();
  const list: ChildSessionFilesystemEntry[] = [
    entry(".", "read_write"),
    entry("project", "read_write"),
  ];
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemEntries: list,
  });
  const recorded = calls[0]!.args;
  list.length = 0;
  list.push(entry(".", "read_only"), entry("evil", "read_write"));
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemEntries: list,
  });
  // the first call kept its captured argv
  expect(calls[0]!.args).toEqual(recorded);
  expect(calls[0]!.args).toContain("project=read_write");
  expect(calls[0]!.args).not.toContain("evil=read_write");
  // the second call reflects the caller's own mutation (caller-owned, never frozen)
  expect(calls[1]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--workspace",
    WORKSPACE,
    "--filesystem-entry",
    ".=read_only",
    "--filesystem-entry",
    "evil=read_write",
  ]);
  expect(Object.isFrozen(list)).toBe(false);
});

test("4. an invalid access value is rejected before the CLI", async () => {
  const { cli, calls } = makeFakeCli();
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [entry(".", "writeable" as "read_write")],
    }),
  ).toThrow(/access for path "\." must be exactly "read_only" or "read_write"/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [entry("project", "READ_WRITE" as "read_write")],
    }),
  ).toThrow(/must be exactly "read_only" or "read_write"/);
  expect(calls.length).toBe(0);
});

test("5. absolute, unclean and .. paths are rejected before the CLI", async () => {
  const { cli, calls } = makeFakeCli();
  const badPaths = [
    "/abs/path",
    "a//b",
    "a/",
    "/",
    "./a",
    "a/./b",
    "a/../b",
    "..",
    "../x",
    "a/..",
  ];
  for (const path of badPaths) {
    expect(() =>
      createChildSession(cli, CONFIG, WORKSPACE, {}, {
        filesystemEntries: [entry(".", "read_only"), entry(path, "read_write")],
      }),
    ).toThrow(/workspace-relative/);
  }
  // an empty path is its own shape violation
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [entry(".", "read_only"), entry("", "read_write")],
    }),
  ).toThrow(/path must be a non-empty string/);
  expect(calls.length).toBe(0);
});

test("6. duplicate entry paths are rejected before the CLI", async () => {
  const { cli, calls } = makeFakeCli();
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [
        entry(".", "read_only"),
        entry("project", "read_write"),
        entry("project", "read_only"),
      ],
    }),
  ).toThrow(/duplicate filesystem entry path "project"/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [
        entry(".", "read_only"),
        entry(".", "read_write"),
      ],
    }),
  ).toThrow(/duplicate filesystem entry path "\."/);
  expect(calls.length).toBe(0);
});

test("7. entries without the workspace root and non-list entries are rejected before the CLI", async () => {
  const { cli, calls } = makeFakeCli();
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [entry("project", "read_write")],
    }),
  ).toThrow(/must include the workspace root "\." exactly once/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [entry("project", "read_write"), entry("other", "read_write")],
    }),
  ).toThrow(/must include the workspace root "\." exactly once/);
  // a root-only list is valid and needs no additional entries
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemEntries: [entry(".", "read_write")],
  });
  expect(calls[0]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--workspace",
    WORKSPACE,
    "--filesystem-entry",
    ".=read_write",
  ]);
  // non-list and non-object shapes are rejected too
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: ["." as unknown as ChildSessionFilesystemEntry],
    }),
  ).toThrow(/filesystem entry must be an object/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [entry(".", "read_only"), null as unknown as ChildSessionFilesystemEntry],
    }),
  ).toThrow(/filesystem entry must be an object/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemEntries: [entry(".", "read_only"), {} as ChildSessionFilesystemEntry],
    }),
  ).toThrow(/path must be a non-empty string/);
  expect(calls.length).toBe(1);
});

test("8. the CLI sees the caller env only; entry values never enter env", async () => {
  const { cli, calls } = makeFakeCli();
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemEntries: [
      entry(".", "read_only"),
      entry("secret-region", "read_write"),
    ],
  });
  expect(calls[0]!.env).toEqual(OPERATOR_ENV);
  expect(Object.values(calls[0]!.env)).not.toContain("secret-region");
  expect(Object.values(calls[0]!.env)).not.toContain(".=read_only");
});
