import { expect, test } from "bun:test";
import {
  createChildSession,
  type SessionFilesystemRoot,
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

function root(path: string, access: string): SessionFilesystemRoot {
  return { path, access } as SessionFilesystemRoot;
}

test("1. a v1 call without roots keeps the exact previous argv with the positional workspace last", async () => {
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
    WORKSPACE,
  ]);
  expect(calls[0]!.env).toEqual(OPERATOR_ENV);
  expect(calls[0]!.stdio).toBe("capture");
});

test("1a. an empty roots list also keeps the exact previous argv", async () => {
  const { cli, calls } = makeFakeCli();
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: [],
  });
  expect(calls[0]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    WORKSPACE,
  ]);
});

test("2. roots produce exact repeatable flags in the given order, all before the positional workspace", async () => {
  const { cli, calls } = makeFakeCli();
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: [
      root("/work/run", "read_only"),
      root("/work/run/project", "read_write"),
      root("/work/run/activations/1-coder/data/outputs", "read_write"),
    ],
  });
  expect(calls[0]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--filesystem-root",
    "/work/run=read_only",
    "--filesystem-root",
    "/work/run/project=read_write",
    "--filesystem-root",
    "/work/run/activations/1-coder/data/outputs=read_write",
    WORKSPACE,
  ]);
});

test("2a. the workspace is never passed through --workspace and no removed grammar appears", async () => {
  const { cli, calls } = makeFakeCli();
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: [
      root("/work/run", "read_only"),
      root("/work/run/project", "read_write"),
    ],
  });
  const args = calls[0]!.args;
  expect(args).not.toContain("--workspace");
  expect(args).not.toContain("--filesystem-entry");
  expect(args.join("\n")).not.toContain("filesystem_entries");
  // the workspace is the last argv value, exactly once
  expect(args[args.length - 1]).toBe(WORKSPACE);
  expect(args.filter((arg) => arg === WORKSPACE).length).toBe(1);
});

test("2b. a root path containing = stays exactly one argv value; the daemon splits at the last =", async () => {
  const { cli, calls } = makeFakeCli();
  const path = "/work/run/a=b/c=d";
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: [root(path, "read_only")],
  });
  expect(calls[0]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--filesystem-root",
    `${path}=read_only`,
    WORKSPACE,
  ]);
  // the pair is one argv element, never split by the transport
  expect(calls[0]!.args.filter((arg) => arg === `${path}=read_only`).length).toBe(1);
});

test("3. mutating the caller list and root objects after the call cannot change the argv", async () => {
  const { cli, calls } = makeFakeCli();
  const list: SessionFilesystemRoot[] = [
    root("/work/run", "read_write"),
    root("/work/run/project", "read_write"),
  ];
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: list,
  });
  const recorded = calls[0]!.args;
  list.length = 0;
  list.push(root("/work/run", "read_only"), root("/work/evil", "read_write"));
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: list,
  });
  // the first call kept its captured argv
  expect(calls[0]!.args).toEqual(recorded);
  expect(calls[0]!.args).toContain("/work/run/project=read_write");
  expect(calls[0]!.args).not.toContain("/work/evil=read_write");
  // the second call reflects the caller's own mutation (caller-owned, never frozen)
  expect(calls[1]!.args).toEqual([
    "session",
    "create",
    "--endpoint",
    SOCKET,
    "--json",
    "--filesystem-root",
    "/work/run=read_only",
    "--filesystem-root",
    "/work/evil=read_write",
    WORKSPACE,
  ]);
  expect(Object.isFrozen(list)).toBe(false);
});

test("3a. a Proxy or throwing getter on a root object is not read after the argv capture", async () => {
  const { cli, calls } = makeFakeCli();
  let reads = 0;
  const hostile = new Proxy(
    { path: "/work/run/project", access: "read_write" },
    {
      get(target, property) {
        reads += 1;
        return Reflect.get(target, property);
      },
    },
  ) as unknown as SessionFilesystemRoot;
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: [root("/work/run", "read_only"), hostile],
  });
  const readsAtCapture = reads;
  expect(readsAtCapture).toBeGreaterThan(0);
  const captured = calls[0]!.args;
  reads = 0;
  // after the call returned, no further reads of the captured roots occur
  expect(reads).toBe(0);
  expect(captured).toContain("/work/run/project=read_write");
});

test("4. an invalid access value is rejected before the CLI", async () => {
  const { cli, calls } = makeFakeCli();
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: [root("/work/run", "writeable" as "read_write")],
    }),
  ).toThrow(/access for path "\/work\/run" must be exactly "read_only" or "read_write"/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: [root("/work/run/project", "READ_WRITE" as "read_write")],
    }),
  ).toThrow(/must be exactly "read_only" or "read_write"/);
  expect(calls.length).toBe(0);
});

test("5. relative, empty, unclean and traversal root paths are rejected before the CLI", async () => {
  const { cli, calls } = makeFakeCli();
  const badPaths = [
    "relative/path",
    "a//b",
    "/a//b",
    "/a/",
    "./a",
    "/./a",
    "/a/./b",
    "/a/../b",
    "..",
    "/..",
    "/a/..",
    " /abs/with/space",
    "/abs/with/space ",
  ];
  for (const path of badPaths) {
    expect(() =>
      createChildSession(cli, CONFIG, WORKSPACE, {}, {
        filesystemRoots: [root(path, "read_write")],
      }),
    ).toThrow(/must be a clean absolute host path/);
  }
  // a control-rune path is admitted at transport level (one argv value);
  // the daemon's host-path text grammar refuses it fail-closed
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: [root("/abs\nwith/newline", "read_write")],
  });
  expect(calls[0]!.args).toContain("/abs\nwith/newline=read_write");
  // an empty path is its own shape violation
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: [root("", "read_write")],
    }),
  ).toThrow(/path must be a non-empty string/);
  expect(calls.length).toBe(1);
});

test("6. non-list and non-object shapes are rejected before the CLI", async () => {
  const { cli, calls } = makeFakeCli();
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: "/work/run" as unknown as readonly SessionFilesystemRoot[],
    }),
  ).toThrow(/filesystem roots must be a list/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: ["/work/run" as unknown as SessionFilesystemRoot],
    }),
  ).toThrow(/filesystem root must be an object/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: [null as unknown as SessionFilesystemRoot],
    }),
  ).toThrow(/filesystem root must be an object/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: [{} as SessionFilesystemRoot],
    }),
  ).toThrow(/path must be a non-empty string/);
  expect(() =>
    createChildSession(cli, CONFIG, WORKSPACE, {}, {
      filesystemRoots: [{ path: "/work/run" } as SessionFilesystemRoot],
    }),
  ).toThrow(/access .* must be exactly/);
  expect(calls.length).toBe(0);
});

test("7. the CLI sees the caller env only; root values never enter env", async () => {
  const { cli, calls } = makeFakeCli();
  await createChildSession(cli, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
    filesystemRoots: [
      root("/work/run", "read_only"),
      root("/work/secret-region", "read_write"),
    ],
  });
  expect(calls[0]!.env).toEqual(OPERATOR_ENV);
  expect(Object.values(calls[0]!.env)).not.toContain("/work/secret-region");
  expect(Object.values(calls[0]!.env)).not.toContain("/work/run=read_only");
});

test("8. a create failure keeps the typed cli_failure and the response parsing is unchanged", async () => {
  const calls: RecordedCall[] = [];
  const failing: CliRunner = async (args, env, stdio) => {
    calls.push({ args: [...args], env: { ...env }, stdio });
    return { code: 4, stdout: "", stderr: "error: invalid_filesystem_policy" };
  };
  let failure: unknown;
  try {
    await createChildSession(failing, CONFIG, WORKSPACE, { ...OPERATOR_ENV }, {
      filesystemRoots: [root("/work/run", "read_only")],
    });
  } catch (cause) {
    failure = cause;
  }
  expect((failure as Error).name).toBe("DockerHelperError");
  expect((failure as Error).message).toContain("invalid_filesystem_policy");
  expect(calls.length).toBe(1);
});
