import { execSync } from "node:child_process";
import { createServer, type Server } from "node:net";
import type { Stats } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  symlink,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  PipelineV2WaitStoreError,
  publishWaitRequestWithIo,
  realWaitStoreIo,
  type WaitStoreIo,
} from "../src/pipeline_v2_wait_store_internal.ts";
import {
  publishPipelineV2WaitRequest,
  publishPipelineV2WaitResponse,
} from "../src/pipeline_v2_wait_store.ts";
import {
  PipelineV2WaitManifestError,
  preparePipelineV2WaitRequest,
  type PreparedPipelineV2WaitRequest,
} from "../src/pipeline_v2_wait_manifest.ts";

interface Fixture {
  root: string;
  runRoot: string;
  waits: string;
}

const RUN_ID = "run-01";

async function setup(runId = RUN_ID): Promise<Fixture> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-wait-store-"));
  const runRoot = join(root, runId);
  await mkdir(runRoot, { mode: 0o700 });
  return { root, runRoot, waits: join(runRoot, "waits") };
}

async function dispose(fixture: Fixture): Promise<void> {
  await rm(fixture.root, { recursive: true, force: true });
}

function requestValue(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    schema_version: 1,
    run_id: RUN_ID,
    wait_index: 1,
    transition_count: 0,
    state_id: "stage-review",
    reason: "stage_iteration_limit_exhausted",
    actions: [{ id: "continue_stage", to: "coder" }],
    ...overrides,
  };
}

const CANARY = "CANARY_secret_value";

function requestWithCanary(): Record<string, unknown> {
  return {
    schema_version: 1,
    run_id: RUN_ID,
    wait_index: 1,
    transition_count: 0,
    state_id: "stage-review",
    reason: "stage_iteration_limit_exhausted",
    actions: [{ id: `act_${CANARY}`, to: "coder" }],
  };
}

function responseRaw(
  request: PreparedPipelineV2WaitRequest,
  actionId: string,
): string {
  return JSON.stringify({
    schema_version: 1,
    run_id: request.manifest.run_id,
    wait_index: request.manifest.wait_index,
    request_sha256: request.sha256,
    action_id: actionId,
  });
}

function expectStoreError(
  cause: unknown,
  outcome: "not_published" | "durability_unknown",
  reason: "invalid_layout" | "conflict" | "io_failure",
  messageFragment = "",
): PipelineV2WaitStoreError {
  expect(cause).toBeInstanceOf(PipelineV2WaitStoreError);
  const error = cause as PipelineV2WaitStoreError;
  expect(error.outcome).toBe(outcome);
  expect(error.reason).toBe(reason);
  if (messageFragment !== "") {
    expect(error.message).toContain(messageFragment);
  }
  return error;
}

function injectedFailure(code?: string): Error {
  return Object.assign(new Error("injected failure"), code === undefined ? {} : { code });
}

function ioFaulting(hook: keyof WaitStoreIo, failure: unknown): WaitStoreIo {
  return Object.freeze({
    ...realWaitStoreIo,
    [hook]: async () => {
      throw failure;
    },
  }) as unknown as WaitStoreIo;
}

function ioReplacing(hook: keyof WaitStoreIo, implementation: unknown): WaitStoreIo {
  return Object.freeze({
    ...realWaitStoreIo,
    [hook]: implementation,
  }) as unknown as WaitStoreIo;
}

async function waitsDirMode(path: string): Promise<number> {
  return (await lstat(path)).mode & 0o777;
}

async function fileIdentity(
  path: string,
): Promise<{ dev: number; ino: number; mtimeMs: number; mode: number }> {
  const info = await lstat(path);
  return { dev: info.dev, ino: info.ino, mtimeMs: info.mtimeMs, mode: info.mode & 0o777 };
}

async function responseFileNames(waits: string): Promise<string[]> {
  return (await readdir(waits)).filter((name) => name.endsWith(".response.json"));
}

async function tempFileNames(waits: string): Promise<string[]> {
  return (await readdir(waits)).filter((name) => name.startsWith(".wait-publish-"));
}

test("1. request happy path: exact path, canonical bytes without newline, modes 0700/0600", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const expectedPath = join(fixture.waits, "1.request.json");
    expect(published.request_path).toBe(expectedPath);
    expect(published.request.manifest.run_id).toBe(RUN_ID);
    expect(published.request.manifest.wait_index).toBe(1);
    const stored = await readFile(expectedPath, "utf8");
    expect(stored).toBe(published.request.canonical_json);
    expect(stored.endsWith("\n")).toBe(false);
    expect(await waitsDirMode(fixture.waits)).toBe(0o700);
    expect((await fileIdentity(expectedPath)).mode).toBe(0o600);
    expect((await readdir(fixture.runRoot)).sort()).toEqual(["waits"]);
  } finally {
    await dispose(fixture);
  }
});

test("2. the wait index defines the file name", async () => {
  const fixture = await setup();
  try {
    const published = await publishPipelineV2WaitRequest(
      fixture.runRoot,
      requestValue({ wait_index: 3 }),
    );
    expect(published.request_path).toBe(join(fixture.waits, "3.request.json"));
    expect(await readFile(join(fixture.waits, "3.request.json"), "utf8")).toBe(
      published.request.canonical_json,
    );
  } finally {
    await dispose(fixture);
  }
});

test("3. response happy path: the publisher loads the request itself", async () => {
  const fixture = await setup();
  try {
    const publishedRequest = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const published = await publishPipelineV2WaitResponse(
      fixture.runRoot,
      1,
      responseRaw(publishedRequest.request, "continue_stage"),
    );
    expect(published.request_path).toBe(join(fixture.waits, "1.request.json"));
    expect(published.response_path).toBe(join(fixture.waits, "1.response.json"));
    expect(published.response.action_to).toBe("coder");
    const stored = await readFile(published.response_path, "utf8");
    expect(stored).toBe(published.response.canonical_json);
    expect(stored.endsWith("\n")).toBe(false);
    expect((await fileIdentity(published.response_path)).mode).toBe(0o600);
    expect((await fileIdentity(published.request_path)).mode).toBe(0o600);
  } finally {
    await dispose(fixture);
  }
});

test("4. repeated identical request publish succeeds without touching inode, mtime or bytes", async () => {
  const fixture = await setup();
  try {
    const first = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const before = await fileIdentity(first.request_path);
    const second = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const after = await fileIdentity(first.request_path);
    expect(second.request.sha256).toBe(first.request.sha256);
    expect(after.dev).toBe(before.dev);
    expect(after.ino).toBe(before.ino);
    expect(after.mtimeMs).toBe(before.mtimeMs);
    expect(after.mode).toBe(before.mode);
    expect(await readFile(first.request_path, "utf8")).toBe(first.request.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("5. repeated identical response publish succeeds without touching inode, mtime or bytes", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const raw = responseRaw(preparePipelineV2WaitRequest(requestValue()), "continue_stage");
    const first = await publishPipelineV2WaitResponse(fixture.runRoot, 1, raw);
    const before = await fileIdentity(first.response_path);
    const second = await publishPipelineV2WaitResponse(fixture.runRoot, 1, raw);
    const after = await fileIdentity(first.response_path);
    expect(second.response.sha256).toBe(first.response.sha256);
    expect(after.dev).toBe(before.dev);
    expect(after.ino).toBe(before.ino);
    expect(after.mtimeMs).toBe(before.mtimeMs);
  } finally {
    await dispose(fixture);
  }
});

test("6. a different manifest on the same request path is a typed conflict and the file is untouched", async () => {
  const fixture = await setup();
  try {
    const first = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const before = await fileIdentity(first.request_path);
    const cause = await publishPipelineV2WaitRequest(
      fixture.runRoot,
      requestValue({ reason: "another_reason" }),
    ).catch((error) => error);
    expectStoreError(cause, "not_published", "conflict", "different canonical bytes");
    const after = await fileIdentity(first.request_path);
    expect(after.ino).toBe(before.ino);
    expect(await readFile(first.request_path, "utf8")).toBe(first.request.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("7. a different accepted response on the same response path is a typed conflict", async () => {
  const fixture = await setup();
  try {
    const value = requestValue({
      actions: [
        { id: "continue_stage", to: "coder" },
        { id: "revise_task", to: "architect" },
      ],
    });
    await publishPipelineV2WaitRequest(fixture.runRoot, value);
    const request = preparePipelineV2WaitRequest(value);
    const first = await publishPipelineV2WaitResponse(
      fixture.runRoot,
      1,
      responseRaw(request, "revise_task"),
    );
    const before = await fileIdentity(first.response_path);
    const cause = await publishPipelineV2WaitResponse(
      fixture.runRoot,
      1,
      responseRaw(request, "continue_stage"),
    ).catch((error) => error);
    expectStoreError(cause, "not_published", "conflict");
    expect((await fileIdentity(first.response_path)).ino).toBe(before.ino);
    expect(await readFile(first.response_path, "utf8")).toBe(first.response.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("8. run root binding: basename mismatch fails before anything is written", async () => {
  const fixture = await setup("run-02");
  try {
    const cause = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "invalid_layout", "run identifier");
    expect(await readdir(fixture.runRoot)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("9. invalid run roots fail closed with invalid_layout and create nothing", async () => {
  const fixture = await setup();
  try {
    const missing = join(fixture.root, "absent");
    const file = join(fixture.root, "plain-file");
    await writeFile(file, "x\n");
    const alias = join(fixture.root, "alias");
    await symlink(fixture.runRoot, alias);
    const cases: [string, string][] = [
      [missing, "does not exist"],
      [file, "exists but is a regular file"],
      [alias, "exists but is a symbolic link"],
      [`${fixture.runRoot}/.`, "not canonical"],
      ["relative/path", "absolute canonical path"],
    ];
    for (const [runRoot, fragment] of cases) {
      const cause = await publishPipelineV2WaitRequest(runRoot, requestValue()).catch(
        (error) => error,
      );
      expectStoreError(cause, "not_published", "invalid_layout", fragment);
    }
    expect(await readdir(fixture.runRoot)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("10. an invalid waits directory fails closed with invalid_layout", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.runRoot, "waits"), "x\n");
    const asFile = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(asFile, "not_published", "invalid_layout", "exists but is a regular file");
    await unlink(join(fixture.runRoot, "waits"));
    await symlink(fixture.root, join(fixture.runRoot, "waits"));
    const asLink = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(asLink, "not_published", "invalid_layout", "exists but is a symbolic link");
    await unlink(join(fixture.runRoot, "waits"));
    await mkdir(fixture.waits, { mode: 0o755 });
    const wrongMode = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(wrongMode, "not_published", "invalid_layout", "mode 0700");
    expect(await readdir(fixture.waits)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("11. the response publisher rejects a missing request file before writing", async () => {
  const fixture = await setup();
  try {
    const cause = await publishPipelineV2WaitResponse(fixture.runRoot, 1, "{}").catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "invalid_layout", "does not exist");
    expect(await responseFileNames(fixture.waits)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("12. the response publisher rejects mismatched run id and wait index before writing", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    const canonical = preparePipelineV2WaitRequest(requestValue()).canonical_json;
    const foreign = preparePipelineV2WaitRequest(
      requestValue({ run_id: "run-02" }),
    ).canonical_json;
    await writeFile(join(fixture.waits, "1.request.json"), foreign, { mode: 0o600 });
    const foreignRun = await publishPipelineV2WaitResponse(fixture.runRoot, 1, "{}").catch(
      (error) => error,
    );
    expectStoreError(foreignRun, "not_published", "conflict", "does not belong to this run root");
    await writeFile(join(fixture.waits, "1.request.json"), canonical, { mode: 0o600 });
    const wrongIndex = await publishPipelineV2WaitResponse(fixture.runRoot, 2, "{}").catch(
      (error) => error,
    );
    expectStoreError(wrongIndex, "not_published", "invalid_layout", "does not exist");
    await writeFile(
      join(fixture.waits, "2.request.json"),
      preparePipelineV2WaitRequest(requestValue({ wait_index: 3 })).canonical_json,
      { mode: 0o600 },
    );
    const mismatched = await publishPipelineV2WaitResponse(fixture.runRoot, 2, "{}").catch(
      (error) => error,
    );
    expectStoreError(mismatched, "not_published", "conflict", "another wait index");
    expect(await responseFileNames(fixture.waits)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("13. the response publisher rejects tampered, noncanonical and malformed request files", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    const canonical = preparePipelineV2WaitRequest(requestValue()).canonical_json;
    const parsed = JSON.parse(canonical) as Record<string, unknown>;
    const actions = parsed["actions"] as { id: string; to: string }[];
    const reordered = JSON.stringify({
      reason: parsed["reason"],
      run_id: parsed["run_id"],
      schema_version: parsed["schema_version"],
      transition_count: parsed["transition_count"],
      wait_index: parsed["wait_index"],
      state_id: parsed["state_id"],
      actions: actions.map((action) => ({ to: action.to, id: action.id })),
    });
    await writeFile(join(fixture.waits, "1.request.json"), reordered, { mode: 0o600 });
    const noncanonical = await publishPipelineV2WaitResponse(fixture.runRoot, 1, "{}").catch(
      (error) => error,
    );
    expectStoreError(noncanonical, "not_published", "conflict", "canonical JSON");
    await writeFile(join(fixture.waits, "1.request.json"), "{not json", { mode: 0o600 });
    const malformed = await publishPipelineV2WaitResponse(fixture.runRoot, 1, "{}").catch(
      (error) => error,
    );
    expect(malformed).toBeInstanceOf(PipelineV2WaitManifestError);
    expect((malformed as Error).message).toBe("the wait request document is not valid JSON");
    await writeFile(
      join(fixture.waits, "1.request.json"),
      preparePipelineV2WaitRequest(requestValue({ reason: "tampered_reason" })).canonical_json,
      { mode: 0o600 },
    );
    const tampered = await publishPipelineV2WaitResponse(
      fixture.runRoot,
      1,
      responseRaw(preparePipelineV2WaitRequest(requestValue()), "continue_stage"),
    ).catch((error) => error);
    expect(tampered).toBeInstanceOf(PipelineV2WaitManifestError);
    expect((tampered as Error).message).toBe("the wait response carries a different request digest");
    expect(await responseFileNames(fixture.waits)).toEqual([]);
  } finally {
    await dispose(fixture);
  }
});

test("14. the response publisher rejects a request file with the wrong mode", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    await writeFile(
      join(fixture.waits, "1.request.json"),
      preparePipelineV2WaitRequest(requestValue()).canonical_json,
      { mode: 0o600 },
    );
    await chmod(join(fixture.waits, "1.request.json"), 0o644);
    const cause = await publishPipelineV2WaitResponse(fixture.runRoot, 1, "{}").catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "conflict", "mode 0600");
  } finally {
    await dispose(fixture);
  }
});

test("15. pre-existing targets of every wrong kind are a conflict and stay untouched", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    const target = join(fixture.waits, "1.request.json");
    const kinds: [string, () => Promise<void>, () => Promise<void>][] = [
      [
        "a symbolic link",
        async () => {
          await symlink(fixture.root, target);
        },
        async () => {
          await rm(target, { force: true, recursive: true });
        },
      ],
      [
        "a directory",
        async () => {
          await mkdir(target);
        },
        async () => {
          await rm(target, { force: true, recursive: true });
        },
      ],
      [
        "a FIFO",
        async () => {
          execSync(`mkfifo "${target}"`);
        },
        async () => {
          await rm(target, { force: true, recursive: true });
        },
      ],
      [
        "a unix socket",
        async () => {
          const server = createServer();
          await new Promise<void>((resolve, reject) => {
            server.once("error", reject);
            server.listen(target, () => resolve());
          });
          sockets.push(server);
        },
        async () => {
          const server = sockets.pop();
          if (server !== undefined) {
            await new Promise<void>((resolve) => server.close(() => resolve()));
          }
          await rm(target, { force: true, recursive: true });
        },
      ],
    ];
    const sockets: Server[] = [];
    for (const [kind, create, destroy] of kinds) {
      await create();
      const cause = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
        (error) => error,
      );
      expectStoreError(cause, "not_published", "conflict", kind);
      const info = await lstat(target);
      expect(
        info.isSymbolicLink() || info.isDirectory() || info.isFIFO() || info.isSocket(),
      ).toBe(true);
      await destroy();
    }
  } finally {
    await dispose(fixture);
  }
});

test("16. an existing regular file with different bytes or the wrong mode is a conflict", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    const target = join(fixture.waits, "1.request.json");
    await writeFile(target, "totally different\n", { mode: 0o600 });
    const cause = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "conflict", "different canonical bytes");
    expect(await readFile(target, "utf8")).toBe("totally different\n");
    await writeFile(target, preparePipelineV2WaitRequest(requestValue()).canonical_json);
    await chmod(target, 0o644);
    const wrongMode = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(wrongMode, "not_published", "conflict", "mode 0600");
    expect((await fileIdentity(target)).mode).toBe(0o644);
  } finally {
    await dispose(fixture);
  }
});

function linkBarrierIo(): WaitStoreIo {
  let arrived = 0;
  let release: () => void = () => undefined;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  return ioReplacing("link", async (from: string, to: string) => {
    arrived += 1;
    if (arrived === 2) {
      release();
    }
    await gate;
    return realWaitStoreIo.link(from, to);
  });
}

test("17. concurrent identical publishers both succeed through a link barrier", async () => {
  const fixture = await setup();
  try {
    const io = linkBarrierIo();
    const both = await Promise.allSettled([
      publishWaitRequestWithIo(io, fixture.runRoot, requestValue()),
      publishWaitRequestWithIo(io, fixture.runRoot, requestValue()),
    ]);
    expect(both.map((entry) => entry.status).sort()).toEqual(["fulfilled", "fulfilled"]);
    const stored = await readFile(join(fixture.waits, "1.request.json"), "utf8");
    expect(stored).toBe(preparePipelineV2WaitRequest(requestValue()).canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("18. concurrent different manifests: one wins, the loser gets a conflict", async () => {
  const fixture = await setup();
  try {
    const a = requestValue({ reason: "reason_a" });
    const b = requestValue({ reason: "reason_b" });
    const io = linkBarrierIo();
    const both = await Promise.allSettled([
      publishWaitRequestWithIo(io, fixture.runRoot, a),
      publishWaitRequestWithIo(io, fixture.runRoot, b),
    ]);
    expect(both.map((entry) => entry.status).sort()).toEqual(["fulfilled", "rejected"]);
    const stored = await readFile(join(fixture.waits, "1.request.json"), "utf8");
    expect([
      preparePipelineV2WaitRequest(a).canonical_json,
      preparePipelineV2WaitRequest(b).canonical_json,
    ]).toContain(stored);
  } finally {
    await dispose(fixture);
  }
});

function ioWithHandleFault(
  fault: Partial<Record<"write" | "stat" | "sync" | "close", unknown>>,
  writeResults: number[] = [],
): WaitStoreIo {
  let writeCall = 0;
  return ioReplacing("openTempExclusive", async (path: string) => {
    const real = await realWaitStoreIo.openTempExclusive(path);
    return Object.freeze({
      write: async (chunk: Uint8Array, offset: number, length: number, position: number) => {
        writeCall += 1;
        if (fault.write !== undefined) {
          throw fault.write;
        }
        const planned = writeResults[writeCall - 1] ?? length;
        const bytesWritten = Math.min(planned, length);
        await real.write(chunk, offset, bytesWritten, position);
        return bytesWritten;
      },
      stat: async () => {
        if (fault.stat !== undefined) {
          throw fault.stat;
        }
        return await real.stat();
      },
      sync: async () => {
        if (fault.sync !== undefined) {
          throw fault.sync;
        }
        await real.sync();
      },
      close: async () => {
        if (fault.close !== undefined) {
          throw fault.close;
        }
        await real.close();
      },
    });
  });
}

test("19. partial writes are completed by the write-all loop", async () => {
  const fixture = await setup();
  try {
    let writeCalls = 0;
    const io = ioReplacing("openTempExclusive", async (path: string) => {
      const real = await realWaitStoreIo.openTempExclusive(path);
      return Object.freeze({
        write: async (chunk: Uint8Array, offset: number, length: number, position: number) => {
          writeCalls += 1;
          const bytesWritten = writeCalls === 1 ? Math.min(5, length) : length;
          await real.write(chunk, offset, bytesWritten, position);
          return bytesWritten;
        },
        stat: real.stat.bind(real),
        sync: real.sync.bind(real),
        close: real.close.bind(real),
      });
    });
    const published = await publishWaitRequestWithIo(io, fixture.runRoot, requestValue());
    expect(writeCalls).toBeGreaterThan(1);
    expect(await readFile(published.request_path, "utf8")).toBe(published.request.canonical_json);
  } finally {
    await dispose(fixture);
  }
});

test("20. a zero-progress, NaN or oversized write fails after exactly one attempt", async () => {
  const fixture = await setup();
  try {
    for (const result of [0, Number.NaN, 999999]) {
      let calls = 0;
      const io = ioReplacing("openTempExclusive", async (path: string) => {
        const real = await realWaitStoreIo.openTempExclusive(path);
        return Object.freeze({
          write: async () => {
            calls += 1;
            return result;
          },
          stat: real.stat.bind(real),
          sync: real.sync.bind(real),
          close: real.close.bind(real),
        });
      });
      const cause = await publishWaitRequestWithIo(io, fixture.runRoot, requestValue()).catch(
        (error) => error,
      );
      expectStoreError(cause, "not_published", "io_failure", "without progress");
      expect(calls).toBe(1);
      expect(await tempFileNames(fixture.waits)).toEqual([]);
      expect((await readdir(fixture.waits)).filter((name) => name.endsWith(".json"))).toEqual([]);
    }
  } finally {
    await dispose(fixture);
  }
});

test("21. pre-link fault matrix: mkdir, chmod, open, file fsync, close, link fail before publication", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    const faults: [WaitStoreIo, string][] = [
      [ioFaulting("openTempExclusive", injectedFailure("EACCES")), "(errno EACCES)"],
      [ioWithHandleFault({ sync: injectedFailure("EIO") }), "could not be synced"],
      [ioWithHandleFault({ close: injectedFailure("EIO") }), "could not be closed"],
      [ioFaulting("link", injectedFailure("EACCES")), "(errno EACCES)"],
    ];
    for (const [io, fragment] of faults) {
      const cause = await publishWaitRequestWithIo(io, fixture.runRoot, requestValue()).catch(
        (error) => error,
      );
      expectStoreError(cause, "not_published", "io_failure", fragment);
      expect(await tempFileNames(fixture.waits)).toEqual([]);
      expect((await readdir(fixture.waits)).filter((name) => name.endsWith(".json"))).toEqual([]);
    }
    await rm(fixture.waits, { recursive: true, force: true });
    const mkdirCause = await publishWaitRequestWithIo(
      ioFaulting("mkdirExclusive", injectedFailure("EACCES")),
      fixture.runRoot,
      requestValue(),
    ).catch((error) => error);
    expectStoreError(mkdirCause, "not_published", "io_failure", "could not be created");
    const chmodCause = await publishWaitRequestWithIo(
      ioFaulting("chmod", injectedFailure("EACCES")),
      fixture.runRoot,
      requestValue(),
    ).catch((error) => error);
    expectStoreError(chmodCause, "not_published", "io_failure", "could not be created");
    expect((await readdir(fixture.runRoot)).includes("waits")).toBe(false);
  } finally {
    await dispose(fixture);
  }
});

test("22. pre-link cleanup failure never masks the original failure and leaves the temp behind", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    const io = Object.freeze({
      ...realWaitStoreIo,
      link: async () => {
        throw injectedFailure("EACCES");
      },
      unlink: async () => {
        throw injectedFailure("EACCES");
      },
    }) as unknown as WaitStoreIo;
    const cause = await publishWaitRequestWithIo(io, fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "io_failure", "could not be published");
    expect(cause.message).not.toContain("temp");
    const tempFiles = await tempFileNames(fixture.waits);
    expect(tempFiles.length).toBe(1);
    expect((await lstat(join(fixture.waits, tempFiles[0] as string))).isFile()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

function forgedStats(): Stats {
  return {
    isFile: () => true,
    isSymbolicLink: () => false,
    isDirectory: () => false,
    isFIFO: () => false,
    isSocket: () => false,
    dev: 12345,
    ino: 99999,
    mode: 0o600,
  } as unknown as Stats;
}

function ioWithTempSubstitution(): { io: WaitStoreIo; tempPath: () => string } {
  let tempPath = "";
  const io = Object.freeze({
    ...realWaitStoreIo,
    openTempExclusive: async (path: string) => {
      tempPath = path;
      return realWaitStoreIo.openTempExclusive(path);
    },
    lstatOrNull: async (path: string) => {
      if (path === tempPath) {
        return forgedStats();
      }
      return realWaitStoreIo.lstatOrNull(path);
    },
  }) as unknown as WaitStoreIo;
  return { io, tempPath: () => tempPath };
}

test("23. the temp cleanup ownership gate never removes a substituted object", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    const { io, tempPath } = ioWithTempSubstitution();
    const failing = Object.freeze({
      ...io,
      link: async () => {
        throw injectedFailure("EACCES");
      },
    }) as unknown as WaitStoreIo;
    const cause = await publishWaitRequestWithIo(failing, fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "io_failure", "could not be published");
    expect(tempPath()).not.toBe("");
    expect((await lstat(tempPath())).isFile()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("24. post-link fault matrix: temp unlink and directory durability become durability_unknown", async () => {
  const fixture = await setup();
  try {
    const cases: [string, WaitStoreIo, string][] = [
      ["temp_unlink", ioFaulting("unlink", injectedFailure("EACCES")), "could not be removed"],
      ["dir_open", ioFaulting("openDir", injectedFailure("EACCES")), "could not be synced"],
      [
        "dir_fsync",
        ioReplacing("openDir", async (path: string) => {
          const real = await realWaitStoreIo.openDir(path);
          return Object.freeze({
            sync: async () => {
              throw injectedFailure("EIO");
            },
            close: real.close.bind(real),
          });
        }),
        "could not be synced",
      ],
      [
        "dir_close",
        ioReplacing("openDir", async (path: string) => {
          const real = await realWaitStoreIo.openDir(path);
          return Object.freeze({
            sync: real.sync.bind(real),
            close: async () => {
              throw injectedFailure("EIO");
            },
          });
        }),
        "could not be synced",
      ],
    ];
    for (const [name, io, fragment] of cases) {
      const value = requestValue({ wait_index: 2, reason: `reason_${name}` });
      const prepared = preparePipelineV2WaitRequest(value);
      const cause = await publishWaitRequestWithIo(io, fixture.runRoot, value).catch(
        (error) => error,
      );
      const error = expectStoreError(cause, "durability_unknown", "io_failure", fragment);
      const finalPath = join(fixture.waits, "2.request.json");
      expect(error.candidate?.kind).toBe("request");
      expect(error.candidate?.wait_index).toBe(2);
      expect(error.candidate?.final_path).toBe(finalPath);
      expect(error.candidate?.canonical_json).toBe(prepared.canonical_json);
      expect(error.candidate?.sha256).toBe(prepared.sha256);
      expect(Object.isFrozen(error.candidate)).toBe(true);
      expect(await readFile(finalPath, "utf8")).toBe(prepared.canonical_json);
      const retry = await publishPipelineV2WaitRequest(fixture.runRoot, value);
      expect(retry.request.sha256).toBe(prepared.sha256);
      expect(await readFile(finalPath, "utf8")).toBe(prepared.canonical_json);
      await rm(finalPath, { force: true });
    }
  } finally {
    await dispose(fixture);
  }
});

test("25. a post-link substituted temp is a durability_unknown and is never removed", async () => {
  const fixture = await setup();
  try {
    const { io, tempPath } = ioWithTempSubstitution();
    const value = requestValue({ wait_index: 4 });
    const prepared = preparePipelineV2WaitRequest(value);
    const cause = await publishWaitRequestWithIo(io, fixture.runRoot, value).catch(
      (error) => error,
    );
    const error = expectStoreError(cause, "durability_unknown", "io_failure", "confirmed as owned");
    expect(error.candidate?.sha256).toBe(prepared.sha256);
    expect(await readFile(join(fixture.waits, "4.request.json"), "utf8")).toBe(prepared.canonical_json);
    expect((await lstat(tempPath())).isFile()).toBe(true);
  } finally {
    await dispose(fixture);
  }
});

test("26. an idempotent repeat still fsyncs the waits directory before returning", async () => {
  const fixture = await setup();
  try {
    await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const cause = await publishWaitRequestWithIo(
      ioFaulting("openDir", injectedFailure("EACCES")),
      fixture.runRoot,
      requestValue(),
    ).catch((error) => error);
    expectStoreError(cause, "durability_unknown", "io_failure", "after adoption");
    expect(await readFile(join(fixture.waits, "1.request.json"), "utf8")).toBe(
      preparePipelineV2WaitRequest(requestValue()).canonical_json,
    );
  } finally {
    await dispose(fixture);
  }
});

test("27. published results are deep-frozen and input mutations cannot reach them", async () => {
  const fixture = await setup();
  try {
    const value = requestValue();
    const published = await publishPipelineV2WaitRequest(fixture.runRoot, value);
    expect(Object.isFrozen(published)).toBe(true);
    expect(Object.isFrozen(published.request)).toBe(true);
    expect(Object.isFrozen(published.request.manifest)).toBe(true);
    expect(Object.isFrozen(published.request.manifest.actions)).toBe(true);
    expect(Object.isFrozen(published.request.manifest.actions[0])).toBe(true);
    const reasonBefore = published.request.manifest.reason;
    (value as Record<string, unknown>).reason = "mutated_after_publish";
    expect(published.request.manifest.reason).toBe(reasonBefore);
    expect(() => {
      (published.request.manifest as unknown as Record<string, unknown>).run_id = "other";
    }).toThrow();
  } finally {
    await dispose(fixture);
  }
});

test("28. diagnostics never contain manifest content or canaries", async () => {
  const fixture = await setup();
  try {
    const value = requestWithCanary();
    const prepared = preparePipelineV2WaitRequest(value);
    await publishPipelineV2WaitRequest(fixture.runRoot, value);
    const conflictCause = await publishPipelineV2WaitRequest(
      fixture.runRoot,
      requestValue({ reason: "different_reason" }),
    ).catch((error) => error);
    expectStoreError(conflictCause, "not_published", "conflict");
    const responseCause = await publishPipelineV2WaitResponse(
      fixture.runRoot,
      1,
      JSON.stringify({
        schema_version: 1,
        run_id: RUN_ID,
        wait_index: 1,
        request_sha256: prepared.sha256,
        action_id: "undeclared_action",
      }),
    ).catch((error) => error);
    expect(responseCause).toBeInstanceOf(PipelineV2WaitManifestError);
    for (const error of [conflictCause, responseCause]) {
      expect((error as Error).message).not.toContain(CANARY);
      expect((error as Error).message).not.toContain("stage_iteration_limit_exhausted");
      expect((error as Error).message).not.toContain("different_reason");
    }
    const layoutCause = await publishPipelineV2WaitRequest("/nonexistent-root", value).catch(
      (error) => error,
    );
    expectStoreError(layoutCause, "not_published", "invalid_layout");
    expect((layoutCause as Error).message).not.toContain("/nonexistent-root");
  } finally {
    await dispose(fixture);
  }
});

test("29. publication never modifies anything outside the waits directory", async () => {
  const fixture = await setup();
  try {
    await writeFile(join(fixture.runRoot, "state.json"), '{"revision":1}\n', { mode: 0o600 });
    await mkdir(join(fixture.runRoot, "project"), { mode: 0o700 });
    await writeFile(join(fixture.runRoot, "project", "a.md"), "project body\n", { mode: 0o600 });
    await mkdir(join(fixture.runRoot, "data", "inputs"), { mode: 0o700, recursive: true });
    await writeFile(join(fixture.runRoot, "data", "inputs", "x.json"), "{}\n", { mode: 0o600 });
    await mkdir(join(fixture.runRoot, "data", "outputs"), { mode: 0o700, recursive: true });
    await writeFile(join(fixture.runRoot, "sentinel"), "sentinel\n", { mode: 0o600 });
    const paths = [
      join(fixture.runRoot, "state.json"),
      join(fixture.runRoot, "project", "a.md"),
      join(fixture.runRoot, "data", "inputs", "x.json"),
      join(fixture.runRoot, "sentinel"),
    ];
    const before = await Promise.all(paths.map((path) => readFile(path)));
    await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    const after = await Promise.all(paths.map((path) => readFile(path)));
    expect(after).toEqual(before);
    expect((await readdir(fixture.runRoot)).sort()).toEqual([
      "data",
      "project",
      "sentinel",
      "state.json",
      "waits",
    ]);
    const conflictCause = await publishPipelineV2WaitRequest(
      fixture.runRoot,
      requestValue({ reason: "another_reason" }),
    ).catch((error) => error);
    expectStoreError(conflictCause, "not_published", "conflict");
    expect(await Promise.all(paths.map((path) => readFile(path)))).toEqual(before);
  } finally {
    await dispose(fixture);
  }
});

test("30. external sentinels inside the waits directory are untouched by failures", async () => {
  const fixture = await setup();
  try {
    await mkdir(fixture.waits, { mode: 0o700 });
    await writeFile(join(fixture.waits, "1.request.json"), "conflicting bytes\n", { mode: 0o600 });
    await writeFile(join(fixture.waits, ".sentinel"), "sentinel\n", { mode: 0o600 });
    const cause = await publishPipelineV2WaitRequest(fixture.runRoot, requestValue()).catch(
      (error) => error,
    );
    expectStoreError(cause, "not_published", "conflict");
    expect(await readFile(join(fixture.waits, ".sentinel"), "utf8")).toBe("sentinel\n");
    await writeFile(
      join(fixture.waits, "1.request.json"),
      preparePipelineV2WaitRequest(requestValue()).canonical_json,
      { mode: 0o600 },
    );
    await publishPipelineV2WaitRequest(fixture.runRoot, requestValue());
    expect(await readFile(join(fixture.waits, ".sentinel"), "utf8")).toBe("sentinel\n");
  } finally {
    await dispose(fixture);
  }
});

test("31. the public export surface carries no test seam", async () => {
  const publicNamespace = (await import("../src/pipeline_v2_wait_store.ts")) as Record<
    string,
    unknown
  >;
  expect(Object.keys(publicNamespace).sort()).toEqual([
    "PipelineV2WaitStoreError",
    "publishPipelineV2WaitRequest",
    "publishPipelineV2WaitResponse",
  ]);
});
