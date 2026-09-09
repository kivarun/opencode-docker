import { lstat, mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";
import { spawn } from "node:child_process";
import { expect, test } from "bun:test";
import { PipelineError, loadPipeline } from "../src/pipeline.ts";
import { loadPipelineV2 } from "../src/pipeline_v2.ts";
import {
  acceptActivationOutputs,
  acceptedOutputDigest,
  prepareActivationData,
  snapshotRunInputs,
} from "../src/pipeline_v2_runtime.ts";
import {
  compilePipelineJsonSchema,
  isSyncValidatorSuccess,
  validatePipelineJson,
} from "../src/pipeline_v2_schema.ts";

const FACTS_SCHEMA = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  type: "object",
  required: ["revision", "actor"],
  additionalProperties: false,
  properties: {
    revision: { $ref: "#/$defs/positiveInt" },
    actor: { type: "string" },
    extra: { $ref: "#/$defs/nested" },
    sentinel: { type: "integer", default: 7 },
  },
  $defs: {
    positiveInt: { type: "integer", minimum: 1 },
    nested: {
      type: "object",
      required: ["id"],
      properties: { id: { $ref: "#/$defs/positiveInt" } },
    },
  },
};

const FACTS_OK = { revision: 2, actor: "orchestrator", extra: { id: 3 } };

const ACCEPT_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: brief
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs:
      - id: brief
        source:
          pipeline_input: brief
    outputs:
      - id: patch
        type: file
      - id: notes
        type: directory
      - id: facts
        type: json
        schema: schemas/facts.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: architect

  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs:
      - id: patch
        source:
          state_output:
            state: coder
            output: patch
      - id: notes
        source:
          state_output:
            state: coder
            output: notes
      - id: facts
        source:
          state_output:
            state: coder
            output: facts
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done

  - id: done
    type: terminal
    result: success
`;

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundleDirs(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-accept-test-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  return { root, bundle };
}

async function writeAcceptBundle(
  dirs: BundleDirs,
  factsSchema: unknown = FACTS_SCHEMA,
): Promise<void> {
  await writeFile(join(dirs.bundle, "pipeline.yaml"), ACCEPT_YAML);
  await writeFile(join(dirs.bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(dirs.bundle, "prompts", "architect.md"), "review the patch\n");
  await writeFile(join(dirs.bundle, "schemas", "facts.schema.json"), JSON.stringify(factsSchema));
}

async function makeRunRoot(root: string, factsValue: unknown = FACTS_OK): Promise<string> {
  const runRoot = join(root, "run");
  await mkdir(runRoot, { recursive: true });
  await mkdir(join(runRoot, "project"), { recursive: true, mode: 0o700 });
  const userdata = join(root, "userdata");
  await mkdir(userdata, { recursive: true });
  await writeFile(join(userdata, "brief.json"), JSON.stringify(factsValue));
  return runRoot;
}

const ALL_BINDINGS = (root: string) => [
  { id: "brief", path: join(root, "userdata", "brief.json") },
];

async function withAccept(
  fn: (dirs: BundleDirs) => Promise<void>,
  factsSchema: unknown = FACTS_SCHEMA,
): Promise<void> {
  const dirs = await makeBundleDirs();
  await writeAcceptBundle(dirs, factsSchema);
  try {
    await fn(dirs);
  } finally {
    await rm(dirs.root, { recursive: true, force: true });
  }
}

async function expectReject(
  run: () => Promise<unknown> | unknown,
  message: RegExp | string,
  notContaining?: string,
): Promise<void> {
  let failure: unknown;
  try {
    await run();
  } catch (cause) {
    failure = cause;
  }
  expect(failure).toBeInstanceOf(PipelineError);
  const error = failure as Error;
  if (typeof message === "string") {
    expect(error.message).toBe(message);
  } else {
    expect(error.message).toMatch(message);
  }
  if (notContaining !== undefined) {
    expect(error.message).not.toContain(notContaining);
  }
}

async function makeFifo(path: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn("mkfifo", [path]);
    child.on("error", (cause) => reject(cause));
    child.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`mkfifo exited ${code}`))));
  });
}

/** Load the pipeline, snapshot the inputs, and prepare the coder activation. */
async function prepareCoder(
  dirs: BundleDirs,
  briefValue: unknown = FACTS_OK,
): Promise<{
  pipeline: Awaited<ReturnType<typeof loadPipelineV2>>;
  snap: Awaited<ReturnType<typeof snapshotRunInputs>>;
  prep: Awaited<ReturnType<typeof prepareActivationData>>;
}> {
  const pipeline = await loadPipelineV2(dirs.bundle);
  const runRoot = await makeRunRoot(dirs.root, briefValue);
  const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(dirs.root), runRoot);
  const prep = await prepareActivationData(pipeline, snap, [], "coder", 1);
  return { pipeline, snap, prep };
}

/** Write a complete, valid worker output set into the coder activation. */
async function writeCoderOutputs(
  prep: Awaited<ReturnType<typeof prepareActivationData>>,
  factsValue: unknown = FACTS_OK,
): Promise<void> {
  await writeFile(join(prep.outputs_root, "patch"), "PATCH-1\n");
  await mkdir(join(prep.outputs_root, "notes", "nested"), { recursive: true });
  await writeFile(join(prep.outputs_root, "notes", "summary.txt"), "SUMMARY");
  await writeFile(join(prep.outputs_root, "notes", "nested", "deep.txt"), "DEEP");
  await writeFile(join(prep.outputs_root, "facts"), JSON.stringify(factsValue));
}

async function cpTree(source: string, target: string): Promise<void> {
  await mkdir(target, { recursive: true });
  for (const entry of await readdir(source, { withFileTypes: true })) {
    const child = join(source, entry.name);
    if (entry.isDirectory()) {
      await cpTree(child, join(target, entry.name));
    } else if (entry.isFile()) {
      await writeFile(join(target, entry.name), await readFile(child));
    }
  }
}

test("1. file/json/directory outputs are accepted as exact declaration-order records", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, snap, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);
    const records = await acceptActivationOutputs(pipeline, prep);

    // one record per declared output, in declaration order
    expect(records.map((record) => record.state)).toEqual(["coder", "coder", "coder"]);
    expect(records.map((record) => record.output)).toEqual(["patch", "notes", "facts"]);
    expect(records.map((record) => record.activation_index)).toEqual([1, 1, 1]);
    for (const record of records) {
      expect(Object.keys(record).sort()).toEqual([
        "activation_index",
        "digest",
        "output",
        "state",
      ]);
      expect(record.digest).toMatch(/^[0-9a-f]{64}$/);
    }

    // the records serialize without paths, types, schemas or content
    const serialized = JSON.stringify(records);
    expect(serialized).not.toContain(prep.outputs_root);
    expect(serialized).not.toContain("PATCH-1");
    expect(serialized).not.toContain("orchestrator");

    // the records are frozen
    expect(Object.isFrozen(records)).toBe(true);
    expect(Object.isFrozen(records[0])).toBe(true);

    // the accepted outputs hand over into the next activation
    const next = await prepareActivationData(pipeline, snap, records, "architect", 2);
    expect(await readFile(join(next.inputs_root, "patch"), "utf8")).toBe("PATCH-1\n");
    expect(await readFile(join(next.inputs_root, "notes", "nested", "deep.txt"), "utf8")).toBe("DEEP");
    expect(JSON.parse(await readFile(join(next.inputs_root, "facts"), "utf8"))).toEqual(FACTS_OK);
  });
});

test("2. missing declared outputs and undeclared top-level entries are rejected", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);

    // a missing declared output fails
    await rm(join(prep.outputs_root, "patch"), { force: true });
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /output port "patch" of agent state "coder" activation 1 is missing from the activation outputs root/,
    );
    await writeFile(join(prep.outputs_root, "patch"), "PATCH-1\n");

    // extra entries of every kind fail
    await writeFile(join(prep.outputs_root, "extra.txt"), "EXTRA");
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /activation outputs root .* contains undeclared entry "extra\.txt"/,
    );
    await rm(join(prep.outputs_root, "extra.txt"), { force: true });

    await mkdir(join(prep.outputs_root, "extra-dir"));
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /contains undeclared entry "extra-dir"/,
    );
    await rm(join(prep.outputs_root, "extra-dir"), { recursive: true, force: true });

    await symlink(join(dirs.root, "outside"), join(prep.outputs_root, "extra-link"));
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /contains undeclared entry "extra-link"/,
    );
    await rm(join(prep.outputs_root, "extra-link"), { force: true });

    await makeFifo(join(prep.outputs_root, "extra-fifo"));
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /contains undeclared entry "extra-fifo"/,
    );
    await rm(join(prep.outputs_root, "extra-fifo"), { force: true });

    // the declared output types come only from the compiled pipeline: a
    // file output replaced by a directory is rejected
    await rm(join(prep.outputs_root, "patch"), { force: true });
    await mkdir(join(prep.outputs_root, "patch"));
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /output port "patch" of agent state "coder" activation 1 declares type "file" but the output is an existing directory/,
    );
  });
});

test("3. symlink final components, substituted roots and broken parents are rejected", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);

    // a symlink at a declared output position is rejected before any read
    await rm(join(prep.outputs_root, "patch"), { force: true });
    const outside = join(dirs.root, "outside.txt");
    await writeFile(outside, "ESCAPED");
    await symlink(outside, join(prep.outputs_root, "patch"));
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /output port "patch" of agent state "coder" activation 1 output .* is a symbolic link/,
    );
    expect(await readFile(outside, "utf8")).toBe("ESCAPED");
    await rm(join(prep.outputs_root, "patch"), { force: true });
    await writeFile(join(prep.outputs_root, "patch"), "PATCH-1\n");

    // the outputs root itself replaced by a symlink (even to a real
    // directory with identical content inside the run) no longer resolves
    // to itself
    const shadow = join(dirs.root, "shadow-outputs");
    await mkdir(shadow, { recursive: true });
    await cpTree(prep.outputs_root, shadow);
    await rm(prep.outputs_root, { recursive: true, force: true });
    await symlink(shadow, prep.outputs_root);
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /activation outputs root .* exists but is a symbolic link/,
    );
    expect((await lstat(prep.outputs_root)).isSymbolicLink()).toBe(true);
  });

  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);

    // a broken parent (the data root replaced by a regular file) cannot
    // even be inspected
    await rm(prep.outputs_root, { recursive: true, force: true });
    await rm(prep.data_root, { recursive: true, force: true });
    await writeFile(prep.data_root, "not a directory");
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /filesystem entry .*\/data\/outputs cannot be inspected/,
    );
  });
});

test("4. directory outputs with nested symlink/FIFO/socket entries are rejected", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);

    const outside = join(dirs.root, "outside-dir");
    await mkdir(outside, { recursive: true });
    await symlink(outside, join(prep.outputs_root, "notes", "link"));
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /output port "notes" of agent state "coder" activation 1 contains symlink entry "link"/,
    );
    await rm(join(prep.outputs_root, "notes", "link"), { force: true });

    await makeFifo(join(prep.outputs_root, "notes", "pipe"));
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /output port "notes" of agent state "coder" activation 1 contains unsupported entry "pipe"/,
    );
    await rm(join(prep.outputs_root, "notes", "pipe"), { force: true });

    const socketPath = join(prep.outputs_root, "notes", "sock");
    const server = net.createServer();
    await new Promise<void>((resolve) => server.listen(socketPath, resolve));
    try {
      await expectReject(
        () => acceptActivationOutputs(pipeline, prep),
        /output port "notes" of agent state "coder" activation 1 contains unsupported entry "sock"/,
      );
    } finally {
      server.close();
    }
  });
});

test("5. invalid JSON and schema-violating JSON are rejected with value-free diagnostics", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);

    // malformed JSON is rejected with one stable, content-free diagnostic:
    // the exact message never carries the parser message, the offending
    // token, a position, or any fragment of the input, so none of the
    // secret canaries below can leak through it
    const factsWhat = 'output port "facts" of agent state "coder" activation 1';
    const factsTarget = join(prep.outputs_root, "facts");
    const notValidJson = `${factsWhat} ${factsTarget} is not valid JSON`;

    // unexpected identifier (bare token)
    await writeFile(factsTarget, '{"revision": 2, "actor": BARE_CANARY_IDENTIFIER}');
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      notValidJson,
      "BARE_CANARY_IDENTIFIER",
    );

    // unterminated string whose canary sits right before the failure
    await writeFile(factsTarget, '{"revision": 2, "actor": "UNTERMINATED_CANARY_STRING}');
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      notValidJson,
      "UNTERMINATED_CANARY_STRING",
    );

    // error after an otherwise valid secret string value
    await writeFile(factsTarget, '{"revision": 2, "actor": "AFTER_SECRET_CANARY",}');
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      notValidJson,
      "AFTER_SECRET_CANARY",
    );

    // wrong types are not coerced: a string revision stays invalid
    await writeFile(
      join(prep.outputs_root, "facts"),
      JSON.stringify({ revision: "5", actor: "x", extra: { id: 3 } }),
    );
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /output port "facts" of agent state "coder" activation 1 does not conform to its JSON schema: \/revision: must be integer/,
      // the diagnostic never contains the invalid value
      '"5"',
    );

    // minimum bounds are enforced and the diagnostics carry no data values
    await writeFile(
      join(prep.outputs_root, "facts"),
      JSON.stringify({ revision: 0, actor: "x" }),
    );
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /\/revision: must be >= 1/,
    );

    // additional properties are rejected and no value is echoed into the
    // diagnostic
    await writeFile(
      join(prep.outputs_root, "facts"),
      JSON.stringify({ ...FACTS_OK, sneaky: "TOP-SECRET-VALUE" }),
    );
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /must NOT have additional properties/,
      "TOP-SECRET-VALUE",
    );

    // a missing required property fails
    await writeFile(
      join(prep.outputs_root, "facts"),
      JSON.stringify({ revision: 2 }),
    );
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /must have required property 'actor'/,
    );
  });
});

test("6. $defs and internal $refs are honored; validation never mutates the value", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);
    // the schema references $defs.nested which itself references
    // $defs.positiveInt; the chained internal refs validate correctly
    const records = await acceptActivationOutputs(pipeline, prep);
    expect(records.map((record) => record.output)).toEqual(["patch", "notes", "facts"]);

    // an invalid nested $ref target is rejected the same way
    await writeFile(
      join(prep.outputs_root, "facts"),
      JSON.stringify({ revision: 2, actor: "x", extra: { id: "wrong" } }),
    );
    await expectReject(
      () => acceptActivationOutputs(pipeline, prep),
      /\/extra\/id: must be integer/,
    );

    // validation applies no coercion, no defaults and removes nothing,
    // whether the value is frozen or not
    const pipeline2 = await loadPipelineV2(dirs.bundle);
    const coderState = pipeline2.states.find((state) => state.type === "agent" && state.id === "coder");
    if (coderState === undefined || coderState.type !== "agent") {
      throw new Error("expected coder agent state");
    }
    const port = coderState.outputs.find((entry) => entry.id === "facts");
    if (port === undefined || port.schema === undefined) {
      throw new Error("expected facts schema snapshot");
    }
    const schema = port.schema;

    const frozenProbe = Object.freeze({ revision: 5, actor: "x" });
    validatePipelineJson(schema, frozenProbe, "probe");
    expect(Object.keys(frozenProbe).sort()).toEqual(["actor", "revision"]);

    const mutatingProbe = { revision: 5, actor: "x" };
    validatePipelineJson(schema, mutatingProbe, "probe");
    expect(Object.keys(mutatingProbe).sort()).toEqual(["actor", "revision"]);

    await expectReject(
      () => validatePipelineJson(schema, { revision: "7", actor: "x" }, "probe"),
      /probe does not conform to its JSON schema/,
      '"5"',
    );
  });
});

test("7. output digests depend only on type, names and content", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);
    const records = await acceptActivationOutputs(pipeline, prep);
    const byOutput = new Map(records.map((record) => [record.output, record]));

    // the same content in a different location with different timestamps
    // digests identically
    const elsewhere = join(dirs.root, "elsewhere");
    await mkdir(join(elsewhere, "nested"), { recursive: true });
    await writeFile(join(elsewhere, "summary.txt"), "SUMMARY");
    await writeFile(join(elsewhere, "nested", "deep.txt"), "DEEP");
    const epoch = new Date(0);
    await utimes(join(elsewhere, "summary.txt"), epoch, epoch);
    await utimes(join(elsewhere, "nested"), epoch, epoch);
    const elsewhere2 = join(dirs.root, "elsewhere2");
    await mkdir(elsewhere2, { recursive: true });
    await writeFile(join(elsewhere2, "f.txt"), "PATCH-1\n");
    await writeFile(join(elsewhere2, "j.json"), JSON.stringify(FACTS_OK));
    await utimes(join(elsewhere2, "f.txt"), epoch, epoch);
    const digestFile = await acceptedOutputDigest("file", join(elsewhere2, "f.txt"), "test file");
    const digestDir = await acceptedOutputDigest("directory", elsewhere, "test dir");
    const digestJson = await acceptedOutputDigest("json", join(elsewhere2, "j.json"), "test json");
    expect(byOutput.get("patch")?.digest).toBe(digestFile);
    expect(byOutput.get("notes")?.digest).toBe(digestDir);
    expect(byOutput.get("facts")?.digest).toBe(digestJson);

    // an empty directory digests identically wherever it lives
    const emptyA = join(dirs.root, "empty-a");
    const emptyB = join(dirs.root, "empty-b");
    await mkdir(emptyA);
    await mkdir(emptyB);
    expect(await acceptedOutputDigest("directory", emptyA, "a")).toBe(
      await acceptedOutputDigest("directory", emptyB, "b"),
    );

    // changing bytes, entry names or empty directories changes the digest
    await writeFile(join(elsewhere2, "f.txt"), "PATCH-1\r\n");
    expect(await acceptedOutputDigest("file", join(elsewhere2, "f.txt"), "test")).not.toBe(
      digestFile,
    );
    await rm(join(elsewhere, "summary.txt"), { force: true });
    await writeFile(join(elsewhere, "renamed.txt"), "SUMMARY");
    expect(await acceptedOutputDigest("directory", elsewhere, "test")).not.toBe(digestDir);
    await rm(join(elsewhere, "renamed.txt"), { force: true });
    await writeFile(join(elsewhere, "summary.txt"), "SUMMARY");
    await mkdir(join(elsewhere, "empty-sub"));
    expect(await acceptedOutputDigest("directory", elsewhere, "test")).not.toBe(digestDir);
    await rm(join(elsewhere, "empty-sub"), { recursive: true, force: true });

    // a directory entry and a file entry are never the same representation
    const dirEntry = join(dirs.root, "dir-entry");
    const fileEntry = join(dirs.root, "file-entry");
    await mkdir(join(dirEntry, "x"), { recursive: true });
    await mkdir(fileEntry, { recursive: true });
    await writeFile(join(fileEntry, "x"), "");
    expect(await acceptedOutputDigest("directory", dirEntry, "a")).not.toBe(
      await acceptedOutputDigest("directory", fileEntry, "b"),
    );

    // the declared type participates in the digest: the same bytes are
    // never equal as file and json
    expect(digestFile).not.toBe(digestJson);

    // an unknown type is rejected
    await expectReject(
      () => acceptedOutputDigest("nope" as never, join(elsewhere, "f.txt"), "test"),
      /must be one of "file", "directory" or "json"/,
    );
  });
});

test("8. outputs changed after acceptance fail the next activation", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, snap, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);
    const records = await acceptActivationOutputs(pipeline, prep);
    const runRoot = prep.run_root;

    // the winning output changes after acceptance
    await writeFile(join(prep.outputs_root, "patch"), "PATCH-TAMPERED");
    await expectReject(
      () => prepareActivationData(pipeline, snap, records, "architect", 2),
      /accepted state output for "coder"\."patch" at activation index 1 digest mismatch: recorded [0-9a-f]{64}, recomputed [0-9a-f]{64}/,
    );
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();

    // renaming a directory entry inside an accepted directory output
    await writeFile(join(prep.outputs_root, "patch"), "PATCH-1\n");
    await rm(join(prep.outputs_root, "notes", "summary.txt"), { force: true });
    await writeFile(join(prep.outputs_root, "notes", "renamed.txt"), "SUMMARY");
    await expectReject(
      () => prepareActivationData(pipeline, snap, records, "architect", 2),
      /accepted state output for "coder"\."notes" at activation index 1 digest mismatch/,
    );
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();
    await rm(join(prep.outputs_root, "notes", "renamed.txt"), { force: true });
    await writeFile(join(prep.outputs_root, "notes", "summary.txt"), "SUMMARY");

    // changing the kind of an accepted output is detected
    await rm(join(prep.outputs_root, "patch"), { force: true });
    await mkdir(join(prep.outputs_root, "patch"));
    await expectReject(
      () => prepareActivationData(pipeline, snap, records, "architect", 2),
      /fixed output .*1-coder\/data\/outputs\/patch is not a regular file/,
    );
    await expect(lstat(join(runRoot, "activations", "2-architect"))).rejects.toThrow();
    await rm(join(prep.outputs_root, "patch"), { recursive: true, force: true });
    await writeFile(join(prep.outputs_root, "patch"), "PATCH-1\n");
  });

  await withAccept(async (dirs) => {
    // an old, non-winning accepted output is verified as well
    const { pipeline, snap, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);
    const firstRecords = await acceptActivationOutputs(pipeline, prep);
    const runRoot = prep.run_root;
    const second = await prepareActivationData(pipeline, snap, firstRecords, "coder", 2);
    await writeFile(join(second.outputs_root, "patch"), "PATCH-2");
    await writeFile(join(second.outputs_root, "facts"), JSON.stringify(FACTS_OK));
    const secondRecords = await acceptActivationOutputs(pipeline, second);
    expect(secondRecords.map((record) => record.output)).toEqual(["patch", "notes", "facts"]);
    expect(secondRecords.map((record) => record.activation_index)).toEqual([2, 2, 2]);

    // corrupting the OLD non-winning patch output fails even though the
    // index-2 record wins
    await writeFile(join(prep.outputs_root, "patch"), "PATCH-TAMPERED");
    await expectReject(
      () => prepareActivationData(
        pipeline,
        snap,
        [...firstRecords, ...secondRecords],
        "architect",
        3,
      ),
      /accepted state output for "coder"\."patch" at activation index 1 digest mismatch/,
    );
    await expect(lstat(join(runRoot, "activations", "3-architect"))).rejects.toThrow();

    // permuting the correct history does not change the winner
    await writeFile(join(prep.outputs_root, "patch"), "PATCH-1\n");
    const shuffled = [
      ...secondRecords.slice(1),
      ...firstRecords,
      secondRecords[0],
    ].filter((record) => record !== undefined);
    const next = await prepareActivationData(pipeline, snap, shuffled, "architect", 3);
    expect(await readFile(join(next.inputs_root, "patch"), "utf8")).toBe("PATCH-2");
  });
});

test("9. provenance: forged, cloned and proxied prepared activations are rejected", async () => {
  await withAccept(async (dirs) => {
    const { pipeline, prep } = await prepareCoder(dirs);
    await writeCoderOutputs(prep);

    // hand-built and cloned objects are rejected before any field is read
    await expectReject(
      () => acceptActivationOutputs(pipeline, {} as never),
      /output acceptance requires the frozen prepared activation data object returned by a successful prepareActivationData call for the same trusted pipeline/,
    );
    await expectReject(
      () => acceptActivationOutputs(pipeline, structuredClone(prep) as never),
      /output acceptance requires the frozen prepared activation data object/,
    );
    await expectReject(
      () => acceptActivationOutputs(pipeline, { ...prep } as never),
      /output acceptance requires the frozen prepared activation data object/,
    );

    // a prepared activation of another pipeline object is rejected
    const otherPipeline = await loadPipelineV2(dirs.bundle);
    await expectReject(
      () => acceptActivationOutputs(otherPipeline, prep),
      /output acceptance requires the frozen prepared activation data object/,
    );

    // getter and Proxy traps are never invoked
    let getterInvoked = 0;
    const spy = new Proxy(prep, {
      get(target, property, receiver) {
        getterInvoked += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    await expectReject(
      () => acceptActivationOutputs(pipeline, spy as never),
      /output acceptance requires the frozen prepared activation data object/,
    );
    expect(getterInvoked).toBe(0);

    // a forged pipeline argument is rejected before any filesystem access
    await expectReject(
      () => acceptActivationOutputs({} as never, prep),
      /output acceptance requires the deep-frozen snapshot object returned by loadPipelineV2/,
    );

    // nothing was read from or written to the outputs tree
    expect(await readFile(join(prep.outputs_root, "patch"), "utf8")).toBe("PATCH-1\n");
    expect((await readdir(prep.outputs_root)).sort()).toEqual([
      "facts",
      "notes",
      "patch",
    ]);
  });
});

test("10. schema compile failures reject loadPipelineV2", async () => {
  const broken: readonly [unknown, RegExp][] = [
    [
      { type: "object", properties: { x: { $ref: "#/$defs/missing" } } },
      /pipeline input "brief" schema cannot be compiled as JSON Schema Draft 2020-12: .*can't resolve reference/,
    ],
    [
      { type: "object", properties: { x: { $ref: "https://example.com/other.json" } } },
      /pipeline input "brief" schema cannot be compiled as JSON Schema Draft 2020-12/,
    ],
    [
      { type: "string", format: "made-up-format" },
      /pipeline input "brief" schema cannot be compiled as JSON Schema Draft 2020-12/,
    ],
    [
      { type: "nonsense" },
      /pipeline input "brief" schema cannot be compiled as JSON Schema Draft 2020-12/,
    ],
    [
      // a top-level $async schema compiles to a Promise-returning validator
      // and is rejected fail-closed before it is ever registered
      {
        type: "object",
        properties: { ok: { type: "boolean" } },
        required: ["ok"],
        $async: true,
      },
      /pipeline input "brief" schema compiles to an asynchronous JSON Schema validator; pipeline v2 supports synchronous validators only/,
    ],
    [
      // an $async subschema behind an internal $ref fails Ajv compilation
      // outright and rejects the load the same way
      {
        type: "object",
        properties: { x: { $ref: "#/$defs/asyncThing" } },
        $defs: { asyncThing: { type: "string", $async: true } },
      },
      /pipeline input "brief" schema cannot be compiled as JSON Schema Draft 2020-12/,
    ],
  ];
  for (const [schema, pattern] of broken) {
    await withAccept(async (dirs) => {
      await expectReject(() => loadPipelineV2(dirs.bundle), pattern);
    }, schema);
  }
});

test("11. json run inputs are validated by the same compiled schema mechanism", async () => {
  await withAccept(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const runRoot = await makeRunRoot(dirs.root, { revision: 0, actor: "x" });
    // a json run input that violates the declared schema is rejected by
    // the same compiled Draft 2020-12 mechanism before any mutation
    await expectReject(
      () => snapshotRunInputs(pipeline, ALL_BINDINGS(dirs.root), runRoot),
      /pipeline input "brief" bound file .* does not conform to its JSON schema: \/revision: must be >= 1/,
    );
    await expect(lstat(join(runRoot, "data"))).rejects.toThrow();

    // a malformed json run input is rejected with the same stable,
    // content-free diagnostic: no canary, parser token or input fragment
    const malformedRoot = await makeRunRoot(dirs.root);
    const briefPath = join(dirs.root, "userdata", "brief.json");
    await writeFile(briefPath, '{"revision": 2, "actor": "RUN_INPUT_CANARY",}');
    await expectReject(
      () => snapshotRunInputs(pipeline, ALL_BINDINGS(dirs.root), malformedRoot),
      `pipeline input "brief" bound file ${briefPath} is not valid JSON`,
      "RUN_INPUT_CANARY",
    );
    await expect(lstat(join(malformedRoot, "data"))).rejects.toThrow();

    // a conforming run input snapshots normally
    const okRoot = await makeRunRoot(dirs.root, { revision: 1, actor: "y" });
    const snap = await snapshotRunInputs(pipeline, ALL_BINDINGS(dirs.root), okRoot);
    expect(snap.inputs.map((entry) => entry.id)).toEqual(["brief"]);
  });
});

test("12. the v1 path and the production v2 rejection are unchanged", async () => {
  await withAccept(async (dirs) => {
    await expect(loadPipeline(dirs.bundle)).rejects.toThrow(
      "pipeline schema version 2 is not executable yet",
    );
    const pipeline = await loadPipelineV2(dirs.bundle);
    expect(pipeline.schema_version).toBe(2);
    expect(pipeline.states.map((state) => state.id)).toEqual(["coder", "architect", "done"]);
  });
});

test("13. only literal true is sync validation success; async validators are rejected and unregistered", async () => {
  await withAccept(async (dirs) => {
    const pipeline = await loadPipelineV2(dirs.bundle);
    const coderState = pipeline.states.find(
      (state) => state.type === "agent" && state.id === "coder",
    );
    if (coderState === undefined || coderState.type !== "agent") {
      throw new Error("expected coder agent state");
    }
    const port = coderState.outputs.find((entry) => entry.id === "facts");
    if (port === undefined || port.schema === undefined) {
      throw new Error("expected facts schema snapshot");
    }

    // the sync-success contract: only the literal boolean true passes, and
    // a Promise, thenable, or any other result is never success
    expect(isSyncValidatorSuccess(true)).toBe(true);
    expect(isSyncValidatorSuccess(false)).toBe(false);
    expect(isSyncValidatorSuccess(Promise.resolve(true))).toBe(false);
    expect(isSyncValidatorSuccess({ then: () => {} })).toBe(false);
    expect(isSyncValidatorSuccess(1)).toBe(false);
    expect(isSyncValidatorSuccess("true")).toBe(false);
    expect(isSyncValidatorSuccess(null)).toBe(false);
    expect(isSyncValidatorSuccess(undefined)).toBe(false);

    // a top-level $async schema is rejected at compile time and registers
    // nothing, so validation with that same schema object fails closed and
    // no Promise-returning validator can ever be invoked
    const asyncSchema = Object.freeze({
      type: "object",
      properties: { ok: { type: "boolean" } },
      required: ["ok"],
      $async: true,
    });
    expect(() => compilePipelineJsonSchema(asyncSchema, "probe")).toThrow(
      "probe compiles to an asynchronous JSON Schema validator; pipeline v2 supports synchronous validators only",
    );
    await expectReject(
      () => validatePipelineJson(asyncSchema, { ok: true }, "probe"),
      "probe requires the JSON schema snapshot compiled by loadPipelineV2; hand-built or uncompiled schemas are rejected",
    );

    // the defensively rejected async validator still compiles through Ajv,
    // so the same failure repeats identically on a second attempt
    expect(() => compilePipelineJsonSchema(asyncSchema, "probe")).toThrow(
      "probe compiles to an asynchronous JSON Schema validator; pipeline v2 supports synchronous validators only",
    );
  });
});
