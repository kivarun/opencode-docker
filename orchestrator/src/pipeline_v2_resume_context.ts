import { basename, isAbsolute } from "node:path";
import { lstat, realpath } from "node:fs/promises";
import type { Stats } from "node:fs";
import { isErrnoException } from "./fs_checks.ts";
import { PipelineError } from "./pipeline.ts";
import { requireResolvedPipelineV2Provenance, type ResolvedPipelineV2 } from "./pipeline_v2.ts";
import {
  PipelineV2StateError,
  validatePipelineV2RunState,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import { pipelineV2RunPipelineIdentity } from "./pipeline_v2_digest.ts";
import { PipelineV2RuntimeError } from "./pipeline_v2_runtime_error.ts";
import {
  mintRestoredRunInputsSnapshot,
  verifyRestoredAcceptedHistory,
  type AcceptedStateOutput,
  type RunInputSnapshotExpectation,
  type RunInputsSnapshot,
} from "./pipeline_v2_runtime.ts";

/**
 * Read-only restoration of the pipeline v2 runtime context (unwired).
 *
 * This module rebuilds, from the trusted resolved pipeline, the durable
 * state schema v6 and the fixed orchestrator-owned `<runRoot>`, exactly
 * the runtime objects a future resume needs to continue execution: a
 * provenance-backed `RunInputsSnapshot`, the full accepted
 * `AcceptedStateOutput[]` history, the durable cursor and the next global
 * execution index. The original user input bindings and the project
 * source are never read again — after the run started they no longer exist
 * as a source of truth; the fixed orchestrator-owned copies are.
 *
 * The whole API is strictly read-only: nothing is created, written,
 * chmodded, linked, renamed or removed, no activation leaf is reserved or
 * created, no Session is created, no sink is called, wait manifests are
 * not touched. Production resume, the coordinator, the runner and the CLI
 * stay unwired.
 *
 * Trust boundary and order (fail-closed):
 *
 *   1. `requireResolvedPipelineV2Provenance` — before any other action:
 *      the `state` is not read, the `runRoot` is not touched and no
 *      filesystem call happens before this gate, and a Proxy pipeline
 *      causes no getter or trap invocation;
 *   2. `validatePipelineV2RunState` — the single state validator; the
 *      result is the normalized deep-frozen snapshot the context carries;
 *   3. the resumable boundary check (a narrower resume policy on top of
 *      the validated state, never a second state validator);
 *   4. the pipeline identity comparison against
 *      `pipelineV2RunPipelineIdentity` (exact: nested schema version,
 *      canonical bundle root, execution snapshot digest, entry state,
 *      transition budget) plus the run-id binding
 *      `basename(runRoot) === state.run_id`;
 *   5. the run-root layout: `<runRoot>` absolute, canonical, real
 *      non-symlink directory; `project`, `data` and `data/inputs` real
 *      non-symlink directories canonically resolving to their declared
 *      paths inside the canonical run root, with `data` and
 *      `data/inputs` keeping the orchestrator-owned mode 0700 (`project`
 *      is worker-writable, so its content and mode are never fixed and
 *      never repaired);
 *   6. the run-input snapshot restoration: the durable inputs must match
 *      the declared pipeline inputs exactly (count, declaration order,
 *      id, type, protected flag, digest), every fixed object
 *      `<runRoot>/data/inputs/<id>` is verified with the data-plane
 *      contract (kind, canonical identity, containment, clean trees, the
 *      exact `pipeline-v2-input` digest) plus a full JSON re-parse and
 *      revalidation against the declaring input's compiled schema — a
 *      matching digest alone never mints provenance — and only then is
 *      the exact snapshot built and registered in the same module-private
 *      provenance registry `snapshotRunInputs` uses (no second registry;
 *      hand-built, cloned, spread, `structuredClone`d or proxied
 *      look-alikes keep failing the existing runtime gates);
 *   7. the accepted-history reconstruction from `state.executions` only:
 *      for every agent execution with durable outputs the record set must
 *      exactly equal the declared output ports of that state in
 *      declaration order (decision executions add nothing); every record
 *      — including old, non-winning ones — is validated through the
 *      single existing accepted-history chain (fixed paths, kinds,
 *      canonical containment, the exact `pipeline-v2-output` digest
 *      framing, and full JSON re-parse plus revalidation against the
 *      declaring output port's compiled schema);
 *   8. only after the full success the deep-frozen context is returned.
 *
 * Resumable boundaries of this increment (clean boundaries without
 * unfinished work only):
 *
 *   - active: `status: "active"`, `phase: "running"`, no terminal, run
 *     outputs or failure, `executions.length === transitions.length`,
 *     every execution settled (agent `cleanup_completed`, decision
 *     `evaluated`) and therefore bound to its committed transition, and
 *     the last wait (if any) already carries its response. This includes
 *     the initial state right after `create_run` with an empty history;
 *   - waiting: `status: "waiting"`, `phase: "waiting"`, the last wait
 *     record open, all executions settled and bound, no terminal, run
 *     outputs or failure. The context of a waiting run is restorable, but
 *     the restore itself continues nothing.
 *
 * Rejected (typed `invalid_state`): any in-flight agent or decision
 * execution, a settled execution without its `transition_committed`,
 * `publishing_outputs`, a reached terminal, the `success`, `failed` and
 * `cleanup_failed` final states, and an incoherent wait/cursor/history
 * shape.
 *
 * `next_execution_index` is `executions.length + 1` — the global index
 * agent activations share with decision executions, so activation
 * directory numbers may skip after a decision. No future activation leaf
 * is checked or reserved here.
 *
 * Failure contract (closed, typed, classified by validation phase — never
 * by message text): `PipelineV2RuntimeContextRestoreError` carries the
 * immutable `reason` — `invalid_state` (schema v6 validation or a
 * non-resumable boundary), `pipeline_mismatch` (durable pipeline/input/
 * output declaration metadata or run-id binding mismatch),
 * `run_layout_invalid` (run root/project/data/inputs structure),
 * `run_input_modified` (a fixed input object, kind, digest, JSON or
 * schema) or `accepted_output_modified` (a fixed accepted output object,
 * kind, digest, JSON or schema). Unexpected programmer errors propagate
 * unchanged and are never masked. Diagnostics are content-free: no file
 * or JSON bodies, no facts, prompts, env values or credentials, no
 * original user paths, and no malformed-JSON parser fragments.
 */

export type PipelineV2RuntimeContextRestoreFailureReason =
  | "invalid_state"
  | "pipeline_mismatch"
  | "run_layout_invalid"
  | "run_input_modified"
  | "accepted_output_modified";

export class PipelineV2RuntimeContextRestoreError extends Error {
  readonly reason: PipelineV2RuntimeContextRestoreFailureReason;

  constructor(reason: PipelineV2RuntimeContextRestoreFailureReason, message: string) {
    super(message);
    this.name = "PipelineV2RuntimeContextRestoreError";
    this.reason = reason;
  }
}

export interface RestoredPipelineV2RuntimeContext {
  readonly state: PipelineV2RunState;
  readonly run_inputs: RunInputsSnapshot;
  readonly accepted_outputs: readonly AcceptedStateOutput[];
  readonly cursor: {
    readonly current_state: string;
    readonly transition_count: number;
  };
  readonly next_execution_index: number;
}

function restoreError(
  reason: PipelineV2RuntimeContextRestoreFailureReason,
  message: string,
): PipelineV2RuntimeContextRestoreError {
  return new PipelineV2RuntimeContextRestoreError(reason, message);
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const entry of value) {
      deepFreeze(entry);
    }
    Object.freeze(value);
    return value;
  }
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) {
      deepFreeze(child);
    }
    Object.freeze(value);
  }
  return value;
}

function describeKind(info: Stats): string {
  return info.isSymbolicLink()
    ? "a symbolic link"
    : info.isDirectory()
      ? "a directory"
      : info.isFile()
        ? "a regular file"
        : info.isFIFO()
          ? "a FIFO"
          : info.isSocket()
            ? "a unix socket"
            : "an unexpected object";
}

/**
 * Structural layout check of one fixed restore path: an existing real
 * non-symlink directory that canonically resolves to exactly its own
 * declared path (canonical identity), optionally with the orchestrator
 * mode enforced. Never created, chmodded, repaired or removed.
 */
async function requireRestoreDirectory(
  path: string,
  what: string,
  mode: number | undefined,
): Promise<void> {
  let info: Stats;
  try {
    info = await lstat(path);
  } catch (cause) {
    if (isErrnoException(cause, "ENOENT")) {
      throw restoreError("run_layout_invalid", `${what} does not exist`);
    }
    throw restoreError("run_layout_invalid", `${what} could not be inspected`);
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw restoreError("run_layout_invalid", `${what} exists but is ${describeKind(info)}`);
  }
  if (mode !== undefined && (info.mode & 0o7777) !== mode) {
    throw restoreError("run_layout_invalid", `${what} does not have the required mode ${mode.toString(8)}`);
  }
  let canonical: string;
  try {
    canonical = await realpath(path);
  } catch {
    throw restoreError("run_layout_invalid", `${what} cannot be canonicalized`);
  }
  if (canonical !== path) {
    throw restoreError("run_layout_invalid", `${what} does not resolve to its canonical path`);
  }
}

/**
 * The resumable-boundary check: a narrower resume policy on top of the
 * already validated state. It re-derives nothing the loader guarantees
 * beyond its own narrow question — which clean boundaries carry no
 * unfinished work.
 */
function checkResumableBoundary(state: PipelineV2RunState): void {
  const fail = (message: string): PipelineV2RuntimeContextRestoreError =>
    restoreError("invalid_state", message);
  const clean =
    state.terminal === undefined && state.run_outputs === undefined && state.failure === undefined;
  const settled = state.executions.every((execution) =>
    execution.type === "agent"
      ? execution.phase === "cleanup_completed"
      : execution.phase === "evaluated",
  );
  const bound = state.executions.length === state.transitions.length;
  const last = state.waits[state.waits.length - 1];
  if (state.status === "active" && state.phase === "running") {
    if (!clean) {
      throw fail("the run carries a terminal, run outputs or a failure; it is not resumable");
    }
    if (!settled) {
      throw fail("the run carries an in-flight or failed execution; it is not resumable");
    }
    if (!bound) {
      throw fail("the run carries a settled execution without its committed transition; it is not resumable");
    }
    if (last !== undefined && last.response === undefined) {
      throw fail("the last wait record is still open; an active run is not resumable");
    }
    return;
  }
  if (state.status === "waiting" && state.phase === "waiting") {
    if (!clean) {
      throw fail("a waiting run must not carry a terminal, run outputs or a failure");
    }
    if (!settled) {
      throw fail("a waiting run carries an in-flight or failed execution; it is not resumable");
    }
    if (!bound) {
      throw fail("a waiting run carries a settled execution without its committed transition; it is not resumable");
    }
    if (last === undefined || last.response !== undefined) {
      throw fail("a waiting run must carry an open last wait record");
    }
    return;
  }
  throw fail(
    "the run is not on a resumable clean boundary; only an active running run without unfinished work or a waiting run is restorable",
  );
}

/**
 * The exact pipeline identity and run-id binding: every identity field
 * must match `pipelineV2RunPipelineIdentity` and the run root's basename
 * must be the durable run id.
 */
function checkPipelineIdentity(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
  runRoot: string,
): void {
  const expected = pipelineV2RunPipelineIdentity(pipeline);
  const actual = state.pipeline;
  const mismatch = (what: string): PipelineV2RuntimeContextRestoreError =>
    restoreError("pipeline_mismatch", `the durable run state was created for a different pipeline: ${what}`);
  if (actual.schema_version !== expected.schema_version) {
    throw mismatch("the pipeline schema version differs");
  }
  if (actual.bundle_root !== expected.bundle_root) {
    throw mismatch("the canonical bundle root differs");
  }
  if (actual.execution_snapshot_sha256 !== expected.execution_snapshot_sha256) {
    throw mismatch("the execution snapshot digest differs");
  }
  if (actual.entry_state !== expected.entry_state) {
    throw mismatch("the entry state differs");
  }
  if (actual.max_transitions !== expected.max_transitions) {
    throw mismatch("the transition budget differs");
  }
  if (basename(runRoot) !== state.run_id) {
    throw restoreError(
      "pipeline_mismatch",
      "the run root does not belong to this run",
    );
  }
}

interface DeclaredPipelineStateRef {
  readonly id: string;
  readonly type: string;
  readonly outputIds: readonly string[];
}

function declaredStates(pipeline: ResolvedPipelineV2): Map<string, DeclaredPipelineStateRef> {
  const map = new Map<string, DeclaredPipelineStateRef>();
  for (const state of pipeline.states) {
    map.set(state.id, {
      id: state.id,
      type: state.type,
      outputIds: state.type === "agent" ? state.outputs.map((port) => port.id) : [],
    });
  }
  return map;
}

/**
 * Reconstruct the accepted-history records from `state.executions` only.
 * Every agent execution's durable output set must exactly equal the
 * declared output ports of its state in declaration order; decision
 * executions add nothing. The record carries no type, path or summary —
 * the fixed location and declared type are derived during validation.
 */
function reconstructAcceptedRecords(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
): AcceptedStateOutput[] {
  const declared = declaredStates(pipeline);
  const records: AcceptedStateOutput[] = [];
  for (const execution of state.executions) {
    if (execution.type === "decision") {
      continue;
    }
    const pipelineState = declared.get(execution.state_id);
    if (pipelineState === undefined || pipelineState.type !== "agent") {
      throw restoreError(
        "pipeline_mismatch",
        `the durable run state records an execution for ${JSON.stringify(execution.state_id)} which the trusted pipeline does not declare as an agent state`,
      );
    }
    const declaredIds = pipelineState.outputIds;
    const durableOutputs = execution.outputs;
    if (durableOutputs === undefined) {
      if (declaredIds.length > 0) {
        throw restoreError(
          "invalid_state",
          `the settled agent execution for ${JSON.stringify(execution.state_id)} records no accepted outputs while the state declares output ports`,
        );
      }
      continue;
    }
    const durableIds = durableOutputs.map((output) => output.id);
    const exact =
      durableIds.length === declaredIds.length &&
      durableIds.every((id, index) => id === declaredIds[index]);
    if (!exact) {
      throw restoreError(
        "pipeline_mismatch",
        `the durable accepted outputs of agent state ${JSON.stringify(execution.state_id)} do not match the declared output ports in declaration order`,
      );
    }
    for (const output of durableOutputs) {
      records.push({
        state: execution.state_id,
        output: output.id,
        activation_index: execution.index,
        digest: output.digest,
      });
    }
  }
  return records;
}

/**
 * Validate the durable inputs against the declared pipeline inputs and
 * build the exact restoration expectations, carrying each declaring
 * input's compiled schema for the JSON revalidation.
 */
function buildRunInputExpectations(
  pipeline: ResolvedPipelineV2,
  state: PipelineV2RunState,
): RunInputSnapshotExpectation[] {
  if (state.inputs.length !== pipeline.inputs.length) {
    throw restoreError(
      "pipeline_mismatch",
      "the durable run state records a different number of pipeline inputs",
    );
  }
  return state.inputs.map((durable, index) => {
    const declared = pipeline.inputs[index];
    if (declared === undefined || durable.id !== declared.id) {
      throw restoreError(
        "pipeline_mismatch",
        "the durable run state inputs do not match the declared pipeline inputs in declaration order",
      );
    }
    if (durable.type !== declared.type) {
      throw restoreError(
        "pipeline_mismatch",
        `pipeline input ${JSON.stringify(durable.id)} declares a different type in the durable state`,
      );
    }
    if (durable.protected !== declared.protected) {
      throw restoreError(
        "pipeline_mismatch",
        `pipeline input ${JSON.stringify(durable.id)} declares a different protection flag in the durable state`,
      );
    }
    return {
      id: durable.id,
      type: durable.type,
      protected: durable.protected,
      digest: durable.digest,
      declaredSchema: declared.schema,
    };
  });
}

/**
 * Map one typed data-plane failure of the restoration helpers onto the
 * restore contract by reason only. Runtime reasons outside the restore
 * set are unexpected programmer errors and propagate unchanged.
 */
function mapTypedRuntimeFailure(cause: PipelineV2RuntimeError): PipelineV2RuntimeContextRestoreError | undefined {
  switch (cause.reason) {
    case "run_input_modified":
      return restoreError("run_input_modified", cause.message);
    case "accepted_output_modified":
      return restoreError("accepted_output_modified", cause.message);
    default:
      return undefined;
  }
}

export async function restorePipelineV2RuntimeContext(
  pipeline: ResolvedPipelineV2,
  state: unknown,
  runRoot: string,
): Promise<RestoredPipelineV2RuntimeContext> {
  // 1. The trust gate is the very first action: no state read, no run-root
  //    access, no filesystem call, no Proxy evaluation before it.
  requireResolvedPipelineV2Provenance(pipeline, "pipeline v2 runtime context restoration");
  // 2. The single state validator produces the normalized deep-frozen
  //    snapshot the context carries.
  let validated: PipelineV2RunState;
  try {
    validated = validatePipelineV2RunState(state);
  } catch (cause) {
    if (cause instanceof PipelineV2StateError) {
      throw restoreError("invalid_state", "the durable run state is not a valid state schema v6 document");
    }
    throw cause;
  }
  // 3. The narrower resumable-boundary policy.
  checkResumableBoundary(validated);
  // 4. The exact pipeline identity plus the run-id binding.
  checkPipelineIdentity(pipeline, validated, runRoot);
  // 5. The run-root layout, strictly read-only.
  if (typeof runRoot !== "string" || !isAbsolute(runRoot)) {
    throw restoreError("run_layout_invalid", "the run root must be an absolute path");
  }
  await requireRestoreDirectory(runRoot, "the run root", undefined);
  const runRootCanonical = runRoot;
  const projectRoot = `${runRootCanonical}/project`;
  await requireRestoreDirectory(projectRoot, "the run project directory", undefined);
  const projectRootCanonical = await realpath(projectRoot);
  const dataRoot = `${runRootCanonical}/data`;
  await requireRestoreDirectory(dataRoot, "the run data directory", 0o700);
  const inputsRoot = `${dataRoot}/inputs`;
  await requireRestoreDirectory(inputsRoot, "the run inputs directory", 0o700);
  // 6. The provenance-backed run-input snapshot restoration.
  let runInputs: RunInputsSnapshot;
  try {
    runInputs = await mintRestoredRunInputsSnapshot(
      pipeline,
      runRootCanonical,
      projectRootCanonical,
      buildRunInputExpectations(pipeline, validated),
    );
  } catch (cause) {
    if (cause instanceof PipelineV2RuntimeError) {
      const mapped = mapTypedRuntimeFailure(cause);
      if (mapped !== undefined) {
        throw mapped;
      }
    }
    throw cause;
  }
  // 7.+8. The accepted-history reconstruction and its full validation
  //    through the single existing chain — fixed paths, kinds, digests and
  //    JSON schemas of every record, including old non-winning ones.
  const acceptedOutputs = reconstructAcceptedRecords(pipeline, validated);
  try {
    await verifyRestoredAcceptedHistory(pipeline, runRootCanonical, acceptedOutputs);
  } catch (cause) {
    if (cause instanceof PipelineV2RuntimeError) {
      const mapped = mapTypedRuntimeFailure(cause);
      if (mapped !== undefined) {
        throw mapped;
      }
    }
    if (cause instanceof PipelineV2StateError) {
      throw restoreError("invalid_state", "the durable accepted history is not coherent");
    }
    if (cause instanceof PipelineError) {
      // Record-shape or coherence failures of a restoration-built history
      // mean forged durable state, not a data-plane modification.
      throw restoreError("invalid_state", "the durable accepted history is not coherent with the trusted pipeline");
    }
    throw cause;
  }
  // 9. The deep-frozen, content-free result. The cursor is a copy of the
  //    validated durable cursor; the next execution index counts every
  //    execution (agent activations share the global index with decision
  //    executions, so activation directory numbers may skip).
  return deepFreeze({
    state: validated,
    run_inputs: runInputs,
    accepted_outputs: acceptedOutputs,
    cursor: {
      current_state: validated.cursor.current_state,
      transition_count: validated.cursor.transition_count,
    },
    next_execution_index: validated.executions.length + 1,
  });
}
