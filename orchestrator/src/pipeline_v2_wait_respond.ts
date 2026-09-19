/**
 * Production wait-response command for pipeline schema version 2: records
 * the user's answer to one durably open wait through the existing
 * production-neutral wait controller — and nothing else.
 *
 * This command is deliberately not a pipeline runner and must not look
 * like one: it loads no pipeline bundle, no profiles, no Launcher
 * credential, no Docker Helper configuration, no LLM credentials, and it
 * registers no signal handler. It never creates a Session, never spawns a
 * subprocess, and never talks to the Docker Helper daemon. The durable
 * state and the run-owned wait manifests are the only inputs; the routing
 * target is derived exclusively from the durable request's own action
 * declaration.
 *
 * The recorded response is only the durable answer: it moves the run from
 * `waiting` back to `active` at the declared action's target state and
 * never continues the pipeline. Continuation is the separate existing
 * production command `orchestrator resume`; there is no respond-and-resume
 * and no automatic retry anywhere.
 *
 * Production ordering (fixed, fail-closed; every step before the
 * controller call is read-only):
 *
 *   1. shape validation of the options and dependencies (the run id
 *      against the shared v6 safe-id grammar, the wait index as a positive
 *      safe integer, the action id as a safe id);
 *   2. the trusted state-root projection (resolved by the CLI through the
 *      same environment resolver `run` and `resume` use; this module never
 *      reads the environment);
 *   3. read-only verification of the existing local/daemon state roots
 *      (real non-symlink directories that are the same object by dev/ino;
 *      nothing is created and nothing is chmodded);
 *   4. the fixed `<state-root>/pipeline-runs/<run-id>` layout;
 *   5. read-only verification of the local/daemon run-root projection as
 *      one real canonical object with exactly mode 0700;
 *   6. the read-only `PipelineV2RunStateSink.open` (the single state
 *      validator; a missing or foreign document is a typed refusal);
 *   7. the normalized durable snapshot (the opened sink guarantees it);
 *   8. `recordPipelineV2WaitAction` — the single internal response chain
 *      shared with the raw-response API: durable request reconstruction,
 *      request-file verification or idempotent restoration, acceptance
 *      through the manifest module, response publication, and the durable
 *      `wait_response_recorded` dispatch.
 *
 * On every step before the controller call nothing is created, nothing is
 * chmodded, no temporary file exists, no durable dispatch happens, and no
 * auth/Session/subprocess is touched; the run tree is byte-identical
 * across all pre-controller refusals.
 *
 * Failure contract (closed, typed; classified by error classes and typed
 * fields, never by message text): `invalid_options` for a hostile
 * options/deps shape, `missing_state` when no durable state document
 * exists for the run id, `run_layout_invalid` for a missing/unsafe/
 * mismatched state-root or run-root layout or projection, the controller's
 * own reasons (`invalid_state`, `invalid_response`, `wait_conflict`,
 * `wait_storage_failed`, `state_persist_failed`) verbatim, and
 * `internal_error` for unexpected programmer errors (propagated causes are
 * never masked as expected user-facing failures and never leak their
 * text into the reason). Diagnostics are content-free: no manifest
 * bodies, action values, digests of foreign content, prompts, facts,
 * credentials or parser fragments ever reach them.
 *
 * Known limitation (documented, unchanged): there is no multi-process
 * locking. Concurrent responders race under the wait store's
 * exclusive-link idempotency (one wins, the loser conflicts) and under the
 * sink's in-process dispatch serialization; the durability-unknown
 * adoption poisons the loaded sink so no further dispatch can happen in
 * this process.
 *
 * The result is deep-frozen and content-free: no raw or canonical JSON, no
 * manifest-file paths, and no arbitrary caller values ever appear on it.
 */
import { join } from "node:path";
import { lstat } from "node:fs/promises";
import { isCleanAbsolutePath } from "./clean_path.ts";
import type { PipelineV2StateRootProjection } from "./pipeline_v2_runner.ts";
import {
  expectSafeId,
  type PipelineV2RunState,
} from "./pipeline_v2_state.ts";
import { PipelineV2RunStateSink } from "./pipeline_v2_state_sink.ts";
import {
  inspectProjectionObject,
  sameProjectionIdentity,
  translateProjectionPath,
} from "./projection_fs.ts";
import { isPositiveSafeInteger } from "./pipeline_v2_scalar.ts";
import type { PipelineStateIo } from "./pipeline_state_store.ts";
import {
  PipelineV2WaitControllerError,
  recordPipelineV2WaitAction,
  type PipelineV2WaitControllerSink,
} from "./pipeline_v2_wait_controller.ts";

export interface PipelineV2WaitResponseOptions {
  /** The run id of the durably waiting run (safe-id validated). */
  readonly runId: string;
  /** The wait journal index of the open wait (positive safe integer). */
  readonly waitIndex: number;
  /** One action id declared by that wait's request; never a target. */
  readonly actionId: string;
}

/**
 * The minimal trusted runtime configuration of the response command: the
 * same state-root projection shape the runner consumes, resolved by the
 * CLI's shared environment resolver, plus the store's optional injected
 * clock. There is deliberately no credential, no Docker Helper CLI, no
 * helper configuration, no profile loading, no signal lifecycle and no
 * configuration, no profile loading and no signal lifecycle; the only
 * seams are the store's own optional injected clock and IO (test fault
 * injection through the existing `PipelineStateIo` abstraction).
 */
export interface PipelineV2WaitResponseDeps {
  readonly stateRootProjection: PipelineV2StateRootProjection;
  readonly now?: () => Date;
  /** The store's existing IO seam (tests inject deterministic faults). */
  readonly io?: PipelineStateIo;
}

export type PipelineV2WaitResponseFailureReason =
  | "invalid_options"
  | "missing_state"
  | "run_layout_invalid"
  | "invalid_state"
  | "invalid_response"
  | "wait_conflict"
  | "wait_storage_failed"
  | "state_persist_failed"
  | "internal_error";

export type PipelineV2WaitResponseOutcome =
  | {
      readonly ok: true;
      readonly exitCode: 0;
      readonly runId: string;
      readonly runRoot: string;
      readonly waitIndex: number;
      readonly actionId: string;
      readonly actionTo: string;
      readonly requestSha256: string;
      readonly responseSha256: string;
      readonly state: PipelineV2RunState;
    }
  | {
      readonly ok: false;
      readonly exitCode: 1;
      readonly runId: string;
      readonly runRoot: string | null;
      readonly state: PipelineV2RunState | null;
      readonly reason: PipelineV2WaitResponseFailureReason;
    };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const item of value) deepFreeze(item);
    return Object.freeze(value) as T;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      deepFreeze(record[key]);
    }
    return Object.freeze(record) as unknown as T;
  }
  return value;
}

/**
 * The run-directory mode belongs to the orchestrator-owned layout: an
 * existing run root must carry exactly mode 0700. The check never chmods —
 * an unsafe existing directory is refused unchanged.
 */
async function assertRunDirectoryMode0700(path: string): Promise<void> {
  const info = await lstat(path);
  const mode = info.mode & 0o7777;
  if (mode !== 0o700) {
    throw new Error(
      `pipeline v2 wait response: the run directory ${JSON.stringify(path)} must have mode 0700, found 0${mode.toString(8)}`,
    );
  }
}

/**
 * Records the user's answer to one durably open wait. See the module
 * documentation for the fixed read-only preflight order, the failure
 * contract and the durability semantics.
 */
export async function respondPipelineV2Wait(
  options: PipelineV2WaitResponseOptions,
  deps: PipelineV2WaitResponseDeps,
): Promise<PipelineV2WaitResponseOutcome> {
  // --- shape validation (pure; no filesystem, no auth, no subprocess) ----
  let runId = "";
  let validated: { runId: string; waitIndex: number; actionId: string } | null = null;
  try {
    if (!isRecord(options)) {
      throw new Error("pipeline v2 wait response options must be an object");
    }
    if (!isRecord(deps)) {
      throw new Error("pipeline v2 wait response deps must be an object");
    }
    if (!isRecord(deps.stateRootProjection)) {
      throw new Error("pipeline v2 wait response deps.stateRootProjection must be an object");
    }
    for (const field of ["localRoot", "daemonRoot"] as const) {
      if (!isCleanAbsolutePath(deps.stateRootProjection[field])) {
        throw new Error(
          `pipeline v2 wait response deps.stateRootProjection.${field} must be an absolute clean path`,
        );
      }
    }
    if (deps.now !== undefined && typeof deps.now !== "function") {
      throw new Error("pipeline v2 wait response deps.now must be a function");
    }
    runId = typeof options.runId === "string" ? options.runId : "";
    expectSafeId(options.runId, "pipeline v2 wait response run id");
    if (!isPositiveSafeInteger(options.waitIndex)) {
      throw new Error("pipeline v2 wait response options.waitIndex must be a positive safe integer");
    }
    expectSafeId(options.actionId, "pipeline v2 wait response action id");
    validated = {
      runId: options.runId,
      waitIndex: options.waitIndex,
      actionId: options.actionId,
    };
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    console.error(`orchestrator: pipeline v2 wait response failed: ${message}`);
    // An unvalidated run id is never echoed into the outcome.
    return deepFreeze<PipelineV2WaitResponseOutcome>({
      ok: false,
      exitCode: 1,
      runId: "",
      runRoot: null,
      state: null,
      reason: "invalid_options",
    });
  }

  const preflightFailure = (cause: unknown): PipelineV2WaitResponseOutcome => {
    const message = cause instanceof Error ? cause.message : String(cause);
    console.error(`orchestrator: pipeline v2 wait response failed: ${message}`);
    return deepFreeze<PipelineV2WaitResponseOutcome>({
      ok: false,
      exitCode: 1,
      runId: validated!.runId,
      runRoot: null,
      state: null,
      reason: "run_layout_invalid",
    });
  };

  // --- read-only verification of the existing layout ----------------------
  const stateRootProjection = deps.stateRootProjection;
  const localStateRoot = stateRootProjection.localRoot;
  const daemonStateRoot = stateRootProjection.daemonRoot;
  const localRootInfo = await inspectProjectionObject(localStateRoot, "directory");
  if (localRootInfo.failure !== null || localRootInfo.identity === null) {
    return preflightFailure(
      new Error(
        `pipeline v2 wait response: the local state root ${JSON.stringify(localStateRoot)} is not a real non-symlink directory`,
      ),
    );
  }
  const daemonRootInfo = await inspectProjectionObject(daemonStateRoot, "directory");
  if (daemonRootInfo.failure !== null || daemonRootInfo.identity === null) {
    return preflightFailure(
      new Error(
        `pipeline v2 wait response: the daemon state root ${JSON.stringify(daemonStateRoot)} is not a real non-symlink directory`,
      ),
    );
  }
  if (!sameProjectionIdentity(localRootInfo.identity, daemonRootInfo.identity)) {
    return preflightFailure(
      new Error(
        "pipeline v2 wait response: the local and daemon state roots are not the same directory object (dev/ino differ)",
      ),
    );
  }
  const localRunRoot = join(localStateRoot, "pipeline-runs", validated.runId);
  const runRootInfo = await inspectProjectionObject(localRunRoot, "directory");
  if (runRootInfo.failure !== null || runRootInfo.identity === null) {
    return preflightFailure(
      new Error(
        `pipeline v2 wait response: ${JSON.stringify(localRunRoot)} is not a real non-symlink directory`,
      ),
    );
  }
  try {
    await assertRunDirectoryMode0700(localRunRoot);
  } catch (cause) {
    return preflightFailure(cause);
  }
  // The inspection succeeded, so the run root canonically resolves to
  // itself: the inspected local path is the canonical run root.
  const canonicalRunRoot = localRunRoot;
  const translation = translateProjectionPath(localStateRoot, daemonStateRoot, localRunRoot);
  if (!translation.ok) {
    return preflightFailure(
      new Error(
        `pipeline v2 wait response: the run root ${JSON.stringify(localRunRoot)} has no clean projection suffix under the state root`,
      ),
    );
  }
  const daemonRunRoot = translation.daemonPath;
  const daemonRunRootInfo = await inspectProjectionObject(daemonRunRoot, "directory");
  if (
    daemonRunRootInfo.failure !== null ||
    daemonRunRootInfo.identity === null ||
    !sameProjectionIdentity(runRootInfo.identity, daemonRunRootInfo.identity)
  ) {
    return preflightFailure(
      new Error(
        `pipeline v2 wait response: the daemon-side run root ${JSON.stringify(daemonRunRoot)} is not the same real object as ${JSON.stringify(localRunRoot)} (dev/ino differ)`,
      ),
    );
  }

  // --- sink open (read-only), then the single response chain --------------
  let sink: PipelineV2RunStateSink;
  try {
    sink = await PipelineV2RunStateSink.open({
      stateRoot: localStateRoot,
      runId: validated.runId,
      io: deps.io,
      now: deps.now,
    });
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    console.error(`orchestrator: pipeline v2 wait response failed: ${message}`);
    return deepFreeze<PipelineV2WaitResponseOutcome>({
      ok: false,
      exitCode: 1,
      runId: validated.runId,
      runRoot: canonicalRunRoot,
      state: null,
      reason: "missing_state",
    });
  }

  const controllerSink: PipelineV2WaitControllerSink = sink;
  let recorded: Awaited<ReturnType<typeof recordPipelineV2WaitAction>>;
  try {
    recorded = await recordPipelineV2WaitAction({
      runRoot: canonicalRunRoot,
      sink: controllerSink,
      waitIndex: validated.waitIndex,
      actionId: validated.actionId,
    });
  } catch (cause) {
    if (cause instanceof PipelineV2WaitControllerError) {
      // `invalid_request` belongs to the enter-wait operation and is
      // unreachable when recording a response; the closed outcome reason
      // set maps that impossible case to `internal_error`.
      const reason: PipelineV2WaitResponseFailureReason =
        cause.reason === "invalid_request" ? "internal_error" : cause.reason;
      const message = cause.message;
      console.error(`orchestrator: pipeline v2 wait response failed: ${message}`);
      return deepFreeze<PipelineV2WaitResponseOutcome>({
        ok: false,
        exitCode: 1,
        runId: validated.runId,
        runRoot: canonicalRunRoot,
        state: cause.state,
        reason,
      });
    }
    const message = cause instanceof Error ? cause.message : String(cause);
    console.error(`orchestrator: pipeline v2 wait response failed: ${message}`);
    return deepFreeze<PipelineV2WaitResponseOutcome>({
      ok: false,
      exitCode: 1,
      runId: validated.runId,
      runRoot: canonicalRunRoot,
      state: sink.snapshot,
      reason: "internal_error",
    });
  }

  return deepFreeze<PipelineV2WaitResponseOutcome>({
    ok: true,
    exitCode: 0,
    runId: validated.runId,
    runRoot: canonicalRunRoot,
    waitIndex: recorded.wait_index,
    actionId: recorded.action_id,
    actionTo: recorded.action_to,
    requestSha256: recorded.request_sha256,
    responseSha256: recorded.response_sha256,
    state: recorded.state,
  });
}
