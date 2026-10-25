/**
 * The control-plane subprocess client: drives the existing Orchestrator
 * CLI as self-contained Bun child processes and converts its public JSON
 * protocol into narrow control-plane projections.
 *
 * Orchestration semantics stay exclusively inside the CLI child: this
 * module never reads the durable run-state document, never imports any
 * orchestration-internal module, and never re-classifies boundaries. It is
 * the single `Bun.spawn` owner of the control plane: no shell, one
 * child per call, concurrent bounded stdout/stderr
 * readers, stdin closed, no credential contents read or logged.
 *
 * The wire contract is the proven external contract of the CLI: exactly
 * one JSON document followed by exactly one newline on stdout, a required
 * `reason` from the closed public vocabulary on every nonzero exit, and
 * the versioned read-only `inspect` projection (schemaVersion 1). The raw
 * durable `state` document of a mutating outcome is verified as a
 * permitted runtime field and then dropped: it is never validated against
 * schema v7, never copied, never returned, never stored and never used
 * for routing.
 */
import { isCleanAbsolutePath } from "../clean_path.ts";
import { deepFreezeValue } from "../pipeline_v2_freeze_internal.ts";
import { isNonNegativeSafeInteger, isLowercaseSha256, isPipelineV2SafeId, isPositiveSafeInteger } from "../pipeline_v2_scalar.ts";
import { isPipelineV2PublicReason, type PipelineV2PublicReason } from "../pipeline_v2_public_reason.ts";
import {
  OrchestratorSubprocessClientError,
  routeOrchestratorCommand,
  type OrchestratorCommand,
  type OrchestratorRoutedCommand,
  type OrchestratorTransportReason,
} from "./routing.ts";

/** Any JSON value; used for the opaque inspect boundary/output projections. */
export type OrchestratorJsonValue = string | number | boolean | null | readonly OrchestratorJsonValue[] | { readonly [key: string]: OrchestratorJsonValue };

/** A JSON object with string keys. */
export type OrchestratorJsonObject = { readonly [key: string]: OrchestratorJsonValue };

/**
 * The immutable client configuration. It is captured and deep-frozen
 * exactly once at construction; later mutations of the caller's object
 * cannot influence any spawn. Every path must be a clean absolute path;
 * the environment is an explicit allow-list (the full `process.env` is
 * never inherited implicitly); limits and the SIGINT grace interval are
 * safe integers. A configuration that fails validation produces a typed
 * `invalid_client_config` error and no child is ever created.
 */
export interface OrchestratorSubprocessClientConfig {
  /** The Bun executable used to spawn the CLI child. */
  readonly bunExecutable: string;
  /** The absolute path of the orchestrator entrypoint (`main.ts`). */
  readonly orchestratorEntrypoint: string;
  /** The child working directory. */
  readonly workingDirectory: string;
  /** The profile config root passed to every command that accepts one. */
  readonly configRoot: string;
  /** Optional default pipeline root; `run` without one keeps the bundled default. */
  readonly pipelineRoot?: string;
  /** The exact child environment (allow-list policy owned by the caller). */
  readonly baseEnv: Readonly<Record<string, string>>;
  /** Hard byte limit for accumulating the child's stdout. */
  readonly stdoutLimitBytes: number;
  /** Hard byte limit forwarded/accumulated from the child's stderr. */
  readonly stderrLimitBytes: number;
  /** How long a SIGINT'd child may take to produce its outcome before SIGKILL. */
  readonly sigintGraceMs: number;
}

/**
 * The projected result of one mutating CLI outcome. The durable `state`
 * document is deliberately absent: the control-plane projection carries
 * only the run identity, the exit code, the outcome discriminant and the
 * public failure reason.
 */
export type OrchestratorRunProjection =
  | { readonly kind: "success"; readonly exitCode: 0; readonly runId: string; readonly runRoot: string }
  | { readonly kind: "waiting"; readonly exitCode: 0; readonly runId: string; readonly runRoot: string }
  | { readonly kind: "planReady"; readonly exitCode: 0; readonly runId: string; readonly runRoot: string }
  | {
      readonly kind: "responded";
      readonly exitCode: 0;
      readonly runId: string;
      readonly runRoot: string;
      readonly waitIndex: number;
      readonly actionId: string;
      readonly actionTo: string;
      readonly requestSha256: string;
      readonly responseSha256: string;
    }
  | {
      readonly kind: "failure";
      readonly exitCode: 1 | 130 | 143;
      readonly runId: string;
      readonly runRoot: string | null;
      readonly reason: PipelineV2PublicReason;
    };

/** The stable external status vocabulary of the inspect projection. */
export type OrchestratorInspectStatus = "running" | "waiting" | "succeeded" | "failed";

/**
 * The projected result of one `inspect` call: the exact schemaVersion-1
 * projection (boundary and outputs pass through as opaque frozen JSON —
 * the client never re-classifies a boundary), or the typed inspect
 * failure outcome of the CLI itself.
 */
export type OrchestratorInspectProjection =
  | {
      readonly kind: "inspect";
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly runRoot: string;
      readonly revision: number;
      readonly status: OrchestratorInspectStatus;
      readonly boundary: OrchestratorJsonObject;
      readonly outputs: readonly OrchestratorJsonValue[];
    }
  | {
      readonly kind: "inspectFailure";
      readonly exitCode: 1;
      readonly runId: string;
      readonly runRoot: string | null;
      readonly reason: string;
    };

/**
 * The projected result of one `events` call: the exact schemaVersion-1
 * events projection (the event entries pass through as opaque frozen
 * JSON — the client never re-classifies or re-orders them; the raw
 * `runRoot` is dropped before the HTTP layer), or the typed events
 * failure outcome of the CLI itself.
 */
export type OrchestratorEventsProjection =
  | {
      readonly kind: "events";
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly revision: number;
      readonly events: readonly OrchestratorJsonValue[];
      readonly nextAfter: number | null;
    }
  | {
      readonly kind: "eventsFailure";
      readonly exitCode: 1;
      readonly runId: string;
      readonly runRoot: string | null;
      readonly reason: string;
    };

/**
 * The projected result of one `logs` call: the versioned worker-logs
 * projection (the metadata union passes through as frozen JSON; the raw
 * `runRoot` is dropped before the HTTP layer; `dataBase64` is the only
 * content-bearing field and carries exactly the requested bounded chunk)
 * or the typed logs failure outcome of the CLI itself.
 */
export type OrchestratorLogsProjection =
  | {
      readonly kind: "logs";
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly revision: number;
      readonly logs: readonly OrchestratorJsonValue[];
    }
  | {
      readonly kind: "logsExecution";
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly revision: number;
      readonly executionIndex: number;
      readonly stateId: string;
      readonly status: "pending" | "unavailable";
    }
  | {
      readonly kind: "logsChunk";
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly revision: number;
      readonly executionIndex: number;
      readonly stateId: string;
      readonly status: "captured" | "truncated";
      readonly sha256: string;
      readonly byteCount: number;
      readonly truncatedDaemon: boolean;
      readonly truncatedLocal: boolean;
      readonly offset: number;
      readonly dataBase64: string;
      readonly nextOffset: number | null;
    }
  | {
      readonly kind: "logsFailure";
      readonly exitCode: 1;
      readonly runId: string;
      readonly runRoot: string | null;
      readonly reason: string;
    };

/**
 * The projected result of one `project_changes` call: the exact
 * schemaVersion-1 changes projection (the summary and the change entries
 * pass through as opaque frozen JSON — the client never re-classifies a
 * change; the raw `runRoot` is dropped before the HTTP layer), or the
 * typed changes failure outcome of the CLI itself.
 */
export type OrchestratorProjectChangesProjection =
  | {
      readonly kind: "projectChanges";
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly revision: number;
      readonly observation: string;
      readonly summary: OrchestratorJsonObject;
      readonly changes: readonly OrchestratorJsonValue[];
      readonly nextAfter: number | null;
    }
  | {
      readonly kind: "projectChangesFailure";
      readonly exitCode: 1;
      readonly runId: string;
      readonly runRoot: string | null;
      readonly reason: string;
    };

/**
 * The projected result of one `project_change_content` call: the exact
 * schemaVersion-1 content projection in one of the four closed success
 * statuses (the node passes through as opaque frozen JSON — the client
 * never re-classifies a node; only `dataBase64` carries bytes and the
 * raw `runRoot` is dropped before the HTTP layer), or the typed content
 * failure outcome of the CLI itself.
 */
export type OrchestratorProjectChangeContentProjection =
  | {
      readonly kind: "projectChangeContent";
      readonly schemaVersion: 1;
      readonly runId: string;
      readonly revision: number;
      readonly observation: string;
      readonly changeIndex: number;
      readonly side: string;
      readonly status: string;
      readonly byteCount?: number;
      readonly sha256?: string;
      readonly offset?: number;
      readonly dataBase64?: string;
      readonly nextOffset?: number | null;
      readonly reason?: string;
      readonly node?: OrchestratorJsonObject;
    }
  | {
      readonly kind: "projectChangeContentFailure";
      readonly exitCode: 1;
      readonly runId: string;
      readonly runRoot: string | null;
      readonly reason: string;
    };

/** A transport failure: not a durable run outcome, no stderr or body contents. */
export interface OrchestratorTransportFailure {
  readonly reason: OrchestratorTransportReason;
  /** The run id known to the caller when the failure happened (empty for anonymous runs). */
  readonly expectedRunId: string;
}

/**
 * The result of one client call. `ok: true` means the child produced a
 * valid CLI outcome document (which may itself be a failure outcome —
 * that is the projected Pipeline V2 outcome, not a transport failure);
 * `ok: false` is a transport failure and the caller should recover
 * through `inspect` when a run id was known.
 */
export type OrchestratorCallResult =
  | {
      readonly ok: true;
      readonly projection:
        | OrchestratorRunProjection
        | OrchestratorInspectProjection
        | OrchestratorEventsProjection
        | OrchestratorLogsProjection
        | OrchestratorProjectChangesProjection
        | OrchestratorProjectChangeContentProjection;
    }
  | { readonly ok: false; readonly failure: OrchestratorTransportFailure };

export interface OrchestratorCallOptions {
  /** Optional cancellation signal for mutating calls (see the client docs). */
  readonly signal?: AbortSignal;
}

interface CapturedClientConfig {
  readonly bunExecutable: string;
  readonly orchestratorEntrypoint: string;
  readonly workingDirectory: string;
  readonly configRoot: string;
  readonly pipelineRoot: string | null;
  readonly baseEnv: Readonly<Record<string, string>>;
  readonly stdoutLimitBytes: number;
  readonly stderrLimitBytes: number;
  readonly sigintGraceMs: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fail(reason: OrchestratorTransportReason, message: string, expectedRunId: string): OrchestratorCallResult {
  return { ok: false, failure: deepFreezeValue({ reason, expectedRunId }) };
}

function throwConfigError(message: string): never {
  throw new OrchestratorSubprocessClientError("invalid_client_config", message);
}

function captureClientConfig(config: unknown): CapturedClientConfig {
  if (!isRecord(config)) throwConfigError("client config must be an object");
  for (const key of Object.keys(config)) {
    if (
      key !== "bunExecutable" &&
      key !== "orchestratorEntrypoint" &&
      key !== "workingDirectory" &&
      key !== "configRoot" &&
      key !== "pipelineRoot" &&
      key !== "baseEnv" &&
      key !== "stdoutLimitBytes" &&
      key !== "stderrLimitBytes" &&
      key !== "sigintGraceMs"
    ) {
      throwConfigError("unknown field in client config");
    }
  }
  const expectPath = (key: string, what: string, required: boolean): string | null => {
    if (!Object.hasOwn(config, key)) {
      if (required) throwConfigError(`${what} is required`);
      return null;
    }
    const value = config[key];
    if (!isCleanAbsolutePath(value)) throwConfigError(`${what} must be a clean absolute path`);
    return value as string;
  };
  const bunExecutable = expectPath("bunExecutable", "bunExecutable", true) as string;
  const orchestratorEntrypoint = expectPath("orchestratorEntrypoint", "orchestratorEntrypoint", true) as string;
  const workingDirectory = expectPath("workingDirectory", "workingDirectory", true) as string;
  const configRoot = expectPath("configRoot", "configRoot", true) as string;
  const pipelineRoot = expectPath("pipelineRoot", "pipelineRoot", false);
  const baseEnvValue = config["baseEnv"];
  if (!isRecord(baseEnvValue)) throwConfigError("baseEnv must be a record of strings");
  for (const key of Object.keys(baseEnvValue)) {
    if (key === "" || key.includes("\0")) throwConfigError("baseEnv keys must be non-empty and free of NUL");
    const value = baseEnvValue[key];
    if (typeof value !== "string" || value.includes("\0")) throwConfigError("baseEnv values must be strings free of NUL");
  }
  const baseEnv: Record<string, string> = {};
  for (const key of Object.keys(baseEnvValue)) {
    baseEnv[key] = baseEnvValue[key] as string;
  }
  const expectLimit = (key: string, what: string): number => {
    if (!Object.hasOwn(config, key)) throwConfigError(`${what} is required`);
    const value = config[key];
    if (!isPositiveSafeInteger(value)) throwConfigError(`${what} must be a positive safe integer`);
    return value as number;
  };
  const stdoutLimitBytes = expectLimit("stdoutLimitBytes", "stdoutLimitBytes");
  const stderrLimitBytes = expectLimit("stderrLimitBytes", "stderrLimitBytes");
  const graceValue = config["sigintGraceMs"];
  if (!Object.hasOwn(config, "sigintGraceMs") || !isNonNegativeSafeInteger(graceValue)) {
    throwConfigError("sigintGraceMs must be a non-negative safe integer");
  }
  return deepFreezeValue({
    bunExecutable,
    orchestratorEntrypoint,
    workingDirectory,
    configRoot,
    pipelineRoot,
    baseEnv: deepFreezeValue(baseEnv),
    stdoutLimitBytes,
    stderrLimitBytes,
    sigintGraceMs: graceValue as number,
  });
}

const SUCCESS_KEYS = ["exitCode", "ok", "runId", "runRoot", "state"] as const;
const WAITING_KEYS = ["exitCode", "ok", "runId", "runRoot", "state", "waiting"] as const;
const PLAN_READY_KEYS = ["exitCode", "ok", "planReady", "runId", "runRoot", "state"] as const;
const FAILURE_KEYS = ["exitCode", "ok", "reason", "runId", "runRoot", "state"] as const;
const RESPONDED_KEYS = [
  "actionId",
  "actionTo",
  "exitCode",
  "ok",
  "requestSha256",
  "responseSha256",
  "runId",
  "runRoot",
  "state",
  "waitIndex",
] as const;
const INSPECT_REPORT_KEYS = [
  "boundary",
  "exitCode",
  "ok",
  "outputs",
  "revision",
  "runId",
  "runRoot",
  "schemaVersion",
  "status",
] as const;
const INSPECT_FAILURE_KEYS = ["exitCode", "ok", "reason", "runId", "runRoot"] as const;
const EVENTS_REPORT_KEYS = [
  "events",
  "exitCode",
  "nextAfter",
  "ok",
  "revision",
  "runId",
  "runRoot",
  "schemaVersion",
] as const;
const EVENTS_FAILURE_KEYS = ["exitCode", "ok", "reason", "runId", "runRoot"] as const;
const CHANGES_REPORT_KEYS = [
  "changes",
  "exitCode",
  "nextAfter",
  "observation",
  "ok",
  "revision",
  "runId",
  "runRoot",
  "schemaVersion",
  "summary",
] as const;
const CHANGES_FAILURE_KEYS = ["exitCode", "ok", "reason", "runId", "runRoot"] as const;
const CONTENT_FAILURE_KEYS = ["exitCode", "ok", "reason", "runId", "runRoot"] as const;
const CONTENT_COMMON_SUCCESS_KEYS = [
  "changeIndex",
  "exitCode",
  "observation",
  "ok",
  "revision",
  "runId",
  "runRoot",
  "schemaVersion",
  "side",
  "status",
] as const;
const CONTENT_AVAILABLE_KEYS = [...CONTENT_COMMON_SUCCESS_KEYS, "byteCount", "dataBase64", "nextOffset", "offset", "sha256"] as const;
const CONTENT_IMPLICIT_EMPTY_KEYS = [...CONTENT_COMMON_SUCCESS_KEYS, "byteCount", "dataBase64", "nextOffset", "offset"] as const;
const CONTENT_METADATA_ONLY_KEYS = [...CONTENT_COMMON_SUCCESS_KEYS, "node"] as const;
const CONTENT_UNAVAILABLE_KEYS = [...CONTENT_COMMON_SUCCESS_KEYS, "node", "reason"] as const;
const LOGS_LIST_KEYS = [
  "exitCode",
  "logs",
  "ok",
  "revision",
  "runId",
  "runRoot",
  "schemaVersion",
] as const;
const LOGS_STATUS_KEYS = [
  "exitCode",
  "executionIndex",
  "ok",
  "revision",
  "runId",
  "runRoot",
  "schemaVersion",
  "stateId",
  "status",
] as const;
const LOGS_CHUNK_KEYS = [
  "byteCount",
  "dataBase64",
  "exitCode",
  "executionIndex",
  "nextOffset",
  "offset",
  "ok",
  "revision",
  "runId",
  "runRoot",
  "schemaVersion",
  "sha256",
  "stateId",
  "status",
  "truncatedDaemon",
  "truncatedLocal",
] as const;
const LOGS_FAILURE_KEYS = ["exitCode", "ok", "reason", "runId", "runRoot"] as const;
const LOGS_METADATA_STATUS_KEYS = ["executionIndex", "stateId", "status"] as const;
const LOGS_METADATA_CONTENT_KEYS = [
  "byteCount",
  "executionIndex",
  "sha256",
  "stateId",
  "status",
  "truncatedDaemon",
  "truncatedLocal",
] as const;

function expectExactKeys(document: Record<string, unknown>, expected: readonly string[]): void {
  const actual = Object.keys(document);
  if (actual.length !== expected.length) {
    throw new OrchestratorSubprocessClientError("unexpected_output_shape", "outcome document has an unexpected own-key set");
  }
  for (const key of expected) {
    if (!Object.hasOwn(document, key)) {
      throw new OrchestratorSubprocessClientError("unexpected_output_shape", "outcome document has an unexpected own-key set");
    }
  }
}

function expectStringField(document: Record<string, unknown>, key: string): string {
  const value = document[key];
  if (typeof value !== "string") {
    throw new OrchestratorSubprocessClientError("unexpected_output_shape", `outcome field ${key} must be a string`);
  }
  return value;
}

function expectStateField(document: Record<string, unknown>): void {
  const value = document["state"];
  if (value !== null && !isRecord(value)) {
    throw new OrchestratorSubprocessClientError("unexpected_output_shape", "outcome field state must be null or an object");
  }
}

/**
 * The single wire parser owner. Validates one outcome document against
 * the exact expected shape for the process exit code and the command
 * kind, cross-checks the document exit code with the process exit code,
 * and returns the narrow projection. The raw `state` value is verified as
 * a permitted runtime field and never copied into the projection.
 */
function parseOutcomeDocument(
  text: string,
  exitCode: number,
  commandKind: string,
  expectedRunId: string,
): OrchestratorCallResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return fail("malformed_output", "child stdout is not exactly one JSON document followed by one newline", expectedRunId);
  }
  if (!isRecord(parsed)) {
    return fail("unexpected_output_shape", "child stdout JSON root must be an object", expectedRunId);
  }
  if (commandKind === "inspect") {
    if (exitCode === 1) {
      try {
        expectExactKeys(parsed, INSPECT_FAILURE_KEYS);
        if (parsed["ok"] !== false || parsed["exitCode"] !== 1) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect failure document has an unexpected shape");
        }
        const runId = expectStringField(parsed, "runId");
        const runRootValue = parsed["runRoot"];
        if (runRootValue !== null && typeof runRootValue !== "string") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect failure runRoot must be a string or null");
        }
        const reason = expectStringField(parsed, "reason");
        if (reason === "") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect failure reason must be a non-empty string");
        }
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "inspectFailure",
            exitCode: 1,
            runId,
            runRoot: runRootValue as string | null,
            reason,
          }),
        };
      } catch (cause) {
        if (cause instanceof OrchestratorSubprocessClientError) {
          return fail(cause.reason, cause.message, expectedRunId);
        }
        throw cause;
      }
    }
    try {
      expectExactKeys(parsed, INSPECT_REPORT_KEYS);
      if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect report has an unexpected shape");
      }
      if (parsed["schemaVersion"] !== 1) {
        throw new OrchestratorSubprocessClientError("unsupported_contract", "inspect projection schemaVersion must be 1");
      }
      const runId = expectStringField(parsed, "runId");
      const runRoot = expectStringField(parsed, "runRoot");
      if (!isPositiveSafeInteger(parsed["revision"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect revision must be a positive safe integer");
      }
      const status = parsed["status"];
      if (status !== "running" && status !== "waiting" && status !== "succeeded" && status !== "failed") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect status must be one of running/waiting/succeeded/failed");
      }
      if (!isRecord(parsed["boundary"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect boundary must be an object");
      }
      if (!Array.isArray(parsed["outputs"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "inspect outputs must be an array");
      }
      return {
        ok: true,
        projection: deepFreezeValue({
          kind: "inspect",
          schemaVersion: 1,
          runId,
          runRoot,
          revision: parsed["revision"] as number,
          status,
          boundary: parsed["boundary"] as OrchestratorJsonObject,
          outputs: deepFreezeValue(parsed["outputs"] as OrchestratorJsonValue[]),
        }),
      };
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
  if (commandKind === "events") {
    if (exitCode === 1) {
      try {
        expectExactKeys(parsed, EVENTS_FAILURE_KEYS);
        if (parsed["ok"] !== false || parsed["exitCode"] !== 1) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "events failure document has an unexpected shape");
        }
        const runId = expectStringField(parsed, "runId");
        const runRootValue = parsed["runRoot"];
        if (runRootValue !== null && typeof runRootValue !== "string") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "events failure runRoot must be a string or null");
        }
        const reason = expectStringField(parsed, "reason");
        if (reason === "") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "events failure reason must be a non-empty string");
        }
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "eventsFailure",
            exitCode: 1,
            runId,
            runRoot: runRootValue as string | null,
            reason,
          }),
        };
      } catch (cause) {
        if (cause instanceof OrchestratorSubprocessClientError) {
          return fail(cause.reason, cause.message, expectedRunId);
        }
        throw cause;
      }
    }
    try {
      expectExactKeys(parsed, EVENTS_REPORT_KEYS);
      if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "events report has an unexpected shape");
      }
      if (parsed["schemaVersion"] !== 1) {
        throw new OrchestratorSubprocessClientError("unsupported_contract", "events projection schemaVersion must be 1");
      }
      const runId = expectStringField(parsed, "runId");
      const runRoot = expectStringField(parsed, "runRoot");
      if (!isPositiveSafeInteger(parsed["revision"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "events revision must be a positive safe integer");
      }
      if (!Array.isArray(parsed["events"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "events must be an array");
      }
      const nextAfterValue = parsed["nextAfter"];
      if (
        nextAfterValue !== null &&
        (typeof nextAfterValue !== "number" || !Number.isSafeInteger(nextAfterValue) || nextAfterValue < 0)
      ) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "events nextAfter must be a non-negative safe integer or null");
      }
      // The raw run root never reaches the HTTP layer: the projection
      // carries the run identity and the events page only.
      return {
        ok: true,
        projection: deepFreezeValue({
          kind: "events",
          schemaVersion: 1,
          runId,
          revision: parsed["revision"] as number,
          events: deepFreezeValue(parsed["events"] as OrchestratorJsonValue[]),
          nextAfter: nextAfterValue as number | null,
        }),
      };
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
  if (commandKind === "project_changes") {
    if (exitCode === 1) {
      try {
        expectExactKeys(parsed, CHANGES_FAILURE_KEYS);
        if (parsed["ok"] !== false || parsed["exitCode"] !== 1) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes failure document has an unexpected shape");
        }
        const runId = expectStringField(parsed, "runId");
        const runRootValue = parsed["runRoot"];
        if (runRootValue !== null && typeof runRootValue !== "string") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes failure runRoot must be a string or null");
        }
        const reason = expectStringField(parsed, "reason");
        if (reason === "") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes failure reason must be a non-empty string");
        }
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "projectChangesFailure",
            exitCode: 1,
            runId,
            runRoot: runRootValue as string | null,
            reason,
          }),
        };
      } catch (cause) {
        if (cause instanceof OrchestratorSubprocessClientError) {
          return fail(cause.reason, cause.message, expectedRunId);
        }
        throw cause;
      }
    }
    try {
      expectExactKeys(parsed, CHANGES_REPORT_KEYS);
      if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes report has an unexpected shape");
      }
      if (parsed["schemaVersion"] !== 1) {
        throw new OrchestratorSubprocessClientError("unsupported_contract", "project-changes projection schemaVersion must be 1");
      }
      const runId = expectStringField(parsed, "runId");
      expectStringField(parsed, "runRoot");
      if (!isPositiveSafeInteger(parsed["revision"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes revision must be a positive safe integer");
      }
      const observation = parsed["observation"];
      if (typeof observation !== "string" || !/^[0-9a-f]{64}$/.test(observation)) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes observation must be a 64-character lowercase hexadecimal digest");
      }
      if (!isRecord(parsed["summary"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes summary must be an object");
      }
      if (!Array.isArray(parsed["changes"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes changes must be an array");
      }
      const nextAfterValue = parsed["nextAfter"];
      if (
        nextAfterValue !== null &&
        (typeof nextAfterValue !== "number" || !Number.isSafeInteger(nextAfterValue) || nextAfterValue < 0)
      ) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-changes nextAfter must be a non-negative safe integer or null");
      }
      // The raw run root never reaches the HTTP layer: the projection
      // carries the run identity, the observation identity and the page
      // only.
      return {
        ok: true,
        projection: deepFreezeValue({
          kind: "projectChanges",
          schemaVersion: 1,
          runId,
          revision: parsed["revision"] as number,
          observation,
          summary: parsed["summary"] as OrchestratorJsonObject,
          changes: deepFreezeValue(parsed["changes"] as OrchestratorJsonValue[]),
          nextAfter: nextAfterValue as number | null,
        }),
      };
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
  if (commandKind === "project_change_content") {
    if (exitCode === 1) {
      try {
        expectExactKeys(parsed, CONTENT_FAILURE_KEYS);
        if (parsed["ok"] !== false || parsed["exitCode"] !== 1) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content failure document has an unexpected shape");
        }
        const runId = expectStringField(parsed, "runId");
        const runRootValue = parsed["runRoot"];
        if (runRootValue !== null && typeof runRootValue !== "string") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content failure runRoot must be a string or null");
        }
        const reason = expectStringField(parsed, "reason");
        if (reason === "") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content failure reason must be a non-empty string");
        }
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "projectChangeContentFailure",
            exitCode: 1,
            runId,
            runRoot: runRootValue as string | null,
            reason,
          }),
        };
      } catch (cause) {
        if (cause instanceof OrchestratorSubprocessClientError) {
          return fail(cause.reason, cause.message, expectedRunId);
        }
        throw cause;
      }
    }
    try {
      const status = parsed["status"];
      if (
        typeof status !== "string" ||
        !(status === "available" || status === "implicit_empty" || status === "metadata_only" || status === "unavailable")
      ) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content status must be one of the four closed statuses");
      }
      const keys =
        status === "available"
          ? CONTENT_AVAILABLE_KEYS
          : status === "implicit_empty"
            ? CONTENT_IMPLICIT_EMPTY_KEYS
            : status === "metadata_only"
              ? CONTENT_METADATA_ONLY_KEYS
              : CONTENT_UNAVAILABLE_KEYS;
      expectExactKeys(parsed, keys);
      if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content outcome has an unexpected shape");
      }
      if (parsed["schemaVersion"] !== 1) {
        throw new OrchestratorSubprocessClientError("unsupported_contract", "project-change-content projection schemaVersion must be 1");
      }
      const runId = expectStringField(parsed, "runId");
      const contentRunRoot = expectStringField(parsed, "runRoot");
      void contentRunRoot;
      if (!isPositiveSafeInteger(parsed["revision"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content revision must be a positive safe integer");
      }
      const observation = parsed["observation"];
      if (typeof observation !== "string" || !/^[0-9a-f]{64}$/.test(observation)) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content observation must be a 64-character lowercase hexadecimal digest");
      }
      if (!isPositiveSafeInteger(parsed["changeIndex"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content changeIndex must be a positive safe integer");
      }
      const side = parsed["side"];
      if (side !== "baseline" && side !== "result") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content side must be 'baseline' or 'result'");
      }
      // The raw run root never reaches the HTTP layer; only `dataBase64`
      // carries bytes.
      if (status === "available") {
        const sha256 = expectStringField(parsed, "sha256");
        if (!isLowercaseSha256(sha256)) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content sha256 must be a lowercase SHA-256 digest");
        }
        const byteCount = parsed["byteCount"];
        const offset = parsed["offset"];
        const nextOffsetValue = parsed["nextOffset"];
        if (typeof byteCount !== "number" || !Number.isSafeInteger(byteCount) || byteCount < 0) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content byteCount must be a non-negative safe integer");
        }
        if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content offset must be a non-negative safe integer");
        }
        if (
          nextOffsetValue !== null &&
          (typeof nextOffsetValue !== "number" || !Number.isSafeInteger(nextOffsetValue) || nextOffsetValue < 0)
        ) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content nextOffset must be a non-negative safe integer or null");
        }
        const dataBase64 = expectStringField(parsed, "dataBase64");
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "projectChangeContent",
            schemaVersion: 1,
            runId,
            revision: parsed["revision"] as number,
            observation,
            changeIndex: parsed["changeIndex"] as number,
            side,
            status,
            byteCount,
            sha256,
            offset,
            dataBase64,
            nextOffset: nextOffsetValue as number | null,
          }),
        };
      }
      if (status === "implicit_empty") {
        if (parsed["byteCount"] !== 0 || parsed["offset"] !== 0 || parsed["dataBase64"] !== "" || parsed["nextOffset"] !== null) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content implicit_empty cursor fields have an unexpected shape");
        }
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "projectChangeContent",
            schemaVersion: 1,
            runId,
            revision: parsed["revision"] as number,
            observation,
            changeIndex: parsed["changeIndex"] as number,
            side,
            status,
            byteCount: 0,
            offset: 0,
            dataBase64: "",
            nextOffset: null,
          }),
        };
      }
      if (!isRecord(parsed["node"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content node must be an object");
      }
      if (status === "metadata_only") {
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "projectChangeContent",
            schemaVersion: 1,
            runId,
            revision: parsed["revision"] as number,
            observation,
            changeIndex: parsed["changeIndex"] as number,
            side,
            status,
            node: parsed["node"] as OrchestratorJsonObject,
          }),
        };
      }
      if (parsed["reason"] !== "source_no_longer_matches_baseline") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "project-change-content unavailable reason has an unexpected value");
      }
      return {
        ok: true,
        projection: deepFreezeValue({
          kind: "projectChangeContent",
          schemaVersion: 1,
          runId,
          revision: parsed["revision"] as number,
          observation,
          changeIndex: parsed["changeIndex"] as number,
          side,
          status,
          reason: parsed["reason"] as string,
          node: parsed["node"] as OrchestratorJsonObject,
        }),
      };
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
  if (commandKind === "logs") {
    if (exitCode === 1) {
      try {
        expectExactKeys(parsed, LOGS_FAILURE_KEYS);
        if (parsed["ok"] !== false || parsed["exitCode"] !== 1) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs failure document has an unexpected shape");
        }
        const runId = expectStringField(parsed, "runId");
        const runRootValue = parsed["runRoot"];
        if (runRootValue !== null && typeof runRootValue !== "string") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs failure runRoot must be a string or null");
        }
        const reason = expectStringField(parsed, "reason");
        if (reason === "") {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs failure reason must be a non-empty string");
        }
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "logsFailure",
            exitCode: 1,
            runId,
            runRoot: runRootValue as string | null,
            reason,
          }),
        };
      } catch (cause) {
        if (cause instanceof OrchestratorSubprocessClientError) {
          return fail(cause.reason, cause.message, expectedRunId);
        }
        throw cause;
      }
    }
    try {
      if (parsed["schemaVersion"] !== 1) {
        throw new OrchestratorSubprocessClientError("unsupported_contract", "logs projection schemaVersion must be 1");
      }
      const runId = expectStringField(parsed, "runId");
      const runRoot = expectStringField(parsed, "runRoot");
      if (!isPositiveSafeInteger(parsed["revision"])) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs revision must be a positive safe integer");
      }
      const revision = parsed["revision"] as number;
      const hasLogs = Object.hasOwn(parsed, "logs");
      const hasExecutionIndex = Object.hasOwn(parsed, "executionIndex");
      if (hasLogs === hasExecutionIndex) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs outcome has an unexpected shape");
      }
      if (hasLogs) {
        expectExactKeys(parsed, LOGS_LIST_KEYS);
        if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs outcome has an unexpected shape");
        }
        if (!Array.isArray(parsed["logs"])) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs must be an array");
        }
        for (const entry of parsed["logs"] as unknown[]) {
          if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
            throw new OrchestratorSubprocessClientError("unexpected_output_shape", "every logs entry must be an object");
          }
          const record = entry as Record<string, unknown>;
          const status = record["status"];
          if (status !== "pending" && status !== "unavailable" && status !== "captured" && status !== "truncated") {
            throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs entry status must be a known worker-log status");
          }
          const expectedKeys = status === "captured" || status === "truncated" ? LOGS_METADATA_CONTENT_KEYS : LOGS_METADATA_STATUS_KEYS;
          expectExactKeys(record, expectedKeys);
          if (!isPositiveSafeInteger(record["executionIndex"])) {
            throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs entry executionIndex must be a positive safe integer");
          }
          if (typeof record["stateId"] !== "string" || record["stateId"] === "") {
            throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs entry stateId must be a non-empty string");
          }
          if (status === "captured" || status === "truncated") {
            if (typeof record["sha256"] !== "string" || !isLowercaseSha256(record["sha256"])) {
              throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs entry sha256 must be a lowercase SHA-256 digest");
            }
            if (typeof record["byteCount"] !== "number" || !Number.isSafeInteger(record["byteCount"]) || record["byteCount"] < 0) {
              throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs entry byteCount must be a non-negative safe integer");
            }
            if (typeof record["truncatedDaemon"] !== "boolean" || typeof record["truncatedLocal"] !== "boolean") {
              throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs entry truncation flags must be booleans");
            }
          }
        }
        // The raw run root never reaches the HTTP layer: the projection
        // carries the run identity and the metadata list only.
        void runRoot;
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "logs",
            schemaVersion: 1,
            runId,
            revision,
            logs: deepFreezeValue(parsed["logs"] as OrchestratorJsonValue[]),
          }),
        };
      }
      const executionIndexValue = parsed["executionIndex"];
      if (!isPositiveSafeInteger(executionIndexValue)) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs executionIndex must be a positive safe integer");
      }
      const stateId = expectStringField(parsed, "stateId");
      if (stateId === "") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs stateId must be a non-empty string");
      }
      const status = parsed["status"];
      if (status === "pending" || status === "unavailable") {
        expectExactKeys(parsed, LOGS_STATUS_KEYS);
        if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs outcome has an unexpected shape");
        }
        void runRoot;
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "logsExecution",
            schemaVersion: 1,
            runId,
            revision,
            executionIndex: executionIndexValue,
            stateId,
            status,
          }),
        };
      }
      if (status !== "captured" && status !== "truncated") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs status must be a known worker-log status");
      }
      expectExactKeys(parsed, LOGS_CHUNK_KEYS);
      if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs outcome has an unexpected shape");
      }
      const sha256 = expectStringField(parsed, "sha256");
      if (!isLowercaseSha256(sha256)) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs sha256 must be a lowercase SHA-256 digest");
      }
      const byteCount = parsed["byteCount"];
      const offset = parsed["offset"];
      const nextOffsetValue = parsed["nextOffset"];
      if (typeof byteCount !== "number" || !Number.isSafeInteger(byteCount) || byteCount < 0) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs byteCount must be a non-negative safe integer");
      }
      if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs offset must be a non-negative safe integer");
      }
      if (
        nextOffsetValue !== null &&
        (typeof nextOffsetValue !== "number" || !Number.isSafeInteger(nextOffsetValue) || nextOffsetValue < 0)
      ) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs nextOffset must be a non-negative safe integer or null");
      }
      if (typeof parsed["truncatedDaemon"] !== "boolean" || typeof parsed["truncatedLocal"] !== "boolean") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "logs truncation flags must be booleans");
      }
      const dataBase64 = expectStringField(parsed, "dataBase64");
      // The raw run root never reaches the HTTP layer; the chunk carries
      // the base64 content only.
      void runRoot;
      return {
        ok: true,
        projection: deepFreezeValue({
          kind: "logsChunk",
          schemaVersion: 1,
          runId,
          revision,
          executionIndex: executionIndexValue,
          stateId,
          status,
          sha256,
          byteCount,
          truncatedDaemon: parsed["truncatedDaemon"] as boolean,
          truncatedLocal: parsed["truncatedLocal"] as boolean,
          offset,
          dataBase64,
          nextOffset: nextOffsetValue as number | null,
        }),
      };
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
  if (exitCode === 0) {
    try {
      if (commandKind === "respond") {
        expectExactKeys(parsed, RESPONDED_KEYS);
        if (parsed["ok"] !== true || parsed["exitCode"] !== 0) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "respond outcome has an unexpected shape");
        }
        const runId = expectStringField(parsed, "runId");
        const runRoot = expectStringField(parsed, "runRoot");
        if (!isPositiveSafeInteger(parsed["waitIndex"])) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "respond waitIndex must be a positive safe integer");
        }
        const actionId = expectStringField(parsed, "actionId");
        const actionTo = expectStringField(parsed, "actionTo");
        const requestSha256 = expectStringField(parsed, "requestSha256");
        const responseSha256 = expectStringField(parsed, "responseSha256");
        if (!isPipelineV2SafeId(actionId) || !isPipelineV2SafeId(actionTo)) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "respond action ids must be safe identifiers");
        }
        if (!isLowercaseSha256(requestSha256) || !isLowercaseSha256(responseSha256)) {
          throw new OrchestratorSubprocessClientError("unexpected_output_shape", "respond digests must be lowercase SHA-256");
        }
        expectStateField(parsed);
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "responded",
            exitCode: 0,
            runId,
            runRoot,
            waitIndex: parsed["waitIndex"] as number,
            actionId,
            actionTo,
            requestSha256,
            responseSha256,
          }),
        };
      }
      if (parsed["ok"] === true) {
        expectExactKeys(parsed, SUCCESS_KEYS);
        if (parsed["exitCode"] !== 0) {
          throw new OrchestratorSubprocessClientError("exit_code_mismatch", "outcome exit code does not match the process exit code");
        }
        expectStateField(parsed);
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "success",
            exitCode: 0,
            runId: expectStringField(parsed, "runId"),
            runRoot: expectStringField(parsed, "runRoot"),
          }),
        };
      }
      if (parsed["waiting"] === true) {
        expectExactKeys(parsed, WAITING_KEYS);
        if (parsed["exitCode"] !== 0) {
          throw new OrchestratorSubprocessClientError("exit_code_mismatch", "outcome exit code does not match the process exit code");
        }
        expectStateField(parsed);
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "waiting",
            exitCode: 0,
            runId: expectStringField(parsed, "runId"),
            runRoot: expectStringField(parsed, "runRoot"),
          }),
        };
      }
      if (parsed["planReady"] === true) {
        expectExactKeys(parsed, PLAN_READY_KEYS);
        if (parsed["exitCode"] !== 0) {
          throw new OrchestratorSubprocessClientError("exit_code_mismatch", "outcome exit code does not match the process exit code");
        }
        expectStateField(parsed);
        return {
          ok: true,
          projection: deepFreezeValue({
            kind: "planReady",
            exitCode: 0,
            runId: expectStringField(parsed, "runId"),
            runRoot: expectStringField(parsed, "runRoot"),
          }),
        };
      }
      throw new OrchestratorSubprocessClientError("unexpected_output_shape", "outcome document has an unexpected shape");
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
  if (exitCode === 1 || exitCode === 130 || exitCode === 143) {
    try {
      expectExactKeys(parsed, FAILURE_KEYS);
      if (parsed["ok"] !== false || parsed["exitCode"] !== exitCode) {
        throw new OrchestratorSubprocessClientError("exit_code_mismatch", "outcome exit code does not match the process exit code");
      }
      const runId = expectStringField(parsed, "runId");
      const runRootValue = parsed["runRoot"];
      if (runRootValue !== null && typeof runRootValue !== "string") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "outcome runRoot must be a string or null");
      }
      const reason = expectStringField(parsed, "reason");
      if (!isPipelineV2PublicReason(reason)) {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "outcome reason is outside the closed public vocabulary");
      }
      if (exitCode === 130 && reason !== "signal_sigint") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "exit 130 must carry the signal_sigint reason");
      }
      if (exitCode === 143 && reason !== "signal_sigterm") {
        throw new OrchestratorSubprocessClientError("unexpected_output_shape", "exit 143 must carry the signal_sigterm reason");
      }
      expectStateField(parsed);
      return {
        ok: true,
        projection: deepFreezeValue({
          kind: "failure",
          exitCode,
          runId,
          runRoot: runRootValue as string | null,
          reason,
        }),
      };
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
  throw new OrchestratorSubprocessClientError("child_terminated", `child exited with unexpected code ${exitCode}`);
}

/** Splits the raw stdout bytes into the exact one-document-plus-newline wire form. */
function extractDocumentText(stdout: Uint8Array): string {
  if (stdout.length === 0) {
    throw new OrchestratorSubprocessClientError("malformed_output", "child stdout is empty");
  }
  if (stdout.at(-1) !== 0x0a) {
    throw new OrchestratorSubprocessClientError("malformed_output", "child stdout must end with exactly one newline");
  }
  const text = new TextDecoder().decode(stdout.subarray(0, stdout.length - 1));
  if (text.length === 0) {
    throw new OrchestratorSubprocessClientError("malformed_output", "child stdout must be exactly one JSON document");
  }
  if (text.includes("\n")) {
    throw new OrchestratorSubprocessClientError("malformed_output", "child stdout must contain exactly one JSON document");
  }
  return text;
}

/**
 * The strict self-subprocess client. One call spawns exactly one child
 * (`Bun.spawn`, no shell), reads stdout and stderr concurrently under
 * hard byte limits (draining continues past the limit so the child can
 * never deadlock on a full pipe), enforces the exact JSON wire contract
 * and returns narrow projections without the raw durable state.
 *
 * Cancellation is isolated per call through an optional `AbortSignal`:
 * an abort before the spawn creates no child; the first abort after the
 * spawn delivers exactly one SIGINT to this call's child only; after the
 * configured grace interval exactly one SIGKILL follows and the call
 * resolves as a `cancellation_forced` transport failure (the caller then
 * recovers through `inspect`). A child that produces its signal outcome
 * within the grace window returns it as an ordinary projected Pipeline V2
 * failure (exit 130/143). No process-global signal handler is installed.
 */
export class OrchestratorSubprocessClient {
  private readonly captured: CapturedClientConfig;

  constructor(config: OrchestratorSubprocessClientConfig) {
    this.captured = captureClientConfig(config);
  }

  async run(command: OrchestratorCommand, options?: OrchestratorCallOptions): Promise<OrchestratorCallResult> {
    const captured = this.captured;
    let routed: OrchestratorRoutedCommand;
    try {
      routed = routeOrchestratorCommand(command, { configRoot: captured.configRoot, pipelineRoot: captured.pipelineRoot ?? undefined });
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, "");
      }
      throw cause;
    }
    const expectedRunId = routed.expectedRunId;
    const signal = options?.signal;
    if (signal !== undefined && signal.aborted) {
      return fail("cancellation_forced", "aborted before spawn; no child was created", expectedRunId);
    }
    let proc: {
      kill(signal?: number | "SIGINT" | "SIGTERM" | "SIGKILL" | "SIGABRT" | "SIGHUP" | "SIGQUIT" | "SIGTRAP" | "SIGBUS" | "SIGFPE" | "SIGSEGV" | "SIGUSR2" | "SIGPIPE" | "SIGALRM" | "SIGCHLD" | "SIGCONT" | "SIGTSTP" | "SIGTTIN" | "SIGTTOU" | "SIGURG" | "SIGXCPU" | "SIGXFSZ" | "SIGVTALRM" | "SIGPROF" | "SIGWINCH" | "SIGIO" | "SIGPWR" | "SIGSYS"): void;
      exited: Promise<number>;
      stdout: ReadableStream<Uint8Array>;
      stderr: ReadableStream<Uint8Array>;
    };
    try {
      proc = Bun.spawn({
        cmd: [captured.bunExecutable, captured.orchestratorEntrypoint, ...routed.argv],
        cwd: captured.workingDirectory,
        env: { ...captured.baseEnv },
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      });
    } catch {
      return fail("spawn_failed", "the orchestrator child process could not be spawned", expectedRunId);
    }
    let sigintSent = false;
    let forced = false;
    let graceTimer: ReturnType<typeof setTimeout> | null = null;
    // The abort listener is installed before the pre-spawn re-check so an
    // abort racing the spawn window still reaches the child exactly once.
    const onAbort = (): void => {
      if (sigintSent) return;
      sigintSent = true;
      try {
        proc.kill("SIGINT");
      } catch {
        // the child is already gone; the outcome wins
      }
      graceTimer = setTimeout(() => {
        forced = true;
        try {
          proc.kill("SIGKILL");
        } catch {
          // the child is already gone; the outcome wins
        }
      }, captured.sigintGraceMs);
    };
    if (signal !== undefined) {
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) {
        onAbort();
      }
    }
    let stdoutBytes = 0;
    let stdoutOverflow = false;
    const stdoutChunks: Uint8Array[] = [];
    const stdoutReader = proc.stdout.getReader();
    const stdoutDone = (async (): Promise<void> => {
      for (;;) {
        const { done, value } = await stdoutReader.read();
        if (done || value === undefined) break;
        const length = value.byteLength;
        if (stdoutOverflow) continue;
        if (stdoutBytes + length > captured.stdoutLimitBytes) {
          stdoutOverflow = true;
          continue;
        }
        stdoutBytes += length;
        stdoutChunks.push(value);
      }
    })();
    let stderrBytes = 0;
    let stderrOverflow = false;
    const stderrReader = proc.stderr.getReader();
    const stderrDone = (async (): Promise<void> => {
      for (;;) {
        const { done, value } = await stderrReader.read();
        if (done || value === undefined) break;
        if (stderrOverflow) continue;
        stderrBytes += value.byteLength;
        if (stderrBytes > captured.stderrLimitBytes) {
          stderrOverflow = true;
          continue;
        }
        process.stderr.write(value);
      }
    })();
    const exitCodeValue = await proc.exited;
    await Promise.all([stdoutDone, stderrDone]);
    if (graceTimer !== null) {
      clearTimeout(graceTimer);
      graceTimer = null;
    }
    if (signal !== undefined) {
      signal.removeEventListener("abort", onAbort);
    }
    if (forced) {
      return fail("cancellation_forced", "the child was force-killed after the SIGINT grace interval; recover through inspect", expectedRunId);
    }
    const exitCode = typeof exitCodeValue === "number" ? exitCodeValue : -1;
    if (exitCode === 2) {
      return fail("command_rejected", "the CLI rejected the invocation (exit 2)", expectedRunId);
    }
    if (exitCode !== 0 && exitCode !== 1 && exitCode !== 130 && exitCode !== 143) {
      return fail("child_terminated", `the child terminated with unexpected exit code ${exitCode}`, expectedRunId);
    }
    if (stdoutOverflow) {
      return fail("stdout_too_large", "the child stdout exceeded the configured byte limit", expectedRunId);
    }
    if (stderrOverflow) {
      return fail("stderr_too_large", "the child stderr exceeded the configured byte limit", expectedRunId);
    }
    let documentText: string;
    try {
      documentText = extractDocumentText(concatChunks(stdoutChunks, stdoutBytes));
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
    try {
      return parseOutcomeDocument(documentText, exitCode, (command as { kind: string }).kind, expectedRunId);
    } catch (cause) {
      if (cause instanceof OrchestratorSubprocessClientError) {
        return fail(cause.reason, cause.message, expectedRunId);
      }
      throw cause;
    }
  }
}

function concatChunks(chunks: readonly Uint8Array[], total: number): Uint8Array {
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}
