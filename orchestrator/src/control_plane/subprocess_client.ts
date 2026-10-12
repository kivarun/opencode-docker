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
  | { readonly ok: true; readonly projection: OrchestratorRunProjection | OrchestratorInspectProjection }
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
