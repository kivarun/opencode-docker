/**
 * Typed command union and the single argv-routing owner for the
 * control-plane subprocess client.
 *
 * The routing layer is the ONLY place that turns a typed control-plane
 * command into orchestrator CLI argv. It never spawns a process, never
 * touches the filesystem, never selects policy (stage, budget, action)
 * and never interprets inspect boundaries. Executable and entrypoint
 * paths belong to the client config and are never part of the returned
 * argv; `--json` is always appended; the argument order is deterministic.
 *
 * Command values are validated here against the existing shared scalar
 * predicates (the same safe-id grammar and safe-integer predicates used
 * by the durable state layer) and the shared clean-absolute-path
 * predicate, so an unsafe value is rejected before any spawn. Command
 * objects are validated with exact own-key sets per kind: hostile or
 * unknown extra fields are rejected without ever reading their values,
 * so internal or deprecated CLI flags cannot be expressed through this
 * layer. Values with spaces, quotes, `$()`, backticks or `=` stay single
 * argv elements — there is no interpolation and no shell.
 */
import { isCleanAbsolutePath } from "../clean_path.ts";
import { isPipelineV2SafeId, isPositiveSafeInteger } from "../pipeline_v2_scalar.ts";

/** One typed run input binding; routing renders each as `--input id=path`. */
export interface OrchestratorRunInput {
  readonly id: string;
  readonly path: string;
}

/**
 * The closed discriminated union of control-plane commands. Every field
 * maps one-to-one onto the existing CLI grammar; the caller always passes
 * an already-chosen run id (anonymous `run` omits it and the CLI generates
 * one). No raw argv fragments, no arbitrary env, no executable, config,
 * state or helper paths are expressible: deployment configuration lives in
 * the immutable client config, per-run data lives in the typed fields.
 */
export type OrchestratorCommand =
  | {
      readonly kind: "run";
      readonly runId?: string;
      readonly pipelineRoot?: string;
      readonly projectPath: string;
      readonly inputs: readonly OrchestratorRunInput[];
      readonly launcherId?: string;
    }
  | { readonly kind: "inspect"; readonly runId: string }
  | {
      readonly kind: "events";
      readonly runId: string;
      readonly after?: number;
      readonly limit?: number;
    }
  | { readonly kind: "resume"; readonly runId: string; readonly launcherId?: string }
  | {
      readonly kind: "resume_plan";
      readonly runId: string;
      readonly stageId: string;
      readonly initialBudget: number;
      readonly launcherId?: string;
    }
  | {
      readonly kind: "continue_stage";
      readonly runId: string;
      readonly waitIndex: number;
      readonly additionalIterations: number;
      readonly launcherId?: string;
    }
  | {
      readonly kind: "revise_task";
      readonly runId: string;
      readonly waitIndex: number;
      readonly taskId: string;
      readonly taskFile: string;
      readonly launcherId?: string;
    }
  | {
      readonly kind: "respond";
      readonly runId: string;
      readonly waitIndex: number;
      readonly actionId: string;
    };

/** Server-side routing configuration: which config root and optional default pipeline root to route with. */
export interface OrchestratorRoutingConfig {
  readonly configRoot: string;
  readonly pipelineRoot?: string;
}

/**
 * The closed transport-failure vocabulary of the subprocess client. These
 * reasons are deliberately disjoint from `PipelineV2PublicReason`: a
 * transport failure is not a durable run outcome. Whenever a run id was
 * already known and the child may have started working, the caller is
 * expected to recover through `inspect` with that run id.
 */
export type OrchestratorTransportReason =
  | "invalid_client_config"
  | "invalid_command"
  | "spawn_failed"
  | "command_rejected"
  | "stdout_too_large"
  | "stderr_too_large"
  | "malformed_output"
  | "unexpected_output_shape"
  | "exit_code_mismatch"
  | "unsupported_contract"
  | "child_terminated"
  | "cancellation_forced";

/**
 * The single closed transport-error class of the control-plane substrate.
 * Diagnostics are content-free: they never contain stderr, file bodies,
 * task/input contents or child output values. The reason is an immutable
 * own property; hostile error objects cannot rewrite it.
 */
export class OrchestratorSubprocessClientError extends Error {
  readonly reason!: OrchestratorTransportReason;

  constructor(reason: OrchestratorTransportReason, message: string) {
    super(message);
    this.name = "OrchestratorSubprocessClientError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** The result of routing: the exact child argv plus the known run id (empty when the command carries none). */
export interface OrchestratorRoutedCommand {
  readonly argv: readonly string[];
  readonly expectedRunId: string;
}

interface CapturedCommand {
  readonly kind: string;
  readonly runId: string;
  readonly pipelineRoot: string | null;
  readonly projectPath: string;
  readonly inputs: readonly { id: string; path: string }[];
  readonly launcherId: string | null;
  readonly stageId: string;
  readonly initialBudget: number;
  readonly waitIndex: number;
  readonly additionalIterations: number;
  readonly taskId: string;
  readonly taskFile: string;
  readonly actionId: string;
  readonly after: number | null;
  readonly limit: number | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function expectSafeIdField(record: Record<string, unknown>, key: string, what: string): string {
  const value = record[key];
  if (!isPipelineV2SafeId(value)) {
    throw new OrchestratorSubprocessClientError("invalid_command", `${what} must be a safe identifier`);
  }
  return value;
}

function expectPositiveIntegerField(record: Record<string, unknown>, key: string, what: string): number {
  const value = record[key];
  if (!isPositiveSafeInteger(value)) {
    throw new OrchestratorSubprocessClientError("invalid_command", `${what} must be a positive safe integer`);
  }
  return value;
}

function expectNonNegativeIntegerField(record: Record<string, unknown>, key: string, what: string): number {
  const value = record[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new OrchestratorSubprocessClientError("invalid_command", `${what} must be a non-negative safe integer`);
  }
  return value;
}

function expectCleanPathField(record: Record<string, unknown>, key: string, what: string): string {
  const value = record[key];
  if (!isCleanAbsolutePath(value)) {
    throw new OrchestratorSubprocessClientError("invalid_command", `${what} must be a clean absolute path`);
  }
  return value;
}

/**
 * Validates one typed run-input binding with an exact own-key set and
 * returns its two fields read exactly once each.
 */
/**
 * Reads one optional command field exactly once. An own property with the
 * value `undefined` is treated as absent (the natural optional-field
 * semantics); any other value is returned for validation.
 */
function optionalField(record: Record<string, unknown>, key: string): { present: boolean; value: unknown } {
  if (!Object.hasOwn(record, key)) return { present: false, value: undefined };
  const value = record[key];
  if (value === undefined) return { present: false, value: undefined };
  return { present: true, value };
}

function expectRunInput(value: unknown, index: number): { id: string; path: string } {
  if (!isRecord(value)) {
    throw new OrchestratorSubprocessClientError("invalid_command", `inputs[${index}] must be an object`);
  }
  const keys = Object.keys(value);
  if (keys.length !== 2 || !Object.hasOwn(value, "id") || !Object.hasOwn(value, "path")) {
    throw new OrchestratorSubprocessClientError("invalid_command", `inputs[${index}] must have exactly id and path`);
  }
  const id = expectSafeIdField(value, "id", `inputs[${index}].id`);
  const path = expectCleanPathField(value, "path", `inputs[${index}].path`);
  return { id, path };
}

/**
 * Captures one typed command in exactly one read per field: the exact
 * own-key set per kind is enforced first (hostile or unknown extra fields
 * are rejected without reading their values), then every field is read
 * once into a plain normalized record. This is the single validation
 * point shared by routing and the client; an invalid command never spawns.
 */
function captureOrchestratorCommand(command: unknown): CapturedCommand {
  if (!isRecord(command)) {
    throw new OrchestratorSubprocessClientError("invalid_command", "command must be an object with a kind");
  }
  const kind = command["kind"];
  if (typeof kind !== "string") {
    throw new OrchestratorSubprocessClientError("invalid_command", "command must be an object with a kind");
  }
  const launcherId = (): string | null => {
    const field = optionalField(command, "launcherId");
    if (!field.present) return null;
    const value = field.value;
    if (typeof value !== "string" || !value.startsWith("dhl_") || value.length === 4) {
      throw new OrchestratorSubprocessClientError("invalid_command", "launcherId must be a launcher ID (dhl_...)");
    }
    return value;
  };
  const empty: CapturedCommand = {
    kind: "",
    runId: "",
    pipelineRoot: null,
    projectPath: "",
    inputs: [],
    launcherId: null,
    stageId: "",
    initialBudget: 0,
    waitIndex: 0,
    additionalIterations: 0,
    taskId: "",
    taskFile: "",
    actionId: "",
    after: null,
    limit: null,
  };
  switch (kind) {
    case "run": {
      const required = ["kind", "projectPath", "inputs"] as const;
      const optional = ["runId", "pipelineRoot", "launcherId"] as const;
      for (const key of Object.keys(command)) {
        if (!([...required, ...optional] as readonly string[]).includes(key)) {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on run command");
        }
      }
      for (const key of required) {
        if (!Object.hasOwn(command, key)) {
          throw new OrchestratorSubprocessClientError("invalid_command", `missing field ${key} on run command`);
        }
      }
      const runIdField = optionalField(command, "runId");
      const runId = runIdField.present ? expectSafeIdField({ runId: runIdField.value }, "runId", "runId") : "";
      const pipelineRootField = optionalField(command, "pipelineRoot");
      const pipelineRoot = pipelineRootField.present ? expectCleanPathField({ pipelineRoot: pipelineRootField.value }, "pipelineRoot", "pipelineRoot") : null;
      const projectPath = expectCleanPathField(command, "projectPath", "projectPath");
      const inputsValue = command["inputs"];
      if (!Array.isArray(inputsValue)) {
        throw new OrchestratorSubprocessClientError("invalid_command", "inputs must be an array");
      }
      const inputs: { id: string; path: string }[] = [];
      const seenIds = new Set<string>();
      for (let index = 0; index < inputsValue.length; index += 1) {
        const input = expectRunInput(inputsValue[index], index);
        if (seenIds.has(input.id)) {
          throw new OrchestratorSubprocessClientError("invalid_command", "input ids must be unique");
        }
        seenIds.add(input.id);
        inputs.push(input);
      }
      return { ...empty, kind, runId, pipelineRoot, projectPath, inputs, launcherId: launcherId() };
    }
    case "inspect": {
      for (const key of Object.keys(command)) {
        if (key !== "kind" && key !== "runId") {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on inspect command");
        }
      }
      if (!Object.hasOwn(command, "runId")) {
        throw new OrchestratorSubprocessClientError("invalid_command", "missing field runId on inspect command");
      }
      return { ...empty, kind, runId: expectSafeIdField(command, "runId", "runId") };
    }
    case "events": {
      for (const key of Object.keys(command)) {
        if (key !== "kind" && key !== "runId" && key !== "after" && key !== "limit") {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on events command");
        }
      }
      if (!Object.hasOwn(command, "runId")) {
        throw new OrchestratorSubprocessClientError("invalid_command", "missing field runId on events command");
      }
      const afterField = optionalField(command, "after");
      const after = afterField.present ? expectNonNegativeIntegerField({ after: afterField.value }, "after", "after") : null;
      const limitField = optionalField(command, "limit");
      const limit = limitField.present ? expectPositiveIntegerField({ limit: limitField.value }, "limit", "limit") : null;
      return { ...empty, kind, runId: expectSafeIdField(command, "runId", "runId"), after, limit };
    }
    case "resume": {
      for (const key of Object.keys(command)) {
        if (key !== "kind" && key !== "runId" && key !== "launcherId") {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on resume command");
        }
      }
      if (!Object.hasOwn(command, "runId")) {
        throw new OrchestratorSubprocessClientError("invalid_command", "missing field runId on resume command");
      }
      return { ...empty, kind, runId: expectSafeIdField(command, "runId", "runId"), launcherId: launcherId() };
    }
    case "resume_plan": {
      for (const key of Object.keys(command)) {
        if (key !== "kind" && key !== "runId" && key !== "stageId" && key !== "initialBudget" && key !== "launcherId") {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on resume_plan command");
        }
      }
      for (const key of ["runId", "stageId", "initialBudget"] as const) {
        if (!Object.hasOwn(command, key)) {
          throw new OrchestratorSubprocessClientError("invalid_command", `missing field ${key} on resume_plan command`);
        }
      }
      return {
        ...empty,
        kind,
        runId: expectSafeIdField(command, "runId", "runId"),
        stageId: expectSafeIdField(command, "stageId", "stageId"),
        initialBudget: expectPositiveIntegerField(command, "initialBudget", "initialBudget"),
        launcherId: launcherId(),
      };
    }
    case "continue_stage": {
      for (const key of Object.keys(command)) {
        if (key !== "kind" && key !== "runId" && key !== "waitIndex" && key !== "additionalIterations" && key !== "launcherId") {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on continue_stage command");
        }
      }
      for (const key of ["runId", "waitIndex", "additionalIterations"] as const) {
        if (!Object.hasOwn(command, key)) {
          throw new OrchestratorSubprocessClientError("invalid_command", `missing field ${key} on continue_stage command`);
        }
      }
      return {
        ...empty,
        kind,
        runId: expectSafeIdField(command, "runId", "runId"),
        waitIndex: expectPositiveIntegerField(command, "waitIndex", "waitIndex"),
        additionalIterations: expectPositiveIntegerField(command, "additionalIterations", "additionalIterations"),
        launcherId: launcherId(),
      };
    }
    case "revise_task": {
      for (const key of Object.keys(command)) {
        if (key !== "kind" && key !== "runId" && key !== "waitIndex" && key !== "taskId" && key !== "taskFile" && key !== "launcherId") {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on revise_task command");
        }
      }
      for (const key of ["runId", "waitIndex", "taskId", "taskFile"] as const) {
        if (!Object.hasOwn(command, key)) {
          throw new OrchestratorSubprocessClientError("invalid_command", `missing field ${key} on revise_task command`);
        }
      }
      return {
        ...empty,
        kind,
        runId: expectSafeIdField(command, "runId", "runId"),
        waitIndex: expectPositiveIntegerField(command, "waitIndex", "waitIndex"),
        taskId: expectSafeIdField(command, "taskId", "taskId"),
        taskFile: expectCleanPathField(command, "taskFile", "taskFile"),
        launcherId: launcherId(),
      };
    }
    case "respond": {
      for (const key of Object.keys(command)) {
        if (key !== "kind" && key !== "runId" && key !== "waitIndex" && key !== "actionId") {
          throw new OrchestratorSubprocessClientError("invalid_command", "unknown field on respond command");
        }
      }
      for (const key of ["runId", "waitIndex", "actionId"] as const) {
        if (!Object.hasOwn(command, key)) {
          throw new OrchestratorSubprocessClientError("invalid_command", `missing field ${key} on respond command`);
        }
      }
      return {
        ...empty,
        kind,
        runId: expectSafeIdField(command, "runId", "runId"),
        waitIndex: expectPositiveIntegerField(command, "waitIndex", "waitIndex"),
        actionId: expectSafeIdField(command, "actionId", "actionId"),
      };
    }
    default:
      throw new OrchestratorSubprocessClientError("invalid_command", "unknown command kind");
  }
}

/**
 * Renders one typed command as orchestrator CLI argv (without the
 * executable and entrypoint, which belong to the client config). The
 * returned argv always ends with `--json`; the order is deterministic and
 * pinned by tests; every value stays a single argv element; `run` without
 * an explicit pipeline root (neither on the command nor in the routing
 * config) keeps the CLI's bundled default-v2 behavior.
 */
export function routeOrchestratorCommand(command: OrchestratorCommand, routingConfig: OrchestratorRoutingConfig): OrchestratorRoutedCommand {
  const captured = captureOrchestratorCommand(command);
  if (captured.kind !== "respond") {
    if (!isCleanAbsolutePath(routingConfig.configRoot)) {
      throw new OrchestratorSubprocessClientError("invalid_client_config", "configRoot must be a clean absolute path");
    }
    if (routingConfig.pipelineRoot !== undefined && !isCleanAbsolutePath(routingConfig.pipelineRoot)) {
      throw new OrchestratorSubprocessClientError("invalid_client_config", "pipelineRoot must be a clean absolute path");
    }
  }
  const cliKind =
    captured.kind === "resume_plan"
      ? "resume-plan"
      : captured.kind === "continue_stage"
        ? "continue-stage"
        : captured.kind === "revise_task"
          ? "revise-task"
          : captured.kind;
  const argv: string[] = [cliKind];
  switch (captured.kind) {
    case "run": {
      if (captured.runId !== "") argv.push("--run-id", captured.runId);
      const pipelineRoot = captured.pipelineRoot ?? routingConfig.pipelineRoot ?? null;
      if (pipelineRoot !== null) argv.push("--pipeline-root", pipelineRoot);
      argv.push("--config-root", routingConfig.configRoot);
      argv.push("--project", captured.projectPath);
      for (const input of captured.inputs) {
        argv.push("--input", `${input.id}=${input.path}`);
      }
      if (captured.launcherId !== null) argv.push("--launcher-id", captured.launcherId);
      break;
    }
    case "inspect":
      argv.push("--run-id", captured.runId, "--config-root", routingConfig.configRoot);
      break;
    case "events":
      argv.push("--run-id", captured.runId, "--config-root", routingConfig.configRoot);
      if (captured.after !== null) argv.push("--after", String(captured.after));
      if (captured.limit !== null) argv.push("--limit", String(captured.limit));
      break;
    case "resume":
      argv.push("--run-id", captured.runId, "--config-root", routingConfig.configRoot);
      if (captured.launcherId !== null) argv.push("--launcher-id", captured.launcherId);
      break;
    case "resume_plan":
      argv.push(
        "--run-id",
        captured.runId,
        "--stage-id",
        captured.stageId,
        "--initial-budget",
        String(captured.initialBudget),
        "--config-root",
        routingConfig.configRoot,
      );
      if (captured.launcherId !== null) argv.push("--launcher-id", captured.launcherId);
      break;
    case "continue_stage":
      argv.push(
        "--run-id",
        captured.runId,
        "--wait-index",
        String(captured.waitIndex),
        "--additional-iterations",
        String(captured.additionalIterations),
        "--config-root",
        routingConfig.configRoot,
      );
      if (captured.launcherId !== null) argv.push("--launcher-id", captured.launcherId);
      break;
    case "revise_task":
      argv.push(
        "--run-id",
        captured.runId,
        "--wait-index",
        String(captured.waitIndex),
        "--task-id",
        captured.taskId,
        "--task-file",
        captured.taskFile,
        "--config-root",
        routingConfig.configRoot,
      );
      if (captured.launcherId !== null) argv.push("--launcher-id", captured.launcherId);
      break;
    case "respond":
      argv.push("--run-id", captured.runId, "--wait-index", String(captured.waitIndex), "--action", captured.actionId);
      break;
    default:
      throw new OrchestratorSubprocessClientError("invalid_command", "unknown command kind");
  }
  argv.push("--json");
  return { argv, expectedRunId: captured.runId };
}
