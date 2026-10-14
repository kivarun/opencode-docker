/**
 * The versioned loopback HTTP service of the control-plane.
 *
 * This module is the single `Bun.serve` owner of the production tree: it
 * exposes exactly seven versioned routes over the existing control-plane
 * substrate and never executes orchestration itself:
 *
 *   HTTP -> project registry -> durable run binding
 *        -> OrchestratorSubprocessClient -> existing CLI subprocess
 *
 *   POST /v1/projects/:projectId/runs      fresh run
 *   GET  /v1/runs/:runId                   inspect
 *   POST /v1/runs/:runId/resume
 *   POST /v1/runs/:runId/resume-plan
 *   POST /v1/runs/:runId/continue-stage
 *   POST /v1/runs/:runId/revise-task
 *   POST /v1/runs/:runId/respond
 *
 * Authority flow: a fresh run resolves the registry request first
 * (invalid requests and missing inputs never reserve a run id), then
 * claims the durable run binding, then spawns the subprocess. Every
 * existing-run operation resolves the binding first
 * (`resolveBoundControlPlaneProject`) and derives the routing authority
 * exclusively from it — a projectId is never accepted from the HTTP
 * caller, and a CLI-created run without a binding fails closed as
 * `binding_not_found`. The registry and the binding store are opened
 * once per service lifetime; the one immutable subprocess client is
 * built after the loaded registry proved that every declared project
 * shares one routing authority (config root and pipeline-root form), so
 * the client's routing is exactly the bound project's routing.
 *
 * Trust boundary (loopback single-user service, NOT auth/RBAC): the
 * request is served only when the Host header exactly matches the
 * actual listen authority (`127.0.0.1:<port>`), the Origin header is
 * absent or exactly the service origin, the path carries no query
 * string, no percent-encoding, no empty or dot segments and no extra
 * segments, and POST bodies declare exactly `application/json` and stay
 * within the fixed byte limit (the body is read through a bounded
 * stream loop, never an unbounded `request.text()`). OPTIONS and CORS
 * are not supported. No HTTP parameter ever becomes a host path, an
 * argv fragment or an env name; body references become host paths only
 * through the registry's own canonicalization policy.
 *
 * Request lifecycle: the HTTP request stays synchronous relative to the
 * subprocess call and the client never receives the request signal — a
 * client disconnect does not cancel the orchestration; a lost response
 * is recovered through `GET /v1/runs/:runId`. Parallel mutators are not
 * serialized by this layer — the existing kernel run mutation lock stays
 * the single owner and the loser observes the ordinary `run_busy`
 * outcome. There is no process-local queue, mutex, registry or retry
 * machine.
 *
 * Response contract (schema version 1, exact keys, no host paths, no raw
 * state, no stderr/stdout, no registry roots, no launcher material, no
 * proposal/task/output contents): a correctly received orchestrator
 * outcome is projected as `{"schema_version":1,"kind":"command_result",
 * "run_id",...,"outcome","exit_code"[,"reason"]}` with HTTP 200 — a
 * failed Pipeline V2 outcome is still a correctly received outcome and
 * its public reason is never reclassified into an HTTP status; a
 * successful inspect is projected as `{"schema_version":1,"kind":"run",
 * "run_id","revision","status","boundary","outputs"}` with the boundary
 * and outputs copied from the verified schemaVersion-1 projection and
 * the run root dropped. Control-plane failures project as
 * `{"schema_version":1,"kind":"error","layer","reason"}` with a closed
 * reason vocabulary and no message, details or paths.
 */

import { isCleanAbsolutePath } from "../clean_path.ts";
import { isPipelineV2SafeId, isPositiveSafeInteger } from "../pipeline_v2_scalar.ts";
import {
  ControlPlaneProjectRegistryError,
  loadControlPlaneProjectRegistry,
  resolveControlPlaneBoundProjectInput,
  resolveControlPlaneRunCommand,
  type ControlPlaneProjectRegistry,
  type ControlPlaneRunInput,
  type ResolvedControlPlaneProject,
} from "./project_registry.ts";
import {
  ControlPlaneRunBindingError,
  ControlPlaneRunBindingStore,
  resolveBoundControlPlaneProject,
} from "./run_binding_store.ts";
import { type OrchestratorCommand } from "./routing.ts";
import {
  OrchestratorSubprocessClient,
  type OrchestratorCallResult,
  type OrchestratorSubprocessClientConfig,
} from "./subprocess_client.ts";

/** The closed failure vocabulary of the HTTP request layer itself. */
export type ControlPlaneHttpRequestFailureReason =
  | "invalid_host"
  | "invalid_origin"
  | "invalid_path"
  | "unknown_route"
  | "method_not_allowed"
  | "unsupported_media_type"
  | "payload_too_large"
  | "invalid_request";

/** The typed request-layer error; the HTTP status comes from the one status table. */
export class ControlPlaneHttpRequestError extends Error {
  readonly reason!: ControlPlaneHttpRequestFailureReason;

  constructor(reason: ControlPlaneHttpRequestFailureReason, message: string) {
    super(message);
    this.name = "ControlPlaneHttpRequestError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/**
 * The closed failure vocabulary of the service itself: startup and
 * per-service configuration problems that are operator errors, not
 * request, registry, binding or transport failures.
 */
export type ControlPlaneHttpServiceFailureReason = "invalid_service_config" | "registry_authority_mismatch";

export class ControlPlaneHttpServiceError extends Error {
  readonly reason!: ControlPlaneHttpServiceFailureReason;

  constructor(reason: ControlPlaneHttpServiceFailureReason, message: string) {
    super(message);
    this.name = "ControlPlaneHttpServiceError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

interface ParsedRoute {
  readonly kind:
    | "fresh_run"
    | "inspect"
    | "resume"
    | "resume_plan"
    | "continue_stage"
    | "revise_task"
    | "respond"
    | "projects"
    | "runs";
  readonly projectId: string | null;
  readonly runId: string | null;
}

const FRESH_RUN_SEGMENTS = 4;
const INSPECT_SEGMENTS = 3;
const ACTION_SEGMENTS = 4;
const ACTION_NAMES: readonly string[] = ["resume", "resume-plan", "continue-stage", "revise-task", "respond"];

/**
 * The one route parser: exact segment shapes, no query string, no
 * percent-encoding, no empty or dot segments, exact method per route,
 * safe ids in every path slot. Unknown paths are `unknown_route`; a
 * known shape with the wrong method is `method_not_allowed`.
 */
export function parseControlPlaneRoute(pathname: string, method: string): ParsedRoute | ControlPlaneHttpRequestError {
  const segments = pathname.split("/");
  if (segments.length < 2 || segments[0] !== "") {
    return new ControlPlaneHttpRequestError("invalid_path", "the request path must be an absolute versioned path");
  }
  const rest = segments.slice(1);
  for (const segment of rest) {
    if (segment === "" || segment === "." || segment === "..") {
      return new ControlPlaneHttpRequestError("invalid_path", "the request path must not contain empty or dot segments");
    }
    if (segment.includes("%")) {
      return new ControlPlaneHttpRequestError("invalid_path", "the request path must not carry encoded components");
    }
  }
  const matchesShape = (shape: readonly string[]): boolean => {
    if (rest.length !== shape.length) return false;
    for (let index = 0; index < shape.length; index += 1) {
      const literal = shape[index];
      if (literal === undefined) return false;
      if (literal !== "*") {
        if (rest[index] !== literal) return false;
      }
    }
    return true;
  };
  const shapeMethodMismatch = (expected: string): boolean => method !== expected;
  const safeId = (value: string | undefined): string | null => {
    if (value === undefined || !isPipelineV2SafeId(value)) {
      return null;
    }
    return value;
  };
  if (matchesShape(["v1", "projects", "*", "runs"])) {
    if (shapeMethodMismatch("POST")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the fresh-run route accepts POST only");
    }
    const projectId = safeId(rest[2]);
    if (projectId === null) {
      return new ControlPlaneHttpRequestError("invalid_path", "the project id must be a safe identifier");
    }
    return { kind: "fresh_run", projectId, runId: null };
  }
  if (matchesShape(["v1", "projects"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the projects discovery route accepts GET only");
    }
    return { kind: "projects", projectId: null, runId: null };
  }
  if (matchesShape(["v1", "runs"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the runs discovery route accepts GET only");
    }
    return { kind: "runs", projectId: null, runId: null };
  }
  if (matchesShape(["v1", "runs", "*"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the inspect route accepts GET only");
    }
    const runId = safeId(rest[2]);
    if (runId === null) {
      return new ControlPlaneHttpRequestError("invalid_path", "the run id must be a safe identifier");
    }
    return { kind: "inspect", projectId: null, runId };
  }
  if (matchesShape(["v1", "runs", "*", "*"])) {
    const action = rest[3];
    if (action === undefined || !ACTION_NAMES.includes(action)) {
      return new ControlPlaneHttpRequestError("unknown_route", "the request path does not name a versioned route");
    }
    if (shapeMethodMismatch("POST")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the mutation routes accept POST only");
    }
    const runId = safeId(rest[2]);
    if (runId === null) {
      return new ControlPlaneHttpRequestError("invalid_path", "the run id must be a safe identifier");
    }
    const kind =
      action === "resume"
        ? "resume"
        : action === "resume-plan"
          ? "resume_plan"
          : action === "continue-stage"
            ? "continue_stage"
            : action === "revise-task"
              ? "revise_task"
              : "respond";
    return { kind, projectId: null, runId };
  }
  return new ControlPlaneHttpRequestError("unknown_route", "the request path does not name a versioned route");
}

/**
 * The one HTTP status table of the service. A correctly received
 * orchestrator projection is always 200 (a failed Pipeline V2 outcome is
 * never reclassified); request failures are 400/404/405/413/415; the
 * not-found family of the registry and the binding store is 404; the
 * conflict family is 409; infrastructure failures are 500; transport
 * failures are 502.
 */
export function statusForControlPlaneFailure(failure: { readonly layer: string; readonly reason: string }): number {
  if (failure.layer === "request") {
    switch (failure.reason) {
      case "unknown_route":
        return 404;
      case "method_not_allowed":
        return 405;
      case "unsupported_media_type":
        return 415;
      case "payload_too_large":
        return 413;
      default:
        return 400;
    }
  }
  if (failure.layer === "registry") {
    switch (failure.reason) {
      case "project_not_found":
      case "input_root_not_found":
      case "input_not_found":
        return 404;
      case "input_invalid":
      case "invalid_request":
        return 400;
      default:
        return 500;
    }
  }
  if (failure.layer === "binding") {
    switch (failure.reason) {
      case "binding_conflict":
      case "project_unavailable":
        return 409;
      case "binding_not_found":
        return 404;
      default:
        return 500;
    }
  }
  if (failure.layer === "transport") {
    return 502;
  }
  return 500;
}

interface ControlPlaneHttpFailure {
  readonly layer: "request" | "registry" | "binding" | "transport";
  readonly reason: string;
}

function jsonBody(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function failureResponse(failure: ControlPlaneHttpFailure): Response {
  return jsonBody({ schema_version: 1, kind: "error", layer: failure.layer, reason: failure.reason }, statusForControlPlaneFailure(failure));
}

function requestFailure(reason: ControlPlaneHttpRequestFailureReason): Response {
  return failureResponse({ layer: "request", reason });
}

/**
 * The single JSON.parse owner of the HTTP layer: every request body is
 * parsed exactly once; a malformed, empty or multi-document body is a
 * typed request failure with no parser fragments in any diagnostic.
 */
function parseJsonBody(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    throw new ControlPlaneHttpRequestError("invalid_request", "the request body must be exactly one JSON document");
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function expectExactKeys(record: Record<string, unknown>, keys: readonly string[], what: string): void {
  const own = Object.keys(record);
  if (own.length !== keys.length) {
    throw new ControlPlaneHttpRequestError("invalid_request", `${what} must carry exactly its declared fields`);
  }
  for (const key of keys) {
    if (!Object.hasOwn(record, key)) {
      throw new ControlPlaneHttpRequestError("invalid_request", `${what} must carry exactly its declared fields`);
    }
  }
}

function expectSafeIdField(record: Record<string, unknown>, key: string, what: string): string {
  const value = record[key];
  if (!isPipelineV2SafeId(value)) {
    throw new ControlPlaneHttpRequestError("invalid_request", `${what} must be a safe identifier`);
  }
  return value as string;
}

function expectPositiveIntegerField(record: Record<string, unknown>, key: string, what: string): number {
  const value = record[key];
  if (!isPositiveSafeInteger(value)) {
    throw new ControlPlaneHttpRequestError("invalid_request", `${what} must be a positive safe integer`);
  }
  return value as number;
}

/** The fresh-run request body: `{run_id, inputs[] {id, root_id, ref}}` in declaration order. */
export function parseFreshRunBody(document: unknown): { readonly runId: string; readonly inputs: readonly ControlPlaneRunInput[] } {
  if (!isRecord(document)) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the fresh-run request body must be an object");
  }
  expectExactKeys(document, ["run_id", "inputs"], "the fresh-run request body");
  const runId = expectSafeIdField(document, "run_id", "the fresh-run request run_id");
  const inputsValue = document["inputs"];
  if (!Array.isArray(inputsValue) || inputsValue.length === 0) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the fresh-run request inputs must be a non-empty list");
  }
  const inputs: ControlPlaneRunInput[] = [];
  const seenIds: string[] = [];
  for (const entry of inputsValue) {
    if (!isRecord(entry)) {
      throw new ControlPlaneHttpRequestError("invalid_request", "each fresh-run input must be an object");
    }
    expectExactKeys(entry, ["id", "root_id", "ref"], "each fresh-run input");
    const id = expectSafeIdField(entry, "id", "each fresh-run input id");
    for (const seen of seenIds) {
      if (seen === id) {
        throw new ControlPlaneHttpRequestError("invalid_request", "the fresh-run input ids must be unique");
      }
    }
    seenIds.push(id);
    inputs.push({
      id,
      rootId: expectSafeIdField(entry, "root_id", "each fresh-run input root_id"),
      ref: expectSafeIdField(entry, "ref", "each fresh-run input ref"),
    });
  }
  return { runId, inputs };
}

export function parseEmptyBody(document: unknown): void {
  if (!isRecord(document)) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the resume request body must be an object");
  }
  if (Object.keys(document).length !== 0) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the resume request body must be empty");
  }
}

export function parseResumePlanBody(document: unknown): { readonly stageId: string; readonly initialBudget: number } {
  if (!isRecord(document)) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the resume-plan request body must be an object");
  }
  expectExactKeys(document, ["stage_id", "initial_budget"], "the resume-plan request body");
  return {
    stageId: expectSafeIdField(document, "stage_id", "the resume-plan request stage_id"),
    initialBudget: expectPositiveIntegerField(document, "initial_budget", "the resume-plan request initial_budget"),
  };
}

export function parseContinueStageBody(document: unknown): { readonly waitIndex: number; readonly additionalIterations: number } {
  if (!isRecord(document)) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the continue-stage request body must be an object");
  }
  expectExactKeys(document, ["wait_index", "additional_iterations"], "the continue-stage request body");
  return {
    waitIndex: expectPositiveIntegerField(document, "wait_index", "the continue-stage request wait_index"),
    additionalIterations: expectPositiveIntegerField(document, "additional_iterations", "the continue-stage request additional_iterations"),
  };
}

export function parseRespondBody(document: unknown): { readonly waitIndex: number; readonly actionId: string } {
  if (!isRecord(document)) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the respond request body must be an object");
  }
  expectExactKeys(document, ["wait_index", "action_id"], "the respond request body");
  return {
    waitIndex: expectPositiveIntegerField(document, "wait_index", "the respond request wait_index"),
    actionId: expectSafeIdField(document, "action_id", "the respond request action_id"),
  };
}

export function parseReviseTaskBody(
  document: unknown,
): { readonly waitIndex: number; readonly taskId: string; readonly taskRootId: string; readonly taskRef: string } {
  if (!isRecord(document)) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the revise-task request body must be an object");
  }
  expectExactKeys(document, ["wait_index", "task_id", "task"], "the revise-task request body");
  const task = document["task"];
  if (!isRecord(task)) {
    throw new ControlPlaneHttpRequestError("invalid_request", "the revise-task request task must be an object");
  }
  expectExactKeys(task, ["root_id", "ref"], "the revise-task request task");
  return {
    waitIndex: expectPositiveIntegerField(document, "wait_index", "the revise-task request wait_index"),
    taskId: expectSafeIdField(document, "task_id", "the revise-task request task_id"),
    taskRootId: expectSafeIdField(task, "root_id", "the revise-task task root_id"),
    taskRef: expectSafeIdField(task, "ref", "the revise-task task ref"),
  };
}

/**
 * The discovery query of the runs listing: an optional positive safe
 * integer limit (default 100, maximum 200) and an optional safe run id
 * cursor. The raw query string is parsed strictly - no percent-encoding,
 * no bare keys, no duplicates, no empty values and no unknown fields.
 */
export function parseRunsDiscoveryQuery(
  search: string,
): { readonly limit: number; readonly after: string | null } | ControlPlaneHttpRequestError {
  if (search.startsWith("?")) {
    search = search.slice(1);
  }
  if (search === "") {
    return { limit: 100, after: null };
  }
  if (search.includes("%")) {
    return new ControlPlaneHttpRequestError("invalid_request", "the discovery query must not carry encoded components");
  }
  let limit: number | null = null;
  let after: string | null = null;
  for (const part of search.split("&")) {
    const separator = part.indexOf("=");
    if (separator <= 0) {
      return new ControlPlaneHttpRequestError("invalid_request", "every discovery query field must carry exactly one value");
    }
    const key = part.slice(0, separator);
    const value = part.slice(separator + 1);
    if (key === "limit") {
      if (limit !== null) {
        return new ControlPlaneHttpRequestError("invalid_request", "the discovery query fields must be unique");
      }
      if (!/^[0-9]+$/.test(value)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the discovery query limit must be a positive safe integer");
      }
      const parsed = Number(value);
      if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 200) {
        return new ControlPlaneHttpRequestError("invalid_request", "the discovery query limit must be a positive safe integer up to 200");
      }
      limit = parsed;
      continue;
    }
    if (key === "after") {
      if (after !== null) {
        return new ControlPlaneHttpRequestError("invalid_request", "the discovery query fields must be unique");
      }
      if (value === "" || !isPipelineV2SafeId(value)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the discovery query after must be a safe run identifier");
      }
      after = value;
      continue;
    }
    return new ControlPlaneHttpRequestError("invalid_request", "the discovery query carries an unknown field");
  }
  return { limit: limit ?? 100, after };
}

/** The projects discovery projection: declaration-order ids only. */
export interface ProjectsDiscoveryProjection {
  readonly schema_version: 1;
  readonly kind: "projects";
  readonly projects: readonly { readonly id: string; readonly input_roots: readonly { readonly id: string }[] }[];
}

export function projectControlPlaneProjects(registry: ControlPlaneProjectRegistry): ProjectsDiscoveryProjection {
  const projects: { id: string; input_roots: { id: string }[] }[] = [];
  for (const project of registry.projects) {
    const inputRoots: { id: string }[] = [];
    for (const root of project.inputRoots) {
      inputRoots.push({ id: root.id });
    }
    projects.push({ id: project.id, input_roots: inputRoots });
  }
  return { schema_version: 1, kind: "projects", projects };
}

/** One projected run of the runs discovery: ids only, never run status. */
export interface RunsDiscoveryEntry {
  readonly run_id: string;
  readonly project_id: string;
  readonly project_available: boolean;
}

export interface RunsDiscoveryProjection {
  readonly schema_version: 1;
  readonly kind: "runs";
  readonly runs: readonly RunsDiscoveryEntry[];
  readonly next_after: string | null;
}

/**
 * The runs discovery projection over the authoritative binding list:
 * exclusive lexical cursor, page limit, and `next_after` exactly when
 * another binding exists beyond the page. A project the current registry
 * no longer declares stays visible with `project_available: false`.
 */
export function projectControlPlaneRuns(
  bindings: readonly { readonly runId: string; readonly projectId: string }[],
  options: { readonly limit: number; readonly after: string | null },
  registry: ControlPlaneProjectRegistry,
): RunsDiscoveryProjection {
  let start = 0;
  if (options.after !== null) {
    while (start < bindings.length && bindings[start] !== undefined && (bindings[start] as { runId: string }).runId <= options.after) {
      start += 1;
    }
  }
  const runs: RunsDiscoveryEntry[] = [];
  for (let index = start; index < bindings.length && runs.length < options.limit; index += 1) {
    const binding = bindings[index];
    if (binding === undefined) {
      break;
    }
    let available = false;
    for (const project of registry.projects) {
      if (project.id === binding.projectId) {
        available = true;
        break;
      }
    }
    runs.push({
      run_id: binding.runId,
      project_id: binding.projectId,
      project_available: available,
    });
  }
  const lastEntry = runs[runs.length - 1];
  const lastIndex = start + runs.length;
  const nextAfter = lastEntry !== undefined && lastIndex < bindings.length ? lastEntry.run_id : null;
  return { schema_version: 1, kind: "runs", runs, next_after: nextAfter };
}

interface CommandOutcomeProjection {
  readonly schema_version: 1;
  readonly kind: "command_result";
  readonly run_id: string;
  readonly outcome: "succeeded" | "waiting" | "plan_ready" | "responded" | "failed";
  readonly exit_code: 0 | 1 | 130 | 143;
  readonly reason?: string;
}

/**
 * The one projection builder for a correctly received orchestrator
 * outcome: built explicitly from the typed client projection — never a
 * spread — so the run root, the raw state and every host path stay out
 * of the HTTP response. A failed Pipeline V2 outcome stays a 200 command
 * result; a transport failure is the typed 502 error body.
 */
export function projectControlPlaneCommandResult(
  result: OrchestratorCallResult,
): CommandOutcomeProjection | ControlPlaneHttpFailure {
  if (!result.ok) {
    return { layer: "transport", reason: result.failure.reason };
  }
  const projection = result.projection;
  if (projection.kind === "inspect" || projection.kind === "inspectFailure") {
    return { layer: "transport", reason: "internal_error" };
  }
  if (projection.kind === "success") {
    return { schema_version: 1, kind: "command_result", run_id: projection.runId, outcome: "succeeded", exit_code: 0 };
  }
  if (projection.kind === "waiting") {
    return { schema_version: 1, kind: "command_result", run_id: projection.runId, outcome: "waiting", exit_code: 0 };
  }
  if (projection.kind === "planReady") {
    return { schema_version: 1, kind: "command_result", run_id: projection.runId, outcome: "plan_ready", exit_code: 0 };
  }
  if (projection.kind === "responded") {
    return { schema_version: 1, kind: "command_result", run_id: projection.runId, outcome: "responded", exit_code: 0 };
  }
  return {
    schema_version: 1,
    kind: "command_result",
    run_id: projection.runId,
    outcome: "failed",
    exit_code: projection.exitCode,
    reason: projection.reason,
  };
}

interface RunProjection {
  readonly schema_version: 1;
  readonly kind: "run";
  readonly run_id: string;
  readonly revision: number;
  readonly status: string;
  readonly boundary: unknown;
  readonly outputs: unknown;
}

/**
 * The one projection builder for a verified schemaVersion-1 inspect
 * projection: `boundary` and `outputs` are copied from the projection
 * without any new classification and the run root is dropped. Anything
 * that is not a verified inspect projection is a defensive internal
 * failure, never a guessed run document.
 */
export function projectControlPlaneInspect(
  result: OrchestratorCallResult,
): RunProjection | CommandOutcomeProjection | ControlPlaneHttpFailure {
  if (!result.ok) {
    return { layer: "transport", reason: result.failure.reason };
  }
  const projection = result.projection;
  if (projection.kind === "inspectFailure") {
    return {
      schema_version: 1,
      kind: "command_result",
      run_id: projection.runId,
      outcome: "failed",
      exit_code: projection.exitCode,
      reason: projection.reason,
    } satisfies CommandOutcomeProjection;
  }
  if (projection.kind !== "inspect" || projection.schemaVersion !== 1) {
    return { layer: "transport", reason: "internal_error" };
  }
  return {
    schema_version: 1,
    kind: "run",
    run_id: projection.runId,
    revision: projection.revision,
    status: projection.status,
    boundary: projection.boundary,
    outputs: projection.outputs,
  };
}

function projectionResponse(
  projection: CommandOutcomeProjection | RunProjection | ProjectsDiscoveryProjection | RunsDiscoveryProjection | ControlPlaneHttpFailure,
): Response {
  if ("layer" in projection && "reason" in projection && !("kind" in projection)) {
    return failureResponse(projection as ControlPlaneHttpFailure);
  }
  return jsonBody(projection, 200);
}

function launcherFields(project: ResolvedControlPlaneProject): { readonly launcherId?: string } {
  return project.launcherId === null ? {} : { launcherId: project.launcherId };
}

/**
 * The per-request handler dependencies: the objects opened once per
 * service lifetime plus the actual bound port (the Host authority) and
 * the fixed body limit.
 */
export interface ControlPlaneHttpServiceDeps {
  readonly registry: ControlPlaneProjectRegistry;
  readonly bindingStore: ControlPlaneRunBindingStore;
  readonly client: OrchestratorSubprocessClient;
  readonly port: number;
  readonly bodyLimitBytes: number;
  /** The raw query string of the current discovery request ("" otherwise). */
  readonly discoveryQuery?: string;
}

async function readBodyLimited(request: Request, limit: number): Promise<{ readonly ok: true; readonly text: string } | { readonly ok: false }> {
  const lengthHeader = request.headers.get("content-length");
  if (lengthHeader !== null) {
    const declared = Number(lengthHeader);
    if (Number.isSafeInteger(declared) && declared > limit) {
      return { ok: false };
    }
  }
  const stream = request.body;
  if (stream === null) {
    return { ok: true, text: "" };
  }
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    if (value !== undefined) {
      total += value.byteLength;
      if (total > limit) {
        try {
          await reader.cancel();
        } catch {
          // the connection is already going away; the size refusal stands
        }
        return { ok: false };
      }
      chunks.push(value);
    }
  }
  const parts: Uint8Array[] = chunks;
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    merged.set(part, offset);
    offset += part.byteLength;
  }
  return { ok: true, text: new TextDecoder().decode(merged) };
}

/**
 * Handles one HTTP request against the opened service dependencies.
 * The trust gates run before any body parsing and before any effect;
 * the request signal is never forwarded to the subprocess client.
 */
export async function handleControlPlaneHttpRequest(deps: ControlPlaneHttpServiceDeps, request: Request): Promise<Response> {
  try {
    const authority = `127.0.0.1:${deps.port}`;
    const host = request.headers.get("host");
    if (host !== authority) {
      return requestFailure("invalid_host");
    }
    const origin = request.headers.get("origin");
    if (origin !== null && origin !== `http://${authority}`) {
      return requestFailure("invalid_origin");
    }
    const url = new URL(request.url);
    if (url.hash !== "") {
      return requestFailure("invalid_path");
    }
    const parsed = parseControlPlaneRoute(url.pathname, request.method);
    if (parsed instanceof ControlPlaneHttpRequestError) {
      return requestFailure(parsed.reason);
    }
    if (parsed.kind !== "projects" && parsed.kind !== "runs" && url.search !== "") {
      return requestFailure("invalid_path");
    }
    if (request.method === "POST") {
      const contentType = request.headers.get("content-type");
      if (contentType !== "application/json") {
        return requestFailure("unsupported_media_type");
      }
    } else {
      const lengthHeader = request.headers.get("content-length");
      if (lengthHeader !== null && lengthHeader !== "0") {
        return requestFailure("invalid_request");
      }
    }
    let bodyText = "";
    if (request.method === "POST") {
      const body = await readBodyLimited(request, deps.bodyLimitBytes);
      if (!body.ok) {
        return requestFailure("payload_too_large");
      }
      bodyText = body.text;
    }
    return await dispatchControlPlaneRoute(
      parsed.kind === "projects" || parsed.kind === "runs" ? { ...deps, discoveryQuery: url.search } : deps,
      parsed,
      bodyText,
    );
  } catch (cause) {
    if (cause instanceof ControlPlaneProjectRegistryError) {
      return failureResponse({ layer: "registry", reason: cause.reason });
    }
    if (cause instanceof ControlPlaneRunBindingError) {
      return failureResponse({ layer: "binding", reason: cause.reason });
    }
    if (cause instanceof ControlPlaneHttpRequestError) {
      return requestFailure(cause.reason);
    }
    return failureResponse({ layer: "transport", reason: "internal_error" });
  }
}

async function dispatchControlPlaneRoute(
  deps: ControlPlaneHttpServiceDeps,
  route: ParsedRoute,
  bodyText: string,
): Promise<Response> {
  if (route.kind === "projects") {
    if ((deps.discoveryQuery ?? "") !== "") {
      return requestFailure("invalid_request");
    }
    return projectionResponse(projectControlPlaneProjects(deps.registry));
  }
  if (route.kind === "runs") {
    const query = parseRunsDiscoveryQuery(deps.discoveryQuery ?? "");
    if (query instanceof ControlPlaneHttpRequestError) {
      return requestFailure(query.reason);
    }
    const bindings = await deps.bindingStore.listBindings();
    return projectionResponse(projectControlPlaneRuns(bindings, query, deps.registry));
  }
  if (route.kind === "fresh_run") {
    const body = parseFreshRunBody(parseJsonBody(bodyText));
    const resolved = await resolveControlPlaneRunCommand(deps.registry, {
      runId: body.runId,
      projectId: route.projectId as string,
      inputs: body.inputs,
    });
    await deps.bindingStore.claim({ runId: body.runId, projectId: route.projectId as string });
    const result = await deps.client.run(resolved.command);
    return projectionResponse(projectControlPlaneCommandResult(result));
  }
  const runId = route.runId as string;
  const authority = await resolveBoundControlPlaneProject(deps.registry, deps.bindingStore, runId);
  switch (route.kind) {
    case "inspect": {
      const result = await deps.client.run({ kind: "inspect", runId });
      return projectionResponse(projectControlPlaneInspect(result));
    }
    case "resume": {
      parseEmptyBody(parseJsonBody(bodyText));
      const command: OrchestratorCommand = { kind: "resume", runId, ...launcherFields(authority.project) };
      return projectionResponse(projectControlPlaneCommandResult(await deps.client.run(command)));
    }
    case "resume_plan": {
      const body = parseResumePlanBody(parseJsonBody(bodyText));
      const command: OrchestratorCommand = {
        kind: "resume_plan",
        runId,
        stageId: body.stageId,
        initialBudget: body.initialBudget,
        ...launcherFields(authority.project),
      };
      return projectionResponse(projectControlPlaneCommandResult(await deps.client.run(command)));
    }
    case "continue_stage": {
      const body = parseContinueStageBody(parseJsonBody(bodyText));
      const command: OrchestratorCommand = {
        kind: "continue_stage",
        runId,
        waitIndex: body.waitIndex,
        additionalIterations: body.additionalIterations,
        ...launcherFields(authority.project),
      };
      return projectionResponse(projectControlPlaneCommandResult(await deps.client.run(command)));
    }
    case "respond": {
      const body = parseRespondBody(parseJsonBody(bodyText));
      const command: OrchestratorCommand = { kind: "respond", runId, waitIndex: body.waitIndex, actionId: body.actionId };
      return projectionResponse(projectControlPlaneCommandResult(await deps.client.run(command)));
    }
    case "revise_task": {
      const body = parseReviseTaskBody(parseJsonBody(bodyText));
      const taskFile = await resolveControlPlaneBoundProjectInput(
        deps.registry,
        authority.project,
        body.taskRootId,
        body.taskRef,
      );
      const command: OrchestratorCommand = {
        kind: "revise_task",
        runId,
        waitIndex: body.waitIndex,
        taskId: body.taskId,
        taskFile,
        ...launcherFields(authority.project),
      };
      return projectionResponse(projectControlPlaneCommandResult(await deps.client.run(command)));
    }
    default:
      return failureResponse({ layer: "transport", reason: "internal_error" });
  }
}

/**
 * The non-routing runtime configuration of the one subprocess client:
 * the routing authority (config root and pipeline-root form) is derived
 * from the loaded registry, never duplicated in operator flags.
 */
export interface ControlPlaneHttpServiceClientRuntime {
  readonly bunExecutable: string;
  readonly orchestratorEntrypoint: string;
  readonly workingDirectory: string;
  readonly baseEnv: Readonly<Record<string, string>>;
  readonly stdoutLimitBytes: number;
  readonly stderrLimitBytes: number;
  readonly sigintGraceMs: number;
}

/** The immutable per-service configuration, loaded once at startup. */
export interface ControlPlaneHttpServiceConfig {
  readonly registryPath: string;
  readonly bindingStoreRoot: string;
  readonly client: ControlPlaneHttpServiceClientRuntime;
  readonly port: number;
  readonly bodyLimitBytes: number;
}

/** A running service: the actual bound port plus the opened authorities. */
export interface RunningControlPlaneHttpService {
  readonly port: number;
  readonly registry: ControlPlaneProjectRegistry;
  readonly bindingStore: ControlPlaneRunBindingStore;
  readonly client: OrchestratorSubprocessClient;
  stop(): Promise<void>;
}

function requireCleanPath(value: unknown, what: string): string {
  if (!isCleanAbsolutePath(value)) {
    throw new ControlPlaneHttpServiceError("invalid_service_config", `${what} must be a clean absolute path`);
  }
  return value as string;
}

/**
 * Starts one loopback HTTP service: validates the configuration, loads
 * the registry once, proves the single routing authority across every
 * declared project, opens the binding store once, builds the one
 * immutable subprocess client and serves the seven versioned routes on
 * `127.0.0.1`. There is no hot reload, no discovery and no second
 * execution path.
 */
export async function startControlPlaneHttpService(config: ControlPlaneHttpServiceConfig): Promise<RunningControlPlaneHttpService> {
  if (typeof config !== "object" || config === null) {
    throw new ControlPlaneHttpServiceError("invalid_service_config", "the service configuration must be an object");
  }
  const registryPath = requireCleanPath(config.registryPath, "the registry path");
  const bindingStoreRoot = requireCleanPath(config.bindingStoreRoot, "the binding store root");
  const port = config.port;
  if (!Number.isSafeInteger(port) || port < 0 || port > 65535) {
    throw new ControlPlaneHttpServiceError("invalid_service_config", "the listen port must be a safe port number");
  }
  const bodyLimitBytes = config.bodyLimitBytes;
  if (!isPositiveSafeInteger(bodyLimitBytes)) {
    throw new ControlPlaneHttpServiceError("invalid_service_config", "the body limit must be a positive safe integer");
  }
  const clientConfig = config.client;
  if (typeof clientConfig !== "object" || clientConfig === null) {
    throw new ControlPlaneHttpServiceError("invalid_service_config", "the client runtime must be an object");
  }
  const bunExecutable = requireCleanPath(clientConfig.bunExecutable, "the bun executable");
  const orchestratorEntrypoint = requireCleanPath(clientConfig.orchestratorEntrypoint, "the orchestrator entrypoint");
  const workingDirectory = requireCleanPath(clientConfig.workingDirectory, "the working directory");
  const baseEnv = clientConfig.baseEnv;
  if (typeof baseEnv !== "object" || baseEnv === null) {
    throw new ControlPlaneHttpServiceError("invalid_service_config", "the client environment must be an object");
  }
  for (const key of Object.keys(baseEnv)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key) || typeof baseEnv[key] !== "string") {
      throw new ControlPlaneHttpServiceError("invalid_service_config", "the client environment must map valid names to strings");
    }
  }
  if (!isPositiveSafeInteger(clientConfig.stdoutLimitBytes) || !isPositiveSafeInteger(clientConfig.stderrLimitBytes) || !isPositiveSafeInteger(clientConfig.sigintGraceMs)) {
    throw new ControlPlaneHttpServiceError("invalid_service_config", "the client limits must be positive safe integers");
  }
  const registry = await loadControlPlaneProjectRegistry(registryPath);
  const firstProject = registry.projects[0];
  if (firstProject === undefined) {
    throw new ControlPlaneHttpServiceError("registry_authority_mismatch", "the registry must declare at least one project");
  }
  const configRoot = firstProject.configRoot;
  const pipelineRoot = firstProject.pipelineRoot === null ? undefined : firstProject.pipelineRoot;
  for (const project of registry.projects) {
    const projectPipelineRoot = project.pipelineRoot === null ? undefined : project.pipelineRoot;
    if (project.configRoot !== configRoot || projectPipelineRoot !== pipelineRoot) {
      throw new ControlPlaneHttpServiceError(
        "registry_authority_mismatch",
        "every declared project must share one routing authority",
      );
    }
  }
  const bindingStore = await ControlPlaneRunBindingStore.open(bindingStoreRoot);
  const clientFullConfig: OrchestratorSubprocessClientConfig = {
    bunExecutable,
    orchestratorEntrypoint,
    workingDirectory,
    configRoot,
    ...(pipelineRoot === undefined ? {} : { pipelineRoot }),
    baseEnv,
    stdoutLimitBytes: clientConfig.stdoutLimitBytes,
    stderrLimitBytes: clientConfig.stderrLimitBytes,
    sigintGraceMs: clientConfig.sigintGraceMs,
  };
  const client = new OrchestratorSubprocessClient(clientFullConfig);
  const depsRef: { current: ControlPlaneHttpServiceDeps | null } = { current: null };
  const server = Bun.serve({
    port,
    hostname: "127.0.0.1",
    async fetch(request: Request): Promise<Response> {
      const deps = depsRef.current;
      if (deps === null) {
        return failureResponse({ layer: "transport", reason: "internal_error" });
      }
      return await handleControlPlaneHttpRequest(deps, request);
    },
  });
  const boundPort: number = typeof server.port === "number" ? server.port : port;
  const deps: ControlPlaneHttpServiceDeps = {
    registry,
    bindingStore,
    client,
    port: boundPort,
    bodyLimitBytes,
  };
  depsRef.current = deps;
  const service: RunningControlPlaneHttpService = {
    port: boundPort,
    registry,
    bindingStore,
    client,
    stop: async (): Promise<void> => {
      server.stop(true);
    },
  };
  return Object.freeze(service);
}
