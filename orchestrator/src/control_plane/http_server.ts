/**
 * The versioned loopback HTTP service of the control-plane.
 *
 * This module is the single `Bun.serve` owner of the production tree: it
 * exposes the versioned orchestration routes and the same-origin browser
 * UI assets, and never executes orchestration itself:
 *
 *   HTTP -> project registry -> durable run binding
 *        -> OrchestratorSubprocessClient -> existing CLI subprocess
 *
 *   POST /v1/projects/:projectId/runs      fresh run
 *   GET  /v1/runs/:runId                   inspect
 *   GET  /v1/runs/:runId/events            lifecycle events (read-only,
 *                                          exclusive revision cursor)
 *   POST /v1/runs/:runId/resume
 *   POST /v1/runs/:runId/resume-plan
 *   POST /v1/runs/:runId/continue-stage
 *   POST /v1/runs/:runId/revise-task
 *   POST /v1/runs/:runId/respond
 *   GET  /                                 browser UI document
 *   GET  /ui/app.js                        browser UI script
 *   GET  /ui/style.css                     browser UI stylesheet
 *
 * The three UI routes serve the static assets captured once by the asset
 * owner before `Bun.serve` starts; every UI response carries the fixed
 * same-origin security headers and `Cache-Control: no-store`. The UI is
 * the only browser-facing surface: it consumes the versioned discovery,
 * inspect and fresh-run projections and never sees raw state, host paths
 * or diagnostics.
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
import {
  defaultControlPlaneUiRoot,
  loadControlPlaneUiAssets,
  type ControlPlaneUiAsset,
  type ControlPlaneUiAssets,
  type ControlPlaneUiRoute,
} from "./ui_assets.ts";

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
    | "events"
    | "logs_list"
    | "logs_execution"
    | "resume"
    | "resume_plan"
    | "continue_stage"
    | "revise_task"
    | "respond"
    | "projects"
    | "runs"
    | "ui_root"
    | "ui_app_js"
    | "ui_style_css";
  readonly projectId: string | null;
  readonly runId: string | null;
  /** The execution index of the logs execution route (null elsewhere). */
  readonly executionIndex?: number;
}

/**
 * The fixed security headers of every UI response: a closed
 * same-origin-only content security policy plus the anti-sniffing,
 * referrer, opener and resource-policy headers.
 */
const UI_SECURITY_HEADERS: Readonly<Record<string, string>> = Object.freeze({
  "content-security-policy":
    "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'; object-src 'none'",
  "x-content-type-options": "nosniff",
  "referrer-policy": "no-referrer",
  "cross-origin-opener-policy": "same-origin",
  "cross-origin-resource-policy": "same-origin",
  "cache-control": "no-store",
});

function isControlPlaneUiRoute(kind: ParsedRoute["kind"]): kind is ControlPlaneUiRoute {
  return kind === "ui_root" || kind === "ui_app_js" || kind === "ui_style_css";
}

/** The exact UI path of one route, or null for any other path. */
function controlPlaneUiRouteForPath(pathname: string): ControlPlaneUiRoute | null {
  if (pathname === "/") return "ui_root";
  if (pathname === "/ui/app.js") return "ui_app_js";
  if (pathname === "/ui/style.css") return "ui_style_css";
  return null;
}

/** Attaches the fixed UI security headers to one response (a copy). */
function withUiSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);
  for (const key of Object.keys(UI_SECURITY_HEADERS)) {
    headers.set(key, UI_SECURITY_HEADERS[key] as string);
  }
  return new Response(response.body, { status: response.status, headers });
}

/** One captured UI asset as a response: exact content type, no store. */
function uiAssetResponse(asset: ControlPlaneUiAsset): Response {
  return new Response(asset.bytes, {
    status: 200,
    headers: {
      "content-type": asset.contentType,
      ...UI_SECURITY_HEADERS,
    },
  });
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
  if (pathname === "/") {
    if (method !== "GET") {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the ui document route accepts GET only");
    }
    return { kind: "ui_root", projectId: null, runId: null };
  }
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
  if (matchesShape(["ui", "app.js"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the ui script route accepts GET only");
    }
    return { kind: "ui_app_js", projectId: null, runId: null };
  }
  if (matchesShape(["ui", "style.css"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the ui stylesheet route accepts GET only");
    }
    return { kind: "ui_style_css", projectId: null, runId: null };
  }
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
  if (matchesShape(["v1", "runs", "*", "events"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the events route accepts GET only");
    }
    const runId = safeId(rest[2]);
    if (runId === null) {
      return new ControlPlaneHttpRequestError("invalid_path", "the run id must be a safe identifier");
    }
    return { kind: "events", projectId: null, runId };
  }
  if (matchesShape(["v1", "runs", "*", "logs"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the logs route accepts GET only");
    }
    const runId = safeId(rest[2]);
    if (runId === null) {
      return new ControlPlaneHttpRequestError("invalid_path", "the run id must be a safe identifier");
    }
    return { kind: "logs_list", projectId: null, runId };
  }
  if (matchesShape(["v1", "runs", "*", "logs", "*"])) {
    if (shapeMethodMismatch("GET")) {
      return new ControlPlaneHttpRequestError("method_not_allowed", "the logs route accepts GET only");
    }
    const runId = safeId(rest[2]);
    if (runId === null) {
      return new ControlPlaneHttpRequestError("invalid_path", "the run id must be a safe identifier");
    }
    const executionSegment = rest[4];
    if (executionSegment === undefined || !/^(0|[1-9][0-9]*)$/.test(executionSegment)) {
      return new ControlPlaneHttpRequestError("invalid_path", "the execution index must be a non-negative decimal integer");
    }
    const executionIndex = Number(executionSegment);
    if (!Number.isSafeInteger(executionIndex) || executionIndex < 1) {
      return new ControlPlaneHttpRequestError("invalid_path", "the execution index must be a positive safe integer");
    }
    return { kind: "logs_execution", projectId: null, runId, executionIndex };
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

/**
 * The events query of one run's lifecycle feed: an optional non-negative
 * safe integer cursor (default 0, the exclusive durable revision cursor)
 * and an optional positive safe integer limit (default 100, maximum 200).
 * The raw query string is parsed strictly - no percent-encoding, no bare
 * keys, no duplicates, no empty values, no sign, fraction or exponent,
 * and no unknown fields; the order of the fields is semantically
 * irrelevant.
 */
export function parseEventsQuery(
  search: string,
): { readonly after: number | null; readonly limit: number | null } | ControlPlaneHttpRequestError {
  if (search.startsWith("?")) {
    search = search.slice(1);
  }
  if (search === "") {
    return { after: null, limit: null };
  }
  if (search.includes("%")) {
    return new ControlPlaneHttpRequestError("invalid_request", "the events query must not carry encoded components");
  }
  let after: number | null = null;
  let limit: number | null = null;
  for (const part of search.split("&")) {
    const separator = part.indexOf("=");
    if (separator <= 0) {
      return new ControlPlaneHttpRequestError("invalid_request", "every events query field must carry exactly one value");
    }
    const key = part.slice(0, separator);
    const value = part.slice(separator + 1);
    if (key === "after") {
      if (after !== null) {
        return new ControlPlaneHttpRequestError("invalid_request", "the events query fields must be unique");
      }
      if (!/^(0|[1-9][0-9]*)$/.test(value)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the events query after must be a non-negative safe integer");
      }
      const parsed = Number(value);
      if (!Number.isSafeInteger(parsed)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the events query after must be a non-negative safe integer");
      }
      after = parsed;
      continue;
    }
    if (key === "limit") {
      if (limit !== null) {
        return new ControlPlaneHttpRequestError("invalid_request", "the events query fields must be unique");
      }
      if (!/^[1-9][0-9]*$/.test(value)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the events query limit must be a positive safe integer");
      }
      const parsed = Number(value);
      if (!Number.isSafeInteger(parsed) || parsed > 200) {
        return new ControlPlaneHttpRequestError("invalid_request", "the events query limit must be a positive safe integer up to 200");
      }
      limit = parsed;
      continue;
    }
    return new ControlPlaneHttpRequestError("invalid_request", "the events query carries an unknown field");
  }
  return { after, limit };
}

/**
 * The logs list query: the list route accepts no query at all — any
 * query string is a request failure.
 */
export function parseLogsListQuery(
  search: string,
): { readonly ok: true } | ControlPlaneHttpRequestError {
  if (search.startsWith("?")) {
    search = search.slice(1);
  }
  if (search === "") {
    return { ok: true };
  }
  return new ControlPlaneHttpRequestError("invalid_request", "the logs list route accepts no query");
}

/**
 * The logs execution query: an optional non-negative safe integer offset
 * (default 0, a raw payload byte offset) and an optional positive safe
 * integer limit (default 65536, maximum 262144, applied to raw payload
 * bytes). The raw query string is parsed strictly — no percent-encoding,
 * no bare keys, no duplicates, no empty values, no sign, fraction or
 * exponent, and no unknown fields; the order of the fields is
 * semantically irrelevant.
 */
export function parseLogsExecutionQuery(
  search: string,
): { readonly offset: number | null; readonly limit: number | null } | ControlPlaneHttpRequestError {
  if (search.startsWith("?")) {
    search = search.slice(1);
  }
  if (search === "") {
    return { offset: null, limit: null };
  }
  if (search.includes("%")) {
    return new ControlPlaneHttpRequestError("invalid_request", "the logs query must not carry encoded components");
  }
  let offset: number | null = null;
  let limit: number | null = null;
  for (const part of search.split("&")) {
    const separator = part.indexOf("=");
    if (separator <= 0) {
      return new ControlPlaneHttpRequestError("invalid_request", "every logs query field must carry exactly one value");
    }
    const key = part.slice(0, separator);
    const value = part.slice(separator + 1);
    if (key === "offset") {
      if (offset !== null) {
        return new ControlPlaneHttpRequestError("invalid_request", "the logs query fields must be unique");
      }
      if (!/^(0|[1-9][0-9]*)$/.test(value)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the logs query offset must be a non-negative safe integer");
      }
      const parsed = Number(value);
      if (!Number.isSafeInteger(parsed)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the logs query offset must be a non-negative safe integer");
      }
      offset = parsed;
      continue;
    }
    if (key === "limit") {
      if (limit !== null) {
        return new ControlPlaneHttpRequestError("invalid_request", "the logs query fields must be unique");
      }
      if (!/^[1-9][0-9]*$/.test(value)) {
        return new ControlPlaneHttpRequestError("invalid_request", "the logs query limit must be a positive safe integer");
      }
      const parsed = Number(value);
      if (!Number.isSafeInteger(parsed) || parsed > 262144) {
        return new ControlPlaneHttpRequestError("invalid_request", "the logs query limit must be a positive safe integer up to 262144");
      }
      limit = parsed;
      continue;
    }
    return new ControlPlaneHttpRequestError("invalid_request", "the logs query carries an unknown field");
  }
  return { offset, limit };
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
  if (
    projection.kind === "inspect" ||
    projection.kind === "inspectFailure" ||
    projection.kind === "events" ||
    projection.kind === "eventsFailure" ||
    projection.kind === "logs" ||
    projection.kind === "logsExecution" ||
    projection.kind === "logsChunk" ||
    projection.kind === "logsFailure"
  ) {
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

export interface WorkerLogsProjection {
  readonly schema_version: 1;
  readonly kind: "worker_logs";
  readonly run_id: string;
  readonly revision: number;
  readonly logs: readonly unknown[];
}

export interface WorkerLogProjection {
  readonly schema_version: 1;
  readonly kind: "worker_log";
  readonly run_id: string;
  readonly revision: number;
  readonly execution_index: number;
  readonly state_id: string;
  readonly status: string;
  readonly sha256?: string;
  readonly byte_count?: number;
  readonly truncated_daemon?: boolean;
  readonly truncated_local?: boolean;
  readonly offset?: number;
  readonly data_base64?: string;
  readonly next_offset?: number | null;
}

/** The one snake_case metadata mapper for one validated logs entry. */
function workerLogMetadataEntry(entry: unknown): Record<string, unknown> {
  if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
    return { layer: "transport", reason: "internal_error" } as never;
  }
  const record = entry as Record<string, unknown>;
  const status = record["status"];
  const base: Record<string, unknown> = {
    execution_index: record["executionIndex"],
    state_id: record["stateId"],
    status,
  };
  if (status === "captured" || status === "truncated") {
    base["sha256"] = record["sha256"];
    base["byte_count"] = record["byteCount"];
    base["truncated_daemon"] = record["truncatedDaemon"];
    base["truncated_local"] = record["truncatedLocal"];
  }
  return base;
}

/**
 * The one projection builder for a verified schemaVersion-1 logs list:
 * the metadata union maps to its snake_case equivalent and the raw run
 * root stays dropped. Anything that is not a verified logs projection is
 * a defensive internal failure, never a guessed document.
 */
export function projectControlPlaneLogsList(
  result: OrchestratorCallResult,
): WorkerLogsProjection | CommandOutcomeProjection | ControlPlaneHttpFailure {
  if (!result.ok) {
    return { layer: "transport", reason: result.failure.reason };
  }
  const projection = result.projection;
  if (projection.kind === "logsFailure") {
    if (projection.reason === "invalid_cursor") {
      return { layer: "request", reason: "invalid_cursor" };
    }
    return {
      schema_version: 1,
      kind: "command_result",
      run_id: projection.runId,
      outcome: "failed",
      exit_code: projection.exitCode,
      reason: projection.reason,
    } satisfies CommandOutcomeProjection;
  }
  if (projection.kind !== "logs" || projection.schemaVersion !== 1) {
    return { layer: "transport", reason: "internal_error" };
  }
  return {
    schema_version: 1,
    kind: "worker_logs",
    run_id: projection.runId,
    revision: projection.revision,
    logs: projection.logs.map(workerLogMetadataEntry),
  };
}

/**
 * The one projection builder for a verified schemaVersion-1 logs
 * execution read: a pending/unavailable execution carries the status
 * keys only, a captured/truncated chunk carries the digest, count and
 * truncation flags plus the bounded base64 slice and the cursor; the raw
 * run root stays dropped. Anything else is a defensive internal failure.
 */
export function projectControlPlaneLogsExecution(
  result: OrchestratorCallResult,
): WorkerLogProjection | CommandOutcomeProjection | ControlPlaneHttpFailure {
  if (!result.ok) {
    return { layer: "transport", reason: result.failure.reason };
  }
  const projection = result.projection;
  if (projection.kind === "logsFailure") {
    if (projection.reason === "invalid_cursor") {
      return { layer: "request", reason: "invalid_cursor" };
    }
    return {
      schema_version: 1,
      kind: "command_result",
      run_id: projection.runId,
      outcome: "failed",
      exit_code: projection.exitCode,
      reason: projection.reason,
    } satisfies CommandOutcomeProjection;
  }
  if (projection.kind !== "logsExecution" && projection.kind !== "logsChunk") {
    return { layer: "transport", reason: "internal_error" };
  }
  if (projection.schemaVersion !== 1) {
    return { layer: "transport", reason: "internal_error" };
  }
  if (projection.kind === "logsExecution") {
    return {
      schema_version: 1,
      kind: "worker_log",
      run_id: projection.runId,
      revision: projection.revision,
      execution_index: projection.executionIndex,
      state_id: projection.stateId,
      status: projection.status,
    };
  }
  if (projection.kind !== "logsChunk") {
    return { layer: "transport", reason: "internal_error" };
  }
  return {
    schema_version: 1,
    kind: "worker_log",
    run_id: projection.runId,
    revision: projection.revision,
    execution_index: projection.executionIndex,
    state_id: projection.stateId,
    status: projection.status,
    sha256: projection.sha256,
    byte_count: projection.byteCount,
    truncated_daemon: projection.truncatedDaemon,
    truncated_local: projection.truncatedLocal,
    offset: projection.offset,
    data_base64: projection.dataBase64,
    next_offset: projection.nextOffset,
  };
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

/** The versioned events projection of one run's lifecycle feed. */
export interface EventsProjection {
  readonly schema_version: 1;
  readonly kind: "events";
  readonly run_id: string;
  readonly revision: number;
  readonly events: readonly unknown[];
  readonly next_after: number | null;
}

/**
 * The one projection builder for a verified schemaVersion-1 events
 * projection: the event entries are copied without any new
 * classification and the run root is already dropped by the client. A
 * typed `invalid_cursor` failure is the one explicit events-layer
 * mapping: the request-layer 400 envelope, never a 502 transport
 * failure; every other events failure follows the inspect precedent (a
 * correctly received failed outcome as a 200 command result).
 */
export function projectControlPlaneEvents(
  result: OrchestratorCallResult,
): EventsProjection | CommandOutcomeProjection | ControlPlaneHttpFailure {
  if (!result.ok) {
    return { layer: "transport", reason: result.failure.reason };
  }
  const projection = result.projection;
  if (projection.kind === "eventsFailure") {
    if (projection.reason === "invalid_cursor") {
      return { layer: "request", reason: "invalid_cursor" };
    }
    return {
      schema_version: 1,
      kind: "command_result",
      run_id: projection.runId,
      outcome: "failed",
      exit_code: projection.exitCode,
      reason: projection.reason,
    } satisfies CommandOutcomeProjection;
  }
  if (projection.kind !== "events" || projection.schemaVersion !== 1) {
    return { layer: "transport", reason: "internal_error" };
  }
  return {
    schema_version: 1,
    kind: "events",
    run_id: projection.runId,
    revision: projection.revision,
    events: projection.events,
    next_after: projection.nextAfter,
  };
}

function projectionResponse(
  projection:
    | CommandOutcomeProjection
    | RunProjection
    | ProjectsDiscoveryProjection
    | RunsDiscoveryProjection
    | EventsProjection
    | WorkerLogsProjection
    | WorkerLogProjection
    | ControlPlaneHttpFailure,
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
  /** The captured static UI assets, loaded once before `Bun.serve`. */
  readonly assets: ControlPlaneUiAssets;
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
    const respondUi = (response: Response): Response =>
      controlPlaneUiRouteForPath(url.pathname) === null ? response : withUiSecurityHeaders(response);
    const parsed = parseControlPlaneRoute(url.pathname, request.method);
    if (parsed instanceof ControlPlaneHttpRequestError) {
      return respondUi(requestFailure(parsed.reason));
    }
    if (
      parsed.kind !== "projects" &&
      parsed.kind !== "runs" &&
      parsed.kind !== "events" &&
      parsed.kind !== "logs_list" &&
      parsed.kind !== "logs_execution" &&
      url.search !== ""
    ) {
      return respondUi(requestFailure("invalid_path"));
    }
    if (request.method === "POST") {
      const contentType = request.headers.get("content-type");
      if (contentType !== "application/json") {
        return respondUi(requestFailure("unsupported_media_type"));
      }
    } else {
      const lengthHeader = request.headers.get("content-length");
      if (lengthHeader !== null && lengthHeader !== "0") {
        return respondUi(requestFailure("invalid_request"));
      }
    }
    if (isControlPlaneUiRoute(parsed.kind)) {
      return respondUi(uiAssetResponse(deps.assets[parsed.kind]));
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
      parsed.kind === "projects" ||
      parsed.kind === "runs" ||
      parsed.kind === "events" ||
      parsed.kind === "logs_list" ||
      parsed.kind === "logs_execution"
        ? { ...deps, discoveryQuery: url.search }
        : deps,
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
    case "events": {
      const query = parseEventsQuery(deps.discoveryQuery ?? "");
      if (query instanceof ControlPlaneHttpRequestError) {
        return requestFailure(query.reason);
      }
      const command: OrchestratorCommand = {
        kind: "events",
        runId,
        ...(query.after === null ? {} : { after: query.after }),
        ...(query.limit === null ? {} : { limit: query.limit }),
      };
      const result = await deps.client.run(command);
      return projectionResponse(projectControlPlaneEvents(result));
    }
    case "logs_list": {
      const query = parseLogsListQuery(deps.discoveryQuery ?? "");
      if (query instanceof ControlPlaneHttpRequestError) {
        return requestFailure(query.reason);
      }
      const result = await deps.client.run({ kind: "logs", runId });
      return projectionResponse(projectControlPlaneLogsList(result));
    }
    case "logs_execution": {
      const query = parseLogsExecutionQuery(deps.discoveryQuery ?? "");
      if (query instanceof ControlPlaneHttpRequestError) {
        return requestFailure(query.reason);
      }
      const executionIndex = route.executionIndex as number;
      const command: OrchestratorCommand = {
        kind: "logs",
        runId,
        executionIndex,
        ...(query.offset === null ? {} : { offset: query.offset }),
        ...(query.limit === null ? {} : { limit: query.limit }),
      };
      const result = await deps.client.run(command);
      return projectionResponse(projectControlPlaneLogsExecution(result));
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

/** The exact listen-address vocabulary: loopback (default) or the container wildcard. */
export type ControlPlaneListenAddress = "127.0.0.1" | "0.0.0.0";

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
  /**
   * The exact bind address of the single `Bun.serve` socket. Absent means
   * the established loopback default (`127.0.0.1`); the only other
   * accepted value is `0.0.0.0`, which allows a container port
   * forwarding to reach the service. The browser authority (the
   * `Host`/`Origin` gates) stays `127.0.0.1:<port>` in both modes.
   */
  readonly listenAddress?: ControlPlaneListenAddress;
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
 * immutable subprocess client, captures the static UI assets once and
 * serves the versioned routes plus the UI assets. The bind address is
 * the established loopback default (`127.0.0.1`) unless the
 * configuration explicitly selects the container wildcard (`0.0.0.0`);
 * the browser authority (`Host`/`Origin` gates) is `127.0.0.1:<port>` in
 * both modes. There is no hot reload, no discovery and no second
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
  const listenAddress = config.listenAddress === undefined ? "127.0.0.1" : config.listenAddress;
  if (listenAddress !== "127.0.0.1" && listenAddress !== "0.0.0.0") {
    throw new ControlPlaneHttpServiceError("invalid_service_config", "the listen address must be exactly 127.0.0.1 or 0.0.0.0");
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
  // The static UI assets are captured once, before the server starts:
  // the request handler never touches the disk.
  const assets = loadControlPlaneUiAssets(defaultControlPlaneUiRoot());
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
    hostname: listenAddress,
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
    assets,
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
