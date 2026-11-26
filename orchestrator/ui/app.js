/**
 * Browser UI (v1) of the orchestrator control-plane.
 *
 * One self-contained ES module: no framework, no bundler, no npm
 * dependencies, no build step, no external origins.
 *
 * Structure:
 *
 * - the pure API/UI model is the exported function surface. It validates
 *   the versioned HTTP documents (schema_version 1, a known `kind`, the
 *   exact field shape per kind), keeps the run-list pagination state,
 *   builds the exact fresh-run request body, and describes the
 *   lost-response recovery. It never touches the DOM, never classifies
 *   message text, never parses headers as orchestration status and never
 *   retains a raw response after the UI model is built;
 * - the thin DOM adapter below renders the models through
 *   `createElement`/`textContent` only and boots only in a browser
 *   (where `document` exists); under a test runner it is inert.
 * - the inspect transport is the one single-flight admission owner of
 *   every inspect GET of the module: exactly one active wire request
 *   and one latest desired request. A desire that arrives while a
 *   request is on the wire supersedes the active result (its response
 *   can never be applied) and coalesces into the latest desired
 *   request; after the active request settles — fulfil, reject or
 *   abort rejection — the wire slot is released exactly once, the
 *   result is applied only when the request was never superseded, and
 *   at most one coalesced follow-up starts. A new inspect fetch never
 *   begins while the previous one is unsettled, even when the previous
 *   result is already superseded.
 * - the live observation (v3) is one pure scheduling owner: it repeats
 *   the read-only inspect GET of the selected run at a fixed interval
 *   only while the last applied authoritative inspect shows `running`
 *   with boundary kind `in_flight`, the tab is visible, live updates
 *   are enabled and no mutation is in flight. Any other boundary, any
 *   error and any lost response stop it until an explicit user action;
 *   it never sends a mutating request and never retries one.
 * - the lifecycle timeline (v4) renders the read-only events feed of
 *   the selected run (`GET /v1/runs/:runId/events`) through its own
 *   document parser and its own single-flight transport. It owns the
 *   dense cursor/pagination merge, the explicit Load more / Reload /
 *   Retry actions and the fail-closed error stop; the only automatic
 *   fetch is the one tail request an applied authoritative inspect may
 *   trigger when the timeline is caught up and behind. The timeline
 *   never sends a mutating request, never creates a timer, never polls
 *   on its own and never retries automatically.
 *
 * Security posture: all inserted content travels through textContent
 * (no direct HTML injection, no dynamic code evaluation, no external
 * origins, no workers, no websockets); failures are
 * shown as `layer: reason` strings only. The only browser storage is
 * the one closed-namespace Project Apply recovery record
 * (`orchestrator.apply.v1:*`, Browser UI v8) holding the run and apply
 * ids and the observation — never paths, contents or secrets. The
 * fresh-run POST is never
 * retried automatically: a lost response is recovered exclusively
 * through the safe GET inspect of the caller-owned run id, which is
 * created and displayed before the request is sent.
 */

const SCHEMA_VERSION = 1;
/** The projects discovery document version: 2 since the effective
 * Project Apply capability flag became a required project field. */
const PROJECTS_SCHEMA_VERSION = 2;
const SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
const RUNS_PAGE_LIMIT = 100;
const MAX_DOCUMENT_BYTES = 1048576;

const INSPECT_STATUSES = ["running", "waiting", "succeeded", "failed"];
const COMMAND_OUTCOMES = ["succeeded", "waiting", "plan_ready", "responded", "failed"];
const COMMAND_EXIT_CODES = [0, 1, 130, 143];
const ERROR_LAYERS = ["request", "registry", "binding", "transport"];
const OUTPUT_TYPES = ["file", "directory", "json"];

/** The fixed page limit of every timeline events request. */
export const EVENTS_PAGE_LIMIT = 100;

/**
 * The closed public event-kind vocabulary of the lifecycle timeline:
 * the 28 durable commit provenance kinds of pipeline state schema v8.
 * The vocabulary is pinned against the real endpoint by the permanent
 * integration suite; the timeline renders the public `kind` verbatim
 * and carries no second display-label set.
 */
export const PIPELINE_V2_EVENT_KINDS = Object.freeze([
  "create_run",
  "start_agent_execution",
  "agent_data_prepared",
  "agent_execution_session_created",
  "agent_tool_session_created",
  "agent_running",
  "agent_outputs_accepted",
  "agent_cleanup_completed",
  "agent_failed",
  "start_decision_execution",
  "decision_evaluated",
  "decision_failed",
  "transition_committed",
  "run_waiting",
  "wait_response_recorded",
  "terminal_reached",
  "run_outputs_published",
  "run_succeeded",
  "run_failed",
  "run_cleanup_failed",
  "stage_generation_opened",
  "stage_iteration_opened",
  "stage_iteration_closed",
  "stage_generation_closed",
  "plan_intent_accepted",
  "task_revision_accepted",
  "plan_revision_accepted",
  "iteration_grant_recorded",
]);

/**
 * The exact reference keys each event kind may carry (the minimal
 * content-free provenance references of schema v8). Every entry must
 * carry exactly its kind's keys — no more, no fewer.
 */
const EVENT_REFERENCE_KEYS = Object.freeze({
  create_run: [],
  start_agent_execution: ["execution_index", "state_id"],
  start_decision_execution: ["execution_index", "state_id"],
  agent_data_prepared: ["execution_index"],
  agent_execution_session_created: ["execution_index"],
  agent_tool_session_created: ["execution_index"],
  agent_running: ["execution_index"],
  agent_outputs_accepted: ["execution_index"],
  agent_cleanup_completed: ["execution_index"],
  agent_failed: ["execution_index"],
  decision_evaluated: ["execution_index"],
  decision_failed: ["execution_index"],
  transition_committed: ["execution_index", "transition_index"],
  run_waiting: ["wait_index"],
  wait_response_recorded: ["wait_index"],
  terminal_reached: [],
  run_outputs_published: [],
  run_succeeded: [],
  run_failed: [],
  run_cleanup_failed: [],
  stage_generation_opened: ["generation_index"],
  stage_iteration_opened: ["generation_index", "iteration_index"],
  stage_iteration_closed: ["generation_index", "iteration_index"],
  stage_generation_closed: ["generation_index"],
  plan_intent_accepted: ["wait_index"],
  task_revision_accepted: ["task_index"],
  plan_revision_accepted: ["plan_index"],
  iteration_grant_recorded: ["grant_index"],
});

/** The canonical presentation order of the reference columns. */
const EVENT_REFERENCE_ORDER = Object.freeze([
  "execution_index",
  "state_id",
  "transition_index",
  "wait_index",
  "generation_index",
  "iteration_index",
  "task_index",
  "plan_index",
  "grant_index",
]);

/** The exact reference keys of one event kind, or null when unknown. */
export function eventReferenceKeys(kind) {
  if (typeof kind !== "string" || !Object.hasOwn(EVENT_REFERENCE_KEYS, kind)) {
    return null;
  }
  return EVENT_REFERENCE_KEYS[kind];
}

/** A UI-side contract violation: stable reason, no echoed values. */
export class UiContractError extends Error {
  constructor(reason) {
    super(`ui contract violation: ${reason}`);
    this.name = "UiContractError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** The single safe-identifier grammar of the browser client. */
export function isSafeId(value) {
  return typeof value === "string" && value !== "" && SAFE_ID_PATTERN.test(value) && !value.includes("..");
}

function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function expectShape(record, keys) {
  const own = Object.keys(record);
  if (own.length !== keys.length) {
    throw new UiContractError("invalid_shape");
  }
  for (const key of keys) {
    if (!Object.hasOwn(record, key)) {
      throw new UiContractError("invalid_shape");
    }
  }
}

function expectString(record, key) {
  const value = record[key];
  if (typeof value !== "string" || value === "") {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectBoolean(record, key) {
  const value = record[key];
  if (typeof value !== "boolean") {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectNonNegativeInteger(record, key) {
  const value = record[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectRecord(record, key) {
  const value = record[key];
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectArray(record, key) {
  const value = record[key];
  if (!Array.isArray(value)) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectOneOf(record, key, allowed) {
  const value = record[key];
  if (typeof value !== "string" || !allowed.includes(value)) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function parseProjectsDocument(document) {
  expectShape(document, ["schema_version", "kind", "projects"]);
  const modelProjects = expectArray(document, "projects").map((entry) => {
    if (!isRecord(entry)) {
      throw new UiContractError("invalid_shape");
    }
    expectShape(entry, ["id", "input_roots", "project_apply"]);
    const inputRootIds = expectArray(entry, "input_roots").map((root) => {
      if (!isRecord(root)) {
        throw new UiContractError("invalid_shape");
      }
      expectShape(root, ["id"]);
      return expectString(root, "id");
    });
    return Object.freeze({
      id: expectString(entry, "id"),
      inputRootIds: Object.freeze(inputRootIds),
      projectApply: expectBoolean(entry, "project_apply"),
    });
  });
  return Object.freeze({ kind: "projects", projects: Object.freeze(modelProjects) });
}

function parseRunsDocument(document) {
  expectShape(document, ["schema_version", "kind", "runs", "next_after"]);
  const runs = expectArray(document, "runs").map((entry) => {
    if (!isRecord(entry)) {
      throw new UiContractError("invalid_shape");
    }
    expectShape(entry, ["run_id", "project_id", "project_available"]);
    return Object.freeze({
      runId: expectString(entry, "run_id"),
      projectId: expectString(entry, "project_id"),
      projectAvailable: expectBoolean(entry, "project_available"),
    });
  });
  const nextAfter = document["next_after"];
  if (nextAfter !== null && !isSafeId(nextAfter)) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({ kind: "runs", runs: Object.freeze(runs), nextAfter });
}

function parseRunDocument(document) {
  expectShape(document, ["schema_version", "kind", "run_id", "revision", "status", "boundary", "outputs"]);
  const outputs = expectArray(document, "outputs").map((entry) => {
    if (!isRecord(entry)) {
      throw new UiContractError("invalid_shape");
    }
    const own = Object.keys(entry);
    const hasDigest = Object.hasOwn(entry, "digest");
    if (own.length !== (hasDigest ? 5 : 4)) {
      throw new UiContractError("invalid_shape");
    }
    expectShape(entry, hasDigest ? ["id", "type", "required", "present", "digest"] : ["id", "type", "required", "present"]);
    const digest = hasDigest ? expectString(entry, "digest") : null;
    if (digest !== null && !/^[0-9a-f]{64}$/.test(digest)) {
      throw new UiContractError("invalid_shape");
    }
    return Object.freeze({
      id: expectString(entry, "id"),
      type: expectOneOf(entry, "type", OUTPUT_TYPES),
      required: expectBoolean(entry, "required"),
      present: expectBoolean(entry, "present"),
      digest,
    });
  });
  return Object.freeze({
    kind: "run",
    runId: expectString(document, "run_id"),
    revision: expectNonNegativeInteger(document, "revision"),
    status: expectOneOf(document, "status", INSPECT_STATUSES),
    boundary: Object.freeze(expectRecord(document, "boundary")),
    outputs: Object.freeze(outputs),
  });
}

function parseCommandResultDocument(document) {
  const hasReason = Object.hasOwn(document, "reason");
  expectShape(document, hasReason ? ["schema_version", "kind", "run_id", "outcome", "exit_code", "reason"] : ["schema_version", "kind", "run_id", "outcome", "exit_code"]);
  const outcome = expectOneOf(document, "outcome", COMMAND_OUTCOMES);
  const exitCode = expectNonNegativeInteger(document, "exit_code");
  if (!COMMAND_EXIT_CODES.includes(exitCode)) {
    throw new UiContractError("invalid_shape");
  }
  if (outcome === "failed" ? exitCode === 0 : exitCode !== 0) {
    throw new UiContractError("invalid_shape");
  }
  if (outcome === "failed" ? !hasReason : hasReason) {
    throw new UiContractError("invalid_shape");
  }
  const reason = hasReason ? expectString(document, "reason") : null;
  return Object.freeze({
    kind: "command_result",
    runId: expectString(document, "run_id"),
    outcome,
    exitCode,
    reason,
  });
}

function parseErrorDocument(document) {
  expectShape(document, ["schema_version", "kind", "layer", "reason"]);
  return Object.freeze({
    kind: "error",
    layer: expectOneOf(document, "layer", ERROR_LAYERS),
    reason: expectString(document, "reason"),
  });
}

/* ------------------------------------------------------------------ */
/* Lifecycle events (v4): the one events document contract owner.      */
/* ------------------------------------------------------------------ */

function expectPositiveInteger(record, key) {
  const value = record[key];
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function parseEventEntry(entry) {
  if (!isRecord(entry)) {
    throw new UiContractError("invalid_shape");
  }
  const kind = entry["kind"];
  if (typeof kind !== "string" || !Object.hasOwn(EVENT_REFERENCE_KEYS, kind)) {
    throw new UiContractError("unknown_event_kind");
  }
  const allowed = EVENT_REFERENCE_KEYS[kind];
  const own = Object.keys(entry);
  if (own.length !== allowed.length + 2 || !Object.hasOwn(entry, "revision")) {
    throw new UiContractError("invalid_shape");
  }
  const projection = { revision: expectPositiveInteger(entry, "revision"), kind };
  for (const key of allowed) {
    if (!Object.hasOwn(entry, key)) {
      throw new UiContractError("invalid_shape");
    }
    projection[key] = key === "state_id" ? expectString(entry, key) : expectPositiveInteger(entry, key);
    if (key === "state_id" && !isSafeId(projection[key])) {
      throw new UiContractError("invalid_shape");
    }
  }
  return Object.freeze(projection);
}

/**
 * The one parser of the versioned events document (schema 1). It
 * accepts only `kind:"events"` with exact own keys at every level, a
 * positive-safe-integer revision, exact reference shapes per event
 * kind, and a `run_id` equal to the selected run. The history order is
 * preserved verbatim; the returned projection is frozen and carries no
 * raw response material.
 */
export function parseEventsDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "events") {
    throw new UiContractError("unknown_kind");
  }
  expectShape(document, ["schema_version", "kind", "run_id", "revision", "events", "next_after"]);
  const runId = expectString(document, "run_id");
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_shape");
  }
  if (typeof expectedRunId !== "string" || runId !== expectedRunId) {
    throw new UiContractError("unexpected_run");
  }
  const events = expectArray(document, "events").map(parseEventEntry);
  const nextAfter = document["next_after"];
  if (nextAfter !== null && (typeof nextAfter !== "number" || !Number.isSafeInteger(nextAfter) || nextAfter < 1)) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({
    kind: "events",
    runId,
    revision: expectPositiveInteger(document, "revision"),
    events: Object.freeze(events),
    nextAfter,
  });
}

/**
 * The one document classifier of the events endpoint: a well-formed
 * `events` document, or one of the endpoint's two documented failure
 * envelopes (`error`, or the failed `command_result` of a rejected
 * events read). A succeeded command result is unsuitable and fails
 * closed; every other kind fails closed. The expected run id binds the
 * events document and the failed command result to the selected run.
 */
function parseEventsEndpointDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "events") {
    return parseEventsDocument(document, expectedRunId);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/** The one URL builder of every timeline events request. */
export function eventsRequestPath(runId, after) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  if (typeof after !== "number" || !Number.isSafeInteger(after) || after < 0) {
    throw new UiContractError("invalid_cursor");
  }
  return `/v1/runs/${runId}/events?after=${after}&limit=${EVENTS_PAGE_LIMIT}`;
}

/**
 * The one document classifier: schema_version 1 and a known kind are
 * required before any per-kind shape is read; anything else is a UI
 * contract error and never a guessed model.
 */
export function parseApiDocument(document) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  // The projects discovery moved honestly to schema version 2 when the
  // effective Project Apply capability flag became a required field;
  // every other kind stays at version 1. An old or foreign form fails
  // closed — no silent unknown field in either version.
  const expectedVersion = document["kind"] === "projects" ? PROJECTS_SCHEMA_VERSION : SCHEMA_VERSION;
  if (document["schema_version"] !== expectedVersion) {
    throw new UiContractError("unknown_schema_version");
  }
  switch (document["kind"]) {
    case "projects":
      return parseProjectsDocument(document);
    case "runs":
      return parseRunsDocument(document);
    case "run":
      return parseRunDocument(document);
    case "command_result":
      return parseCommandResultDocument(document);
    case "error":
      return parseErrorDocument(document);
    default:
      throw new UiContractError("unknown_kind");
  }
}

/** The empty run-list state (the Refresh starting point). */
export function createRunsState() {
  return Object.freeze({ runs: Object.freeze([]), nextAfter: null });
}

/** Appends one page to the run-list state; Refresh rebuilds from scratch. */
export function applyRunsPage(state, page) {
  if (!isRecord(state) || !Array.isArray(state.runs)) {
    throw new UiContractError("invalid_state");
  }
  if (!isRecord(page) || page["kind"] !== "runs" || !Array.isArray(page["runs"])) {
    throw new UiContractError("invalid_state");
  }
  return Object.freeze({
    runs: Object.freeze([...state.runs, ...page["runs"]]),
    nextAfter: page["nextAfter"] === undefined ? null : page["nextAfter"],
  });
}

/** The discovery path of one runs page. */
export function runsPagePath(nextAfter) {
  return nextAfter === null ? `/v1/runs?limit=${RUNS_PAGE_LIMIT}` : `/v1/runs?limit=${RUNS_PAGE_LIMIT}&after=${nextAfter}`;
}

/** The fresh-run path of one project. */
export function freshRunPath(projectId) {
  if (!isSafeId(projectId)) {
    throw new UiContractError("invalid_project");
  }
  return `/v1/projects/${projectId}/runs`;
}

/**
 * The exact fresh-run request body: `{run_id, inputs[] {id, root_id,
 * ref}}` in declaration order, every field a safe identifier, ids
 * unique. No filesystem path, launcher identity, environment value,
 * secret material or task body is ever accepted.
 */
export function buildFreshRunBody(projectId, runId, inputs) {
  if (!isSafeId(projectId)) {
    throw new UiContractError("invalid_project");
  }
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  if (!Array.isArray(inputs) || inputs.length === 0) {
    throw new UiContractError("invalid_inputs");
  }
  const seenIds = [];
  const wireInputs = inputs.map((row) => {
    if (!isRecord(row)) {
      throw new UiContractError("invalid_inputs");
    }
    const id = row["id"];
    const rootId = row["rootId"];
    const ref = row["ref"];
    if (!isSafeId(id) || !isSafeId(rootId) || !isSafeId(ref)) {
      throw new UiContractError("invalid_inputs");
    }
    for (const seen of seenIds) {
      if (seen === id) {
        throw new UiContractError("invalid_inputs");
      }
    }
    seenIds.push(id);
    return Object.freeze({ id, root_id: rootId, ref });
  });
  return Object.freeze({ run_id: runId, inputs: Object.freeze(wireInputs) });
}

/**
 * The lost-response recovery plan: never a second POST, only the safe
 * GET inspect of the known caller-owned run id.
 */
export function lostResponseRecovery(runId) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  return Object.freeze({ kind: "inspect_only", runId, postAgain: false, inspectPath: `/v1/runs/${runId}` });
}

/** Failures are shown as `layer: reason` and nothing else. */
export function formatFailure(failure) {
  if (!isRecord(failure) || typeof failure["layer"] !== "string" || typeof failure["reason"] !== "string" || failure["reason"] === "") {
    return "transport: unknown_failure";
  }
  return `${failure["layer"]}: ${failure["reason"]}`;
}

class ResponseTooLarge extends Error {}

async function readBoundedText(response) {
  if (response.body === null) {
    return "";
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    if (value !== undefined) {
      total += value.byteLength;
      if (total > MAX_DOCUMENT_BYTES) {
        try {
          await reader.cancel();
        } catch {
          // the connection is already going away; the size refusal stands
        }
        throw new ResponseTooLarge();
      }
      text += decoder.decode(value, { stream: true });
    }
  }
  text += decoder.decode();
  return text;
}

/**
 * The one wire owner of every API GET of the module: it checks the HTTP
 * content type, reads the body bounded, requires JSON, and hands the
 * parsed document to the endpoint's own document classifier — the
 * general API client classifies with `parseApiDocument`, the events
 * client with the events endpoint classifier, so the expected response
 * kind stays bound to the called endpoint. A mutating request whose
 * delivery is unknown (network failure after send) is reported as
 * `lost`, which the caller must treat as outcome-unknown and recover
 * through inspect only.
 */
async function requestJsonDocument(fetchImpl, path, init, parseDocument) {
  const method = init !== undefined && init !== null && typeof init.method === "string" ? init.method : "GET";
  let response;
  try {
    response = await fetchImpl(path, init);
  } catch {
    return Object.freeze({
      ok: false,
      lost: method !== "GET",
      failure: Object.freeze({ layer: "transport", reason: "network_error" }),
    });
  }
  const contentType = response.headers.get("content-type");
  if (typeof contentType !== "string" || !contentType.toLowerCase().startsWith("application/json")) {
    return Object.freeze({
      ok: false,
      lost: false,
      failure: Object.freeze({ layer: "transport", reason: "unexpected_content_type" }),
    });
  }
  let text;
  try {
    text = await readBoundedText(response);
  } catch (error) {
    if (error instanceof ResponseTooLarge) {
      return Object.freeze({
        ok: false,
        lost: false,
        failure: Object.freeze({ layer: "transport", reason: "response_too_large" }),
      });
    }
    return Object.freeze({
      ok: false,
      lost: method !== "GET",
      failure: Object.freeze({ layer: "transport", reason: "network_error" }),
    });
  }
  let document;
  try {
    document = JSON.parse(text);
  } catch {
    return Object.freeze({
      ok: false,
      lost: false,
      failure: Object.freeze({ layer: "transport", reason: "malformed_response" }),
    });
  }
  try {
    return Object.freeze({ ok: true, lost: false, model: parseDocument(document) });
  } catch (error) {
    if (error instanceof UiContractError) {
      return Object.freeze({
        ok: false,
        lost: false,
        failure: Object.freeze({ layer: "contract", reason: error.reason }),
      });
    }
    throw error;
  }
}

/**
 * The general API client: inspect, discovery and mutation endpoints.
 * It never accepts an `events` document — the expected response kind
 * stays bound to the called endpoint.
 */
export async function requestApiJson(fetchImpl, path, init) {
  return requestJsonDocument(fetchImpl, path, init, parseApiDocument);
}

/**
 * The events endpoint client: the lifecycle feed of one run. It accepts
 * only the events document of that run (or the endpoint's documented
 * failure envelopes) and never a run, discovery or succeeded mutation
 * document.
 */
export async function requestEventsJson(fetchImpl, path, init, expectedRunId) {
  return requestJsonDocument(fetchImpl, path, init, (document) => parseEventsEndpointDocument(document, expectedRunId));
}

/* ------------------------------------------------------------------ */
/* Lifecycle history (v9): the identity and execution-binding streams.  */
/* Contract owners of the P-3 identity documents and the S5-P0          */
/* execution-binding documents. Both pages are revision-pinned; the     */
/* expected response kind stays bound to the called endpoint.           */
/* ------------------------------------------------------------------ */

const IDENTITY_SCHEMA_VERSION = 1;
const IDENTITY_PAGE_LIMIT = 100;
const IDENTITY_MAX_LIMIT = 200;

/** The closed 12-kind identity record vocabulary with the exact own-key
 * shapes of every kind (bijective with `PIPELINE_V2_IDENTITY_RECORD_KINDS`
 * of `src/pipeline_v2_identity.ts`). Every value is content-free: safe
 * ids, lowercase digests, closed-vocabulary strings and bounded numbers. */
const IDENTITY_RECORD_KEYS = Object.freeze({
  plan_revision: Object.freeze(["index", "revision", "sha256", "previous_sha256", "origin_execution"]),
  task_revision: Object.freeze(["index", "task_id", "revision", "sha256", "previous_sha256", "wait_index", "intent_sha256"]),
  generation: Object.freeze([
    "index",
    "stage_id",
    "stage_position",
    "template_id",
    "plan_sha256",
    "initial_budget",
    "opened_transition_count",
    "iteration_count",
    "open_iteration",
    "closed",
  ]),
  iteration: Object.freeze(["generation_index", "index", "opened_transition_count", "closed"]),
  grant: Object.freeze(["index", "generation_index", "wait_index", "intent_sha256", "additional_iterations"]),
  wait: Object.freeze([
    "index",
    "transition_count",
    "state_id",
    "reason",
    "request_sha256",
    "intent_sha256",
    "response",
  ]),
  wait_action: Object.freeze(["wait_index", "id", "to"]),
  execution_agent: Object.freeze(["index", "type", "state_id", "execution_role", "iteration_index", "phase", "stage_task"]),
  execution_decision: Object.freeze([
    "index",
    "type",
    "state_id",
    "execution_role",
    "iteration_index",
    "phase",
    "input_digest",
    "result",
  ]),
  agent_output: Object.freeze(["execution_index", "id", "digest"]),
  decision_active_constraint: Object.freeze(["execution_index", "position", "id"]),
  decision_violated_relation: Object.freeze(["execution_index", "position", "id"]),
});

function expectSafeIdString(record, key) {
  const value = expectString(record, key);
  if (!isSafeId(value)) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectDigest(value) {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectDigestOrNull(value) {
  if (value === null) {
    return null;
  }
  return expectDigest(value);
}

function expectIndexOrNull(value) {
  if (value === null) {
    return null;
  }
  return expectPositiveInteger({ value }, "value");
}

function expectIdOrNull(value) {
  if (value === null) {
    return null;
  }
  return expectSafeIdString({ value }, "value");
}

function expectTextValue(value) {
  const text = typeof value === "string" ? value : null;
  if (text === null || text.length === 0) {
    throw new UiContractError("invalid_shape");
  }
  return text;
}

function expectTextOrNull(value) {
  if (value === null) {
    return null;
  }
  return expectTextValue(value);
}

function expectStageTask(value) {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  expectShape(value, ["task_id", "task_revision", "task_manifest_sha256", "body_sha256"]);
  return Object.freeze({
    taskId: expectSafeIdString(value, "task_id"),
    taskRevision: expectPositiveInteger(value, "task_revision"),
    taskManifestSha256: expectDigest(value["task_manifest_sha256"]),
    bodySha256: expectDigest(value["body_sha256"]),
  });
}

function expectCount(value) {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new UiContractError("invalid_shape");
  }
  return value;
}

function expectGenerationOpenIteration(value) {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  expectShape(value, ["index", "opened_transition_count"]);
  return Object.freeze({
    index: expectPositiveInteger(value, "index"),
    openedTransitionCount: expectCount(value["opened_transition_count"]),
  });
}

function expectGenerationClosure(value) {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  expectShape(value, ["by", "closed_transition_count"]);
  return Object.freeze({ by: expectTextOrNull(value["by"]), closedTransitionCount: expectCount(value["closed_transition_count"]) });
}

function expectIterationClosure(value) {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  expectShape(value, ["by", "wait_index", "closed_transition_count"]);
  return Object.freeze({
    by: expectTextOrNull(value["by"]),
    waitIndex: expectIndexOrNull(value["wait_index"]),
    closedTransitionCount: expectCount(value["closed_transition_count"]),
  });
}

function expectWaitResponse(value) {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  expectShape(value, ["action_id", "response_sha256"]);
  return Object.freeze({
    actionId: expectSafeIdString(value, "action_id"),
    responseSha256: expectDigest(value["response_sha256"]),
  });
}

function expectDecisionResult(value) {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  const status = value["status"];
  if (status === "selected") {
    expectShape(value, ["status", "outcome", "decision", "rule_id"]);
    return Object.freeze({
      status,
      outcome: expectSafeIdString(value, "outcome"),
      decision: expectSafeIdString(value, "decision"),
      ruleId: expectSafeIdString(value, "rule_id"),
    });
  }
  if (status === "uncovered" || status === "inconsistent_facts") {
    expectShape(value, ["status", "outcome"]);
    if (value["outcome"] !== status) {
      throw new UiContractError("invalid_shape");
    }
    return Object.freeze({ status, outcome: status });
  }
  if (status === "invalid_facts") {
    expectShape(value, ["status", "outcome", "reason", "fact_id", "actual_type"]);
    if (value["outcome"] !== status) {
      throw new UiContractError("invalid_shape");
    }
    return Object.freeze({
      status,
      outcome: status,
      reason: expectTextOrNull(value["reason"]),
      factId: expectIdOrNull(value["fact_id"]),
      actualType: expectIdOrNull(value["actual_type"]),
    });
  }
  throw new UiContractError("invalid_shape");
}

/**
 * The one parser of one identity record: the exact own-key shape of the
 * named kind and the member grammar; every semantics question (placement,
 * ledger chains) belongs to the state owner, never to the document parser.
 */
function parseIdentityRecord(entry) {
  if (!isRecord(entry)) {
    throw new UiContractError("invalid_shape");
  }
  const kind = entry["record"];
  if (typeof kind !== "string" || !Object.hasOwn(IDENTITY_RECORD_KEYS, kind)) {
    throw new UiContractError("unknown_record_kind");
  }
  const allowed = IDENTITY_RECORD_KEYS[kind];
  if (Object.keys(entry).length !== allowed.length + 1) {
    throw new UiContractError("invalid_shape");
  }
  for (const key of allowed) {
    if (!Object.hasOwn(entry, key)) {
      throw new UiContractError("invalid_shape");
    }
  }
  const index = (key) => expectPositiveInteger(entry, key);
  const digestKey = (key) => expectDigest(entry[key]);
  switch (kind) {
    case "plan_revision":
      return Object.freeze({
        record: kind,
        index: index("index"),
        revision: index("revision"),
        sha256: digestKey("sha256"),
        previousSha256: expectDigestOrNull(entry["previous_sha256"]),
        originExecution: index("origin_execution"),
      });
    case "task_revision":
      return Object.freeze({
        record: kind,
        index: index("index"),
        taskId: expectSafeIdString(entry, "task_id"),
        revision: index("revision"),
        sha256: digestKey("sha256"),
        previousSha256: expectDigestOrNull(entry["previous_sha256"]),
        waitIndex: expectIndexOrNull(entry["wait_index"]),
        intentSha256: expectDigestOrNull(entry["intent_sha256"]),
      });
    case "generation":
      return Object.freeze({
        record: kind,
        index: index("index"),
        stageId: expectSafeIdString(entry, "stage_id"),
        stagePosition: index("stage_position"),
        templateId: expectSafeIdString(entry, "template_id"),
        planSha256: digestKey("plan_sha256"),
        initialBudget: index("initial_budget"),
        openedTransitionCount: expectCount(entry["opened_transition_count"]),
        iterationCount: index("iteration_count"),
        openIteration: expectGenerationOpenIteration(entry["open_iteration"]),
        closed: expectGenerationClosure(entry["closed"]),
      });
    case "iteration":
      return Object.freeze({
        record: kind,
        generationIndex: index("generation_index"),
        index: index("index"),
        openedTransitionCount: expectCount(entry["opened_transition_count"]),
        closed: expectIterationClosure(entry["closed"]),
      });
    case "grant":
      return Object.freeze({
        record: kind,
        index: index("index"),
        generationIndex: index("generation_index"),
        waitIndex: index("wait_index"),
        intentSha256: digestKey("intent_sha256"),
        additionalIterations: index("additional_iterations"),
      });
    case "wait":
      return Object.freeze({
        record: kind,
        index: index("index"),
        transitionCount: expectCount(entry["transition_count"]),
        stateId: expectSafeIdString(entry, "state_id"),
        reason: expectTextOrNull(entry["reason"]),
        requestSha256: digestKey("request_sha256"),
        intentSha256: expectDigestOrNull(entry["intent_sha256"]),
        response: expectWaitResponse(entry["response"]),
      });
    case "wait_action":
      return Object.freeze({
        record: kind,
        waitIndex: index("wait_index"),
        id: expectSafeIdString(entry, "id"),
        to: expectSafeIdString(entry, "to"),
      });
    case "execution_agent":
      return Object.freeze({
        record: kind,
        index: index("index"),
        type: expectTextOrNull(entry["type"]),
        stateId: expectSafeIdString(entry, "state_id"),
        executionRole: expectTextOrNull(entry["execution_role"]),
        iterationIndex: expectIndexOrNull(entry["iteration_index"]),
        phase: expectTextOrNull(entry["phase"]),
        stageTask: expectStageTask(entry["stage_task"]),
      });
    case "execution_decision":
      return Object.freeze({
        record: kind,
        index: index("index"),
        type: expectTextOrNull(entry["type"]),
        stateId: expectSafeIdString(entry, "state_id"),
        executionRole: expectTextOrNull(entry["execution_role"]),
        iterationIndex: expectIndexOrNull(entry["iteration_index"]),
        phase: expectTextOrNull(entry["phase"]),
        inputDigest: digestKey("input_digest"),
        result: expectDecisionResult(entry["result"]),
      });
    case "agent_output":
      return Object.freeze({
        record: kind,
        executionIndex: index("execution_index"),
        id: expectSafeIdString(entry, "id"),
        digest: digestKey("digest"),
      });
    case "decision_active_constraint":
    case "decision_violated_relation":
      return Object.freeze({
        record: kind,
        executionIndex: index("execution_index"),
        position: index("position"),
        id: expectSafeIdString(entry, "id"),
      });
    default:
      throw new UiContractError("unknown_record_kind");
  }
}

/**
 * The one parser of the versioned identity document (schema 1, kind
 * `identity`): exact own keys, a positive-safe revision, records in
 * preserved journal order, and the cursor coherence of the pinned
 * revision stream (`next_after` is null exactly at the end of the
 * loaded glowstream; otherwise it equals `after + records.length`).
 * The `run_id` must equal the selected run.
 */
export function parseIdentityDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "identity") {
    throw new UiContractError("unknown_kind");
  }
  expectShape(document, ["schema_version", "kind", "run_id", "revision", "after", "records", "next_after"]);
  const runId = expectString(document, "run_id");
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_shape");
  }
  if (typeof expectedRunId !== "string" || runId !== expectedRunId) {
    throw new UiContractError("unexpected_run");
  }
  const after = expectNonNegativeInteger(document, "after");
  const records = expectArray(document, "records").map(parseIdentityRecord);
  const nextAfter = document["next_after"];
  if (nextAfter !== null) {
    if (typeof nextAfter !== "number" || !Number.isSafeInteger(nextAfter) || nextAfter < 1) {
      throw new UiContractError("invalid_shape");
    }
    if (nextAfter !== after + records.length) {
      throw new UiContractError("invalid_shape");
    }
  }
  return Object.freeze({
    kind: "identity",
    runId,
    revision: expectPositiveInteger(document, "revision"),
    after,
    records: Object.freeze(records),
    nextAfter,
  });
}

/** The exact one-kind execution-binding record (`{record:
 * "execution_binding", execution_index, generation_index,
 * iteration_index}`), in durable journal order. */
function parseExecutionBindingRecord(entry) {
  if (!isRecord(entry)) {
    throw new UiContractError("invalid_shape");
  }
  expectShape(entry, ["record", "execution_index", "generation_index", "iteration_index"]);
  const record = entry["record"];
  if (record !== "execution_binding") {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({
    record: "execution_binding",
    executionIndex: expectPositiveInteger(entry, "execution_index"),
    generationIndex: expectIndexOrNull(entry["generation_index"]),
    iterationIndex: expectIndexOrNull(entry["iteration_index"]),
  });
}

/** The one parser of the versioned execution-bindings document (schema 1,
 * kind `execution_bindings`); the glide cursor coherence of the pinned
 * revision stream mirrors the identity document. */
export function parseExecutionBindingsDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "execution_bindings") {
    throw new UiContractError("unknown_kind");
  }
  expectShape(document, ["schema_version", "kind", "run_id", "revision", "after", "records", "next_after"]);
  const runId = expectString(document, "run_id");
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_shape");
  }
  if (typeof expectedRunId !== "string" || runId !== expectedRunId) {
    throw new UiContractError("unexpected_run");
  }
  const after = expectNonNegativeInteger(document, "after");
  const records = expectArray(document, "records").map(parseExecutionBindingRecord);
  const nextAfter = document["next_after"];
  if (nextAfter !== null) {
    if (typeof nextAfter !== "number" || !Number.isSafeInteger(nextAfter) || nextAfter < 1) {
      throw new UiContractError("invalid_shape");
    }
    if (nextAfter !== after + records.length) {
      throw new UiContractError("invalid_shape");
    }
  }
  return Object.freeze({
    kind: "execution_bindings",
    runId,
    revision: expectPositiveInteger(document, "revision"),
    after,
    records: Object.freeze(records),
    nextAfter,
  });
}

/**
 * The identity endpoint classifier: a well-formed `identity` document of
 * the selected run, or one of the endpoint's documented failure
 * envelopes (`error`, or the failed `command_result` of a rejected
 * identity read). A succeeded command result is unsuitable; every other
 * kind fails closed.
 */
export function parseIdentityEndpointDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "identity") {
    return parseIdentityDocument(document, expectedRunId);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/** The execution-bindings endpoint classifier (mirrors the identity one). */
export function parseExecutionBindingsEndpointDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "execution_bindings") {
    return parseExecutionBindingsDocument(document, expectedRunId);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/** The one URL builder of every identity request; the revision pin
 * travels on the first page and every continuation (never omitted once
 * the pinned stream is bound). */
export function identityRequestPath(runId, after, revision, limit) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  if (typeof after !== "number" || !Number.isSafeInteger(after) || after < 0) {
    throw new UiContractError("invalid_cursor");
  }
  if (revision !== null && (typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1)) {
    throw new UiContractError("invalid_cursor");
  }
  let path = `/v1/runs/${runId}/identity?after=${after}`;
  if (revision !== null) {
    path += `&revision=${revision}`;
  }
  return `${path}&limit=${typeof limit === "number" && Number.isSafeInteger(limit) ? limit : IDENTITY_PAGE_LIMIT}`;
}

/** The one URL builder of every execution-bindings request (the identity
 * grammar, the execution-bindings route). */
export function executionBindingsRequestPath(runId, after, revision, limit) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  if (typeof after !== "number" || !Number.isSafeInteger(after) || after < 0) {
    throw new UiContractError("invalid_cursor");
  }
  if (revision !== null && (typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1)) {
    throw new UiContractError("invalid_cursor");
  }
  let path = `/v1/runs/${runId}/execution-bindings?after=${after}`;
  if (revision !== null) {
    path += `&revision=${revision}`;
  }
  return `${path}&limit=${typeof limit === "number" && Number.isSafeInteger(limit) ? limit : IDENTITY_PAGE_LIMIT}`;
}

/** The identity endpoint client: the expected response kind stays bound
 * to the called endpoint. */
export async function requestIdentityJson(fetchImpl, path, init, expectedRunId) {
  return requestJsonDocument(fetchImpl, path, init, (document) => parseIdentityEndpointDocument(document, expectedRunId));
}

/** The execution-bindings endpoint client. */
export async function requestExecutionBindingsJson(fetchImpl, path, init, expectedRunId) {
  return requestJsonDocument(fetchImpl, path, init, (document) => parseExecutionBindingsEndpointDocument(document, expectedRunId));
}

/* ------------------------------------------------------------------ */
/* Confirmation panel (S4): one current confirmation for the page.      */

/* ------------------------------------------------------------------ */

/**
 * The one page-level current-confirmation owner: at most one waiting
 * action confirmation exists at any instant. The authoritative record
 * is exactly one frozen `{token, request, sourceIdentity}` — `token`
 * the one-shot admission token, `request` the frozen result of exactly
 * one `buildMutationRequest` call (the only displayed and only sent
 * request), and `sourceIdentity` the frozen normalized source identity
 * (never object identity) the owner validates against every applied
 * inspect and authority publish. The rendered DOM panel is deliberately
 * not part of the authoritative state: it lives in the separate
 * non-authoritative `currentConfirmationPanel` element variable.
 */
let currentConfirmation = null;
let currentConfirmationPanel = null;
let confirmationCounter = 0;
/** The last applied authoritative inspect of the selected run; the
 * open-confirmation source identity is built from it at submit time. */
let lastRunModel = null;

/**
 * The normalized revise authority of one run from the current
 * discovery: the exact binding and the exact matching project snapshot
 * with its ordered input-root ids, or null when the authority is
 * unavailable for the run.
 */
function buildReviseAuthoritySnapshot(runId, authority) {
  const projectsModel = isRecord(authority) ? authority["projectsModel"] : null;
  const runEntry = isRecord(authority) ? authority["runEntry"] : null;
  if (
    !isRecord(projectsModel) ||
    projectsModel["kind"] !== "projects" ||
    !isRecord(runEntry) ||
    runEntry["runId"] !== runId
  ) {
    return null;
  }
  const projectId = runEntry["projectId"];
  if (!isSafeId(projectId)) {
    return null;
  }
  const project = projectsModel["projects"].find((entry) => isRecord(entry) && entry["id"] === projectId);
  if (project === undefined) {
    return null;
  }
  if (!Array.isArray(project["inputRootIds"]) || project["inputRootIds"].some((root) => !isSafeId(root))) {
    return null;
  }
  return Object.freeze({
    binding: Object.freeze({
      runId,
      projectId,
      projectAvailable: runEntry["projectAvailable"] === true,
    }),
    project: Object.freeze({
      id: projectId,
      inputRootIds: Object.freeze(project["inputRootIds"].slice()),
    }),
  });
}

/** The pure content comparison of two revise authority snapshots (both
 * built by `buildReviseAuthoritySnapshot` or literal null); no method
 * lives on the frozen data. */
function reviseAuthorityEquals(stored, current) {
  if (stored === null || current === null) {
    return stored === current;
  }
  if (
    stored.binding.runId !== current.binding.runId ||
    stored.binding.projectId !== current.binding.projectId ||
    stored.binding.projectAvailable !== current.binding.projectAvailable
  ) {
    return false;
  }
  if (stored.project.id !== current.project.id) {
    return false;
  }
  const storedRoots = stored.project.inputRootIds;
  const currentRoots = current.project.inputRootIds;
  return (
    storedRoots.length === currentRoots.length &&
    storedRoots.every((root, index) => root === currentRoots[index])
  );
}

/**
 * The source identity of one open confirmation, frozen as normalized
 * values: the run id, the document key of the applied inspect it was
 * built from, the declared action (id, destination, kind), and — for
 * revise — the normalized revise authority. Recorded actions never use
 * object identity, so authority refreshes are compared by content.
 */
function confirmationSourceOf(control, model, authority) {
  const hasModel = isRecord(model) && model["runId"] === control.runId;
  const actionKind = control.kind;
  return Object.freeze({
    runId: control.runId,
    documentKey: hasModel ? liveObservationDocumentKey(model) : null,
    waitIndex: typeof control.waitIndex === "number" ? control.waitIndex : null,
    actionId: actionKind === "respond" ? control.actionId : actionKind,
    actionTo: control.actionTo === undefined ? null : control.actionTo,
    actionKind: actionKind,
    reviseAuthority:
      actionKind === "revise_task" && control.projectId !== undefined
        ? buildReviseAuthoritySnapshot(control.runId, authority)
        : null,
  });
}

/** The describe label of one open confirmation, derived exclusively
 * from the frozen source identity (the holder carries no `describe`
 * field). */
function confirmationDescribe(sourceIdentity) {
  if (sourceIdentity.actionKind === "continue_stage") {
    return "continue-stage";
  }
  if (sourceIdentity.actionKind === "revise_task") {
    return "revise-task";
  }
  return `respond (${sourceIdentity.actionId})`;
}

/** Whether an open confirmation still matches the applied inspection. */
function confirmationMatches(entry, model, authority) {
  const source = entry.sourceIdentity;
  if (!isRecord(model) || model["runId"] !== source.runId) {
    return false;
  }
  if (source.documentKey === null || liveObservationDocumentKey(model) !== source.documentKey) {
    return false;
  }
  if (source.reviseAuthority === null) {
    return true;
  }
  const revised = buildReviseAuthoritySnapshot(source.runId, authority);
  return reviseAuthorityEquals(source.reviseAuthority, revised);
}

/** Destroys the open confirmation: the authoritative record and its
 * rendered panel both die here (the panel is not part of the record). */
function destroyCurrentConfirmation() {
  if (currentConfirmationPanel !== null) {
    currentConfirmationPanel.remove();
    currentConfirmationPanel = null;
  }
  currentConfirmation = null;
}

/**
 * The authority-publish invalidation point: an open revise-task
 * confirmation is destroyed when the normalized revise authority of
 * its run changed (binding gone, projectAvailable flipped, project id
 * changed, matching project gone, ordered input roots changed).
 */
function dropConfirmationIfAuthorityChanged() {
  if (currentConfirmation === null || currentConfirmation.sourceIdentity.reviseAuthority === null) {
    return;
  }
  const runId = currentConfirmation.sourceIdentity.runId;
  const revised = buildReviseAuthoritySnapshot(runId, authorityFor(runId));
  if (!reviseAuthorityEquals(currentConfirmation.sourceIdentity.reviseAuthority, revised)) {
    destroyCurrentConfirmation();
  }
}

/* ------------------------------------------------------------------ */
/* Manual lifecycle controls (v2): inspect document → action model.    */
/* ------------------------------------------------------------------ */

const RESERVED_WAIT_ACTIONS = ["continue_stage", "revise_task"];
const INSPECT_BOUNDARY_KINDS = ["plan_ready", "waiting", "resumable", "in_flight", "succeeded", "failed"];

function unusableBoundary(runId, reason) {
  return Object.freeze({ kind: "unusable_boundary", runId, reason });
}

function isPositiveIntegerText(text) {
  if (typeof text !== "string" || !/^[0-9]+$/.test(text)) {
    return null;
  }
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < 1) {
    return null;
  }
  return value;
}

function expectWaitingBoundaryFields(runId, boundary) {
  const waitIndex = boundary["waitIndex"];
  if (typeof waitIndex !== "number" || !Number.isSafeInteger(waitIndex) || waitIndex < 1) {
    return null;
  }
  if (typeof boundary["reason"] !== "string" || boundary["reason"] === "") {
    return null;
  }
  const actions = boundary["actions"];
  if (!Array.isArray(actions) || actions.length === 0) {
    return null;
  }
  const seen = new Set();
  for (const action of actions) {
    // closed per-action contract (S4): exact own keys, safe ids, unique
    // ids; any structural damage moves the whole boundary fail closed
    if (!isRecord(action) || Object.keys(action).length !== 2 || !Object.hasOwn(action, "id") || !Object.hasOwn(action, "to")) {
      return null;
    }
    if (!isSafeId(action["id"]) || !isSafeId(action["to"])) {
      return null;
    }
    if (seen.has(action["id"])) {
      return null;
    }
    seen.add(action["id"]);
  }
  return waitIndex;
}

/**
 * The one inspect-to-action-model owner. The boundary of the last
 * applied inspect projection is the single source of every control;
 * previous mutating results, local history and guessed state never
 * participate. Unknown statuses, boundaries, continuations or actions
 * fail closed into `unusable_boundary` (no control, zero POST).
 */
export function buildRunActionModel(runModel, authority) {
  if (!isRecord(runModel) || runModel["kind"] !== "run" || !isSafeId(runModel["runId"])) {
    throw new UiContractError("invalid_state");
  }
  const runId = runModel["runId"];
  const status = runModel["status"];
  const boundary = runModel["boundary"];
  if (!isRecord(boundary) || typeof boundary["kind"] !== "string" || !INSPECT_BOUNDARY_KINDS.includes(boundary["kind"])) {
    return unusableBoundary(runId, "unknown_boundary");
  }
  const boundaryKind = boundary["kind"];
  if (status === "running" ? !["plan_ready", "resumable", "in_flight"].includes(boundaryKind) : boundaryKind !== status) {
    return unusableBoundary(runId, "status_boundary_mismatch");
  }
  if (boundaryKind === "resumable") {
    return Object.freeze({ kind: "resume", runId });
  }
  if (boundaryKind === "in_flight") {
    return Object.freeze({ kind: "observe_only", runId });
  }
  if (boundaryKind === "succeeded" || boundaryKind === "failed") {
    return Object.freeze({ kind: "terminal", runId, status });
  }
  if (boundaryKind === "plan_ready") {
    const continuation = boundary["continuation"];
    if (continuation === "resume") {
      return Object.freeze({ kind: "resume", runId });
    }
    if (continuation === "resume_plan") {
      return Object.freeze({ kind: "resume_plan", runId });
    }
    return unusableBoundary(runId, "unknown_continuation");
  }
  const waitIndex = expectWaitingBoundaryFields(runId, boundary);
  if (typeof waitIndex !== "number") {
    return waitIndex === null ? unusableBoundary(runId, "invalid_boundary_shape") : waitIndex;
  }
  const projectsModel = isRecord(authority) ? authority["projectsModel"] : null;
  const runEntry = isRecord(authority) ? authority["runEntry"] : null;
  const controls = boundary["actions"].map((action) => {
    if (action["id"] === "continue_stage") {
      return Object.freeze({ kind: "continue_stage", runId, waitIndex, actionTo: action["to"] });
    }
    if (action["id"] === "revise_task") {
      return Object.freeze(reviseTaskControl(runId, waitIndex, projectsModel, runEntry, action["to"]));
    }
    // any other declared safe action id routes to the generic respond
    // control; the declared `to` travels as contract metadata only
    return Object.freeze({ kind: "respond", runId, waitIndex, actionId: action["id"], actionTo: action["to"] });
  });
  return Object.freeze({ kind: "waiting", runId, controls: Object.freeze(controls) });
}

function reviseTaskControl(runId, waitIndex, projectsModel, runEntry, actionTo) {
  const blocked = (cause) => ({ kind: "revise_task_blocked", runId, waitIndex, actionTo, cause });
  if (!isRecord(projectsModel) || projectsModel["kind"] !== "projects") {
    return blocked("projects_not_loaded");
  }
  if (!isRecord(runEntry) || runEntry["runId"] !== runId) {
    return blocked("project_missing");
  }
  if (runEntry["projectAvailable"] !== true) {
    return blocked("project_unavailable");
  }
  const projectId = runEntry["projectId"];
  const project = projectsModel["projects"].find((entry) => isRecord(entry) && entry["id"] === projectId);
  if (project === undefined) {
    return blocked("project_missing");
  }
  if (!Array.isArray(project["inputRootIds"]) || project["inputRootIds"].length === 0) {
    return blocked("no_input_roots");
  }
  return { kind: "revise_task", runId, waitIndex, projectId, inputRootIds: project["inputRootIds"], actionTo };
}

/**
 * The one mutation request owner: exact route path and exact snake_case
 * wire body with exact own keys. The wait index comes only from the
 * action model (the last applied inspect), never from a form value;
 * validation completes before any fetch.
 */
export function buildMutationRequest(action, formValues) {
  if (!isRecord(action) || !isSafeId(action["runId"])) {
    throw new UiContractError("no_mutation");
  }
  const runId = action["runId"];
  const values = isRecord(formValues) ? formValues : {};
  if (action["kind"] === "resume") {
    return Object.freeze({ endpoint: "resume", path: `/v1/runs/${runId}/resume`, body: Object.freeze({}) });
  }
  if (action["kind"] === "resume_plan") {
    const stageId = values["stageId"];
    if (!isSafeId(stageId)) {
      throw new UiContractError("invalid_stage_id");
    }
    const initialBudget = isPositiveIntegerText(values["initialBudget"]);
    if (initialBudget === null) {
      throw new UiContractError("invalid_initial_budget");
    }
    return Object.freeze({
      endpoint: "resume_plan",
      path: `/v1/runs/${runId}/resume-plan`,
      body: Object.freeze({ stage_id: stageId, initial_budget: initialBudget }),
    });
  }
  if (action["kind"] === "continue_stage") {
    const waitIndex = action["waitIndex"];
    if (typeof waitIndex !== "number" || !Number.isSafeInteger(waitIndex) || waitIndex < 1) {
      throw new UiContractError("no_mutation");
    }
    const additionalIterations = isPositiveIntegerText(values["additionalIterations"]);
    if (additionalIterations === null) {
      throw new UiContractError("invalid_additional_iterations");
    }
    return Object.freeze({
      endpoint: "continue_stage",
      path: `/v1/runs/${runId}/continue-stage`,
      body: Object.freeze({ wait_index: waitIndex, additional_iterations: additionalIterations }),
    });
  }
  if (action["kind"] === "revise_task") {
    const waitIndex = action["waitIndex"];
    if (typeof waitIndex !== "number" || !Number.isSafeInteger(waitIndex) || waitIndex < 1) {
      throw new UiContractError("no_mutation");
    }
    const taskId = values["taskId"];
    const rootId = values["rootId"];
    const ref = values["ref"];
    if (!isSafeId(taskId) || !isSafeId(rootId) || !isSafeId(ref)) {
      throw new UiContractError("invalid_task_reference");
    }
    const inputRootIds = Array.isArray(action["inputRootIds"]) ? action["inputRootIds"] : [];
    if (!inputRootIds.includes(rootId)) {
      throw new UiContractError("invalid_task_reference");
    }
    return Object.freeze({
      endpoint: "revise_task",
      path: `/v1/runs/${runId}/revise-task`,
      body: Object.freeze({
        wait_index: waitIndex,
        task_id: taskId,
        task: Object.freeze({ root_id: rootId, ref }),
      }),
    });
  }
  if (action["kind"] === "respond") {
    // the request-owner guard: the model routing alone cannot make an
    // exported call safe — a forged respond control naming a reserved
    // id (or any unsafe shape) is refused here with zero request
    const waitIndex = action["waitIndex"];
    if (typeof waitIndex !== "number" || !Number.isSafeInteger(waitIndex) || waitIndex < 1) {
      throw new UiContractError("no_mutation");
    }
    const actionId = action["actionId"];
    if (!isSafeId(actionId) || RESERVED_WAIT_ACTIONS.includes(actionId)) {
      throw new UiContractError("no_mutation");
    }
    return Object.freeze({
      endpoint: "respond",
      path: `/v1/runs/${runId}/respond`,
      body: Object.freeze({ wait_index: waitIndex, action_id: actionId }),
    });
  }
  throw new UiContractError("no_mutation");
}

/**
 * Endpoint-specific response acceptance: an inspect accepts only
 * `run|error`, a discovery call only its own discovery kind or `error`,
 * a mutation only `command_result|error`. A known but unsuitable kind is
 * a contract error, never a guessed interpretation.
 */
export function expectRunOrError(result) {
  return acceptEndpointKinds(result, ["run", "error"]);
}

export function expectDiscoveryModel(result, kind) {
  if (kind !== "projects" && kind !== "runs") {
    throw new UiContractError("invalid_endpoint");
  }
  return acceptEndpointKinds(result, [kind, "error"]);
}

export function expectMutationResult(result) {
  return acceptEndpointKinds(result, ["command_result", "error"]);
}

function acceptEndpointKinds(result, kinds) {
  if (!isRecord(result) || typeof result["ok"] !== "boolean") {
    return Object.freeze({ ok: false, failure: Object.freeze({ layer: "contract", reason: "invalid_result" }) });
  }
  if (result["ok"] === false) {
    return Object.freeze({ ok: false, failure: result["failure"] });
  }
  const model = result["model"];
  if (!isRecord(model) || typeof model["kind"] !== "string" || !kinds.includes(model["kind"])) {
    return Object.freeze({ ok: false, failure: Object.freeze({ layer: "contract", reason: "unexpected_kind" }) });
  }
  return Object.freeze({ ok: true, model });
}

/** The one UI-wide mutation gate: at most one mutating POST in flight. */
export function createMutationGate() {
  let inFlight = false;
  return Object.freeze({
    begin() {
      if (inFlight) {
        return false;
      }
      inFlight = true;
      return true;
    },
    end() {
      inFlight = false;
    },
    isInFlight() {
      return inFlight;
    },
  });
}

/**
 * Monotonic GET generation guard: a response from an older generation
 * never overwrites newer state (stale page responses after a Refresh
 * are dropped; safe GET cancellation stays possible).
 */
export function createGenerationGuard() {
  let current = 0;
  return Object.freeze({
    next() {
      current += 1;
      return current;
    },
    isCurrent(generation) {
      return generation === current;
    },
  });
}

/**
 * The one mutation flow owner for every mutating POST (fresh run,
 * resume, resume-plan, continue-stage, revise-task). Exactly one POST
 * is sent; a network failure after send means outcome unknown and is
 * recovered only through exactly one safe GET inspect of the known run
 * id — the POST is never retried. A received command_result is shown,
 * followed by exactly one authoritative inspect of the same run id;
 * no next mutator is started automatically.
 */
export async function runMutationFlow(flow) {
  const handlers = isRecord(flow) && isRecord(flow["handlers"]) ? flow["handlers"] : null;
  const runId = isRecord(flow) ? flow["runId"] : null;
  if (handlers === null || !isSafeId(runId) || typeof flow["send"] !== "function" || typeof flow["inspect"] !== "function") {
    throw new UiContractError("invalid_flow");
  }
  const recover = async () => {
    const inspected = expectRunOrError(await flow["inspect"](runId));
    if (inspected.ok && inspected.model["kind"] === "run") {
      await handlers["onRecovered"](inspected.model);
      return;
    }
    await handlers["onRecoveryUnavailable"](inspected.ok ? inspected.model : inspected.failure);
  };
  const result = await flow["send"]();
  if (result.ok === false) {
    if (result.lost === true) {
      await handlers["onLost"]();
      await recover();
      return;
    }
    await handlers["onError"](result.failure);
    return;
  }
  const mutation = expectMutationResult(result);
  if (mutation.ok === false) {
    await handlers["onError"](mutation.failure);
    return;
  }
  if (mutation.model["kind"] === "error") {
    await handlers["onError"](mutation.model);
    return;
  }
  await handlers["onCommandResult"](mutation.model);
  await recover();
}

/* ------------------------------------------------------------------ */
/* Inspect transport: the one single-flight admission owner of every   */
/* inspect GET of the module.                                          */
/* ------------------------------------------------------------------ */

/**
 * The one inspect transport admission owner. It distinguishes the
 * active wire request, the validity of its result, the latest desired
 * request and nothing else — there is no queue, no pending-request
 * list and no second admission path.
 *
 * Semantics:
 *
 * - `request(desire)`: when the wire slot is free the desire starts
 *   immediately; while a request is on the wire the desire supersedes
 *   the active result (its response can never be applied) and
 *   coalesces into the latest desired request, so several takeover,
 *   refresh and selection events collapse into one last desire.
 * - Supersede never starts a fetch. `invalidate()` supersedes the
 *   active result and withdraws the pending desire (the mutation
 *   boundary); `dispose()` withdraws everything and refuses every
 *   later desire.
 * - After the active request settles — fulfil, reject or abort
 *   rejection, exactly once — the slot is released, the result is
 *   applied only when the request was never superseded (the stale
 *   filter is an application filter, never an admission mechanism),
 *   and at most one coalesced follow-up starts. Aborting a superseded
 *   request is a best-effort wire release: it never frees the slot
 *   and never starts the follow-up; a fetch implementation that
 *   ignores the abort delays the follow-up instead of overlapping it.
 * - An invalid desire (unsafe run id, malformed shape) never reaches
 *   the wire and resolves without effects.
 *
 * The returned promise of every accepted request resolves with the raw
 * API result (or undefined when the transport produced none), whatever
 * the supersede state is — the per-request application callback decides
 * what a superseded result may still mean.
 */
export function createInspectTransport(io) {
  if (
    !isRecord(io) ||
    typeof io["fetchInspect"] !== "function" ||
    typeof io["onInspectStarted"] !== "function" ||
    typeof io["onInspectDropped"] !== "function"
  ) {
    throw new UiContractError("invalid_transport_io");
  }
  const fetchInspect = io["fetchInspect"];
  const onInspectStarted = io["onInspectStarted"];
  const onInspectDropped = io["onInspectDropped"];
  let active = null;
  let desired = null;
  let disposed = false;

  /** The only fetch start site: the wire slot must be free. */
  const startRequest = (request) => {
    if (active !== null) {
      throw new UiContractError("transport_slot_busy");
    }
    active = request;
    const controller = new AbortController();
    request.controller = controller;
    onInspectStarted(request.runId);
    void (async () => {
      let result;
      try {
        result = await fetchInspect(request.runId, controller.signal);
      } catch {
        result = undefined;
      } finally {
        settleActive(request, result);
      }
    })();
  };

  /** The single settlement owner: release, apply-or-drop, one follow-up. */
  const settleActive = (request, result) => {
    try {
      if (request.superseded) {
        if (desired === null) {
          onInspectDropped(request.runId);
        }
      } else {
        request.onApplied(result);
      }
    } finally {
      active = null;
      const followUp = desired;
      desired = null;
      request.resolve(result);
      if (followUp !== null && !disposed) {
        startRequest(followUp);
      }
    }
  };

  return Object.freeze({
    request(desire) {
      const runId = isRecord(desire) && isSafeId(desire["runId"]) ? desire["runId"] : null;
      const onApplied = isRecord(desire) && typeof desire["onApplied"] === "function" ? desire["onApplied"] : null;
      let resolveResult = null;
      const settled = new Promise((resolve) => {
        resolveResult = resolve;
      });
      if (disposed || runId === null || onApplied === null) {
        resolveResult(undefined);
        return settled;
      }
      const request = { runId, onApplied, superseded: false, controller: null, resolve: resolveResult };
      if (active === null) {
        startRequest(request);
      } else {
        active.superseded = true;
        active.controller.abort();
        const replaced = desired;
        desired = request;
        if (replaced !== null) {
          // a coalesced-over desire never runs; it settles without a result
          replaced.resolve(undefined);
        }
      }
      return settled;
    },
    invalidate() {
      if (active !== null) {
        active.superseded = true;
        active.controller.abort();
      }
      const withdrawn = desired;
      desired = null;
      if (withdrawn !== null) {
        withdrawn.resolve(undefined);
      }
    },
    dispose() {
      disposed = true;
      const withdrawn = desired;
      desired = null;
      if (withdrawn !== null) {
        withdrawn.resolve(undefined);
      }
      if (active !== null) {
        active.superseded = true;
        active.controller.abort();
      }
    },
  });
}


/* ------------------------------------------------------------------ */
/* Worker logs (v5): the one bounded read-only viewer of the durable   */
/* schema-v9 worker-log artifacts over the two existing endpoints.     */
/* ------------------------------------------------------------------ */

const WORKER_LOG_STATUSES = ["pending", "unavailable", "captured", "truncated"];
const WORKER_LOG_METADATA_STATUS_KEYS = ["execution_index", "state_id", "status"];
const WORKER_LOG_METADATA_CONTENT_KEYS = [
  "byte_count",
  "execution_index",
  "sha256",
  "state_id",
  "status",
  "truncated_daemon",
  "truncated_local",
];
const WORKER_LOG_STATUS_KEYS = ["execution_index", "revision", "run_id", "schema_version", "kind", "state_id", "status"];
const WORKER_LOG_CHUNK_KEYS = [
  "byte_count",
  "data_base64",
  "execution_index",
  "kind",
  "next_offset",
  "offset",
  "revision",
  "run_id",
  "schema_version",
  "sha256",
  "state_id",
  "status",
  "truncated_daemon",
  "truncated_local",
];

function parseWorkerLogMetadata(entry) {
  if (!isRecord(entry)) {
    throw new UiContractError("invalid_shape");
  }
  const status = expectOneOf(entry, "status", WORKER_LOG_STATUSES);
  const keys = status === "captured" || status === "truncated" ? WORKER_LOG_METADATA_CONTENT_KEYS : WORKER_LOG_METADATA_STATUS_KEYS;
  expectShape(entry, keys);
  const model = Object.freeze({
    executionIndex: expectPositiveInteger(entry, "execution_index"),
    stateId: expectString(entry, "state_id"),
    status,
  });
  if (status !== "captured" && status !== "truncated") {
    return model;
  }
  const sha256 = expectString(entry, "sha256");
  if (!/^[0-9a-f]{64}$/.test(sha256)) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({
    ...model,
    sha256,
    byteCount: expectNonNegativeInteger(entry, "byte_count"),
    truncatedDaemon: expectBoolean(entry, "truncated_daemon"),
    truncatedLocal: expectBoolean(entry, "truncated_local"),
  });
}

/**
 * The worker-logs list document: the metadata union of every agent
 * execution of one run, in execution-index order. The run id must match
 * the expected run; the revision is the authoritative state snapshot
 * revision the list was read at.
 */
export function parseWorkerLogsDocument(document, expectedRunId) {
  expectShape(document, ["schema_version", "kind", "run_id", "revision", "logs"]);
  const runId = expectString(document, "run_id");
  if (runId !== expectedRunId) {
    throw new UiContractError("run_mismatch");
  }
  return Object.freeze({
    kind: "worker_logs",
    runId,
    revision: expectNonNegativeInteger(document, "revision"),
    logs: Object.freeze(expectArray(document, "logs").map(parseWorkerLogMetadata)),
  });
}

/**
 * The worker-log execution document: the status of one execution
 * (`pending`/`unavailable`, no content keys) or one bounded content
 * chunk (`captured`/`truncated`). The chunk fields must be coherent
 * with the raw-byte cursor: the decoded chunk length never exceeds the
 * remaining bytes and `next_offset` is exactly the end of the returned
 * slice whenever a tail exists.
 */
export function parseWorkerLogDocument(document, expectedRunId) {
  const status = expectOneOf(document, "status", WORKER_LOG_STATUSES);
  const keys = status === "captured" || status === "truncated" ? WORKER_LOG_CHUNK_KEYS : WORKER_LOG_STATUS_KEYS;
  expectShape(document, keys);
  const runId = expectString(document, "run_id");
  if (runId !== expectedRunId) {
    throw new UiContractError("run_mismatch");
  }
  const model = Object.freeze({
    kind: "worker_log",
    runId,
    revision: expectNonNegativeInteger(document, "revision"),
    executionIndex: expectPositiveInteger(document, "execution_index"),
    stateId: expectString(document, "state_id"),
    status,
  });
  if (status !== "captured" && status !== "truncated") {
    return model;
  }
  const sha256 = expectString(document, "sha256");
  if (!/^[0-9a-f]{64}$/.test(sha256)) {
    throw new UiContractError("invalid_shape");
  }
  const byteCount = expectNonNegativeInteger(document, "byte_count");
  const offset = expectNonNegativeInteger(document, "offset");
  const nextOffsetValue = document["next_offset"];
  if (nextOffsetValue !== null && !Object.hasOwn(document, "next_offset")) {
    throw new UiContractError("invalid_shape");
  }
  if (
    nextOffsetValue !== null &&
    (typeof nextOffsetValue !== "number" || !Number.isSafeInteger(nextOffsetValue) || nextOffsetValue < 0)
  ) {
    throw new UiContractError("invalid_shape");
  }
  const dataBase64 = expectString(document, "data_base64");
  const decodedLength = base64ByteLength(dataBase64);
  if (decodedLength < 0 || decodedLength > byteCount - offset) {
    throw new UiContractError("invalid_shape");
  }
  if (nextOffsetValue !== null && (decodedLength === 0 || offset + decodedLength !== nextOffsetValue)) {
    throw new UiContractError("invalid_shape");
  }
  if (nextOffsetValue === null && offset !== byteCount && decodedLength !== byteCount - offset) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({
    ...model,
    sha256,
    byteCount,
    truncatedDaemon: expectBoolean(document, "truncated_daemon"),
    truncatedLocal: expectBoolean(document, "truncated_local"),
    offset,
    dataBase64,
    nextOffset: nextOffsetValue,
  });
}

/** The decoded byte length of one base64 string; -1 when malformed. */
function base64ByteLength(text) {
  if (text.length % 4 !== 0) {
    return -1;
  }
  let padding = 0;
  if (text.endsWith("==")) {
    padding = 2;
  } else if (text.endsWith("=")) {
    padding = 1;
  }
  const body = text.slice(0, text.length - padding);
  if (!/^[A-Za-z0-9+/]*$/.test(body)) {
    return -1;
  }
  return Math.floor((body.length * 3) / 4) + padding - padding;
}

/** The logs endpoint parser: only the two logs kinds of one run, the
 * endpoint failure envelopes, and nothing else. */
function parseWorkerLogsEndpointDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "worker_logs") {
    return parseWorkerLogsDocument(document, expectedRunId);
  }
  if (document["kind"] === "worker_log") {
    return parseWorkerLogDocument(document, expectedRunId);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/** The one URL builder of every worker-logs request. */
export function workerLogsRequestPath(runId, executionIndex, offset, limit) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  if (executionIndex === null) {
    // the list route carries no query; the offset/limit arguments stay unused
    return `/v1/runs/${runId}/logs`;
  }
  if (!Number.isSafeInteger(executionIndex) || executionIndex < 1) {
    throw new UiContractError("invalid_execution_index");
  }
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new UiContractError("invalid_offset");
  }
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 262144) {
    throw new UiContractError("invalid_limit");
  }
  return `/v1/runs/${runId}/logs/${executionIndex}?offset=${offset}&limit=${limit}`;
}

/** The worker-logs endpoint client: kind-bound like the events client. */
export async function requestWorkerLogsJson(fetchImpl, path, init, expectedRunId) {
  return requestJsonDocument(fetchImpl, path, init, (document) => parseWorkerLogsEndpointDocument(document, expectedRunId));
}

/**
 * The bounded single-flight logs transport: at most one unsettled logs
 * GET at any instant. A new desire supersedes the active request (its
 * response is dropped, the abort is best effort) and runs as the single
 * follow-up when the slot is released. There is no auto-retry, no
 * retries and no streaming; `dispose` withdraws everything.
 */
export function createLogsTransport(io) {
  if (
    !isRecord(io) ||
    typeof io["fetchLogs"] !== "function" ||
    typeof io["onStarted"] !== "function" ||
    typeof io["onSettled"] !== "function"
  ) {
    throw new UiContractError("invalid_transport_io");
  }
  const fetchLogs = io["fetchLogs"];
  const onStarted = io["onStarted"];
  const onSettled = io["onSettled"];
  let active = null;
  let desired = null;
  let disposed = false;

  const startRequest = (request) => {
    if (active !== null) {
      throw new UiContractError("transport_slot_busy");
    }
    active = request;
    const controller = new AbortController();
    request.controller = controller;
    onStarted(request.runId);
    void (async () => {
      let result;
      try {
        result = await fetchLogs(request.runId, request.path, controller.signal);
      } catch {
        result = undefined;
      } finally {
        settleActive(request, result);
      }
    })();
  };

  const settleActive = (request, result) => {
    try {
      if (request.superseded) {
        if (desired === null) {
          onSettled(request.runId);
        }
      } else {
        request.onApplied(result);
        onSettled(request.runId);
      }
    } finally {
      active = null;
      const followUp = desired;
      desired = null;
      request.resolve(result);
      if (followUp !== null && !disposed) {
        startRequest(followUp);
      }
    }
  };

  return Object.freeze({
    /**
     * Admits one logs desire carrying its priority kind. Arbitration:
     * an in-flight manual content request is never cancelled by a
     * metadata desire — the metadata is kept as at most one coalesced
     * follow-up; a content desire supersedes an active metadata
     * refresh (the user's explicit read wins and the metadata
     * re-derives from the next applied inspect); equal kinds supersede
     * and coalesce to the latest. The wire slot stays single and is
     * freed only by the actual settlement.
     */
    request(desire) {
      const runId = isRecord(desire) && isSafeId(desire["runId"]) ? desire["runId"] : null;
      const path = isRecord(desire) && typeof desire["path"] === "string" ? desire["path"] : null;
      const kind = isRecord(desire) && (desire["kind"] === "metadata" || desire["kind"] === "content") ? desire["kind"] : null;
      const onApplied = isRecord(desire) && typeof desire["onApplied"] === "function" ? desire["onApplied"] : null;
      let resolveResult = null;
      const settled = new Promise((resolve) => {
        resolveResult = resolve;
      });
      if (disposed || runId === null || path === null || kind === null || onApplied === null) {
        resolveResult(undefined);
        return settled;
      }
      const request = { runId, path, kind, onApplied, superseded: false, controller: null, resolve: resolveResult };
      if (active === null) {
        startRequest(request);
      } else if (active.kind === "content" && kind === "metadata") {
        // the manual chunk read is protected; queue the metadata instead
        const replaced = desired;
        desired = request;
        if (replaced !== null) {
          replaced.resolve(undefined);
        }
      } else {
        active.superseded = true;
        active.controller.abort();
        const replaced = desired;
        desired = request;
        if (replaced !== null) {
          replaced.resolve(undefined);
        }
      }
      return settled;
    },
    invalidate() {
      if (active !== null) {
        active.superseded = true;
        active.controller.abort();
      }
      const withdrawn = desired;
      desired = null;
      if (withdrawn !== null) {
        withdrawn.resolve(undefined);
      }
    },
    dispose() {
      disposed = true;
      this.invalidate();
    },
  });
}

/** The empty logs state of one run selection. The closed S2 request
 * descriptor (`requestKind`/`requestExecutionIndex`) starts in the null
 * form: no desire is live yet, so no unsettled logs GET exists. */
export function createWorkerLogsState(runId) {
  return Object.freeze({
    runId,
    snapshotRevision: null,
    logs: [],
    selectedExecutionIndex: null,
    content: null,
    requestState: "idle",
    requestKind: null,
    requestExecutionIndex: null,
    error: null,
  });
}

/**
 * The one incremental UTF-8 decoder owner of the logs viewer: exactly
 * one non-fatal UTF-8 decoder per selected execution. Every non-final
 * chunk is decoded with `{stream: true}`; the terminal chunk is decoded
 * with `{stream: false}`, which flushes any multi-byte sequence split
 * across a chunk boundary. Invalid byte sequences become replacement
 * characters; NUL bytes, ANSI escapes and markup-looking text can never
 * become markup because the rendered value goes through `textContent`.
 */
export function createLogChunkDecoder() {
  let decoder = new TextDecoder("utf-8", { fatal: false });
  return Object.freeze({
    decodeChunk(dataBase64, isFinal) {
      const bytes = Uint8Array.from(atob(dataBase64), (char) => char.charCodeAt(0));
      return decoder.decode(bytes, { stream: !isFinal });
    },
    reset() {
      decoder = new TextDecoder("utf-8", { fatal: false });
    },
  });
}

/**
 * The closed viewer state machine of the Worker logs section. It owns
 * no fetch and no URL: every desire is handed to the single-flight
 * logs transport with its priority kind, and every transition accepts
 * only an already validated logs document of the same run.
 *
 * The viewer is read-only over the already authorized worker-log API
 * and carries no boundary classifier: after ANY successful
 * authoritative inspect of the selected run it serves the metadata
 * list (first inspect: exactly one automatic list GET; later inspects:
 * one automatic list GET per strictly greater inspect revision) and a
 * captured/truncated chunk loads on the explicit `Load log` press of
 * the selected execution on every boundary, including succeeded, failed
 * and resumable runs. The automatic metadata path never loads payload
 * bytes, never cancels an in-flight manual content request (the
 * transport queues it) and never disturbs the inspect, events or
 * mutation surfaces: a metadata failure settles as this section's own
 * typed failure state.
 *
 * The immutable binding of a loaded execution is checked on every
 * metadata application: a changed status, digest or byte count fails
 * closed as `invalid_log_binding` instead of silently mixing bytes from
 * two different artifacts. An unchanged binding preserves the loaded
 * content, the selection and the incremental decoder state.
 *
 * The published descriptor (`requestKind`/`requestExecutionIndex`) is
 * owned by the private desire-liveness registry and never written by
 * hand: a live content desire carries the presentation priority over
 * metadata (a metadata desire requested during a content GET stays
 * queued/live without replacing the content descriptor, so the row
 * keeps the disabled `Loading…` affordance), and every desire terminal
 * re-derives the descriptor from the remaining live desires. The
 * tokens stay private viewer state and are never part of the published
 * document.
 */
export function createWorkerLogViewer(io) {
  if (
    !isRecord(io) ||
    typeof io["transport"] !== "object" ||
    typeof io["onState"] !== "function"
  ) {
    throw new UiContractError("invalid_viewer_io");
  }
  const transport = io["transport"];
  const onState = io["onState"];
  let state = null;
  let decoder = createLogChunkDecoder();
  let lastChunkDesire = null;
  // The last inspect revision an automatic metadata refresh was issued
  // for; every strictly greater applied inspect revision issues exactly
  // one automatic list GET, and a failed application never re-triggers
  // for the same revision.
  let requestedRevision = 0;
  // The S2 desire-liveness registry: the private owner of the published
  // descriptor. Every live desire carries its transport kind and, for a
  // content desire, the execution identity it named. It mirrors the
  // single-flight transport's own supersession: a new content desire
  // supersedes every live desire (an active metadata refresh is aborted
  // and a queued one withdrawn), while a new metadata desire supersedes
  // the live metadata desires only (an in-flight content request stays
  // protected). A callback whose token is no longer live is stale; it
  // changes nothing in the published state. The tokens are an internal
  // ownership marker, never part of the published descriptor; the
  // published descriptor is derived from this registry at every publish.
  let nextDesireToken = 0;
  const liveDesires = new Map();

  /** Establishes one transport desire: prunes the superseded live
   * desires exactly like the transport would and returns this
   * desire's token. */
  const beginDesire = (kind, executionIndex = null) => {
    nextDesireToken += 1;
    if (kind === "content") {
      liveDesires.clear();
    } else {
      for (const [token, live] of liveDesires) {
        if (live.kind === "metadata") {
          liveDesires.delete(token);
        }
      }
    }
    liveDesires.set(nextDesireToken, { kind, executionIndex });
    return nextDesireToken;
  };

  /** One desire terminal: the token leaves the registry. The published
   * descriptor is re-derived by the next publish from the remaining
   * live desires — the descriptor owner changes only by ownership,
   * never by an unconditional write. */
  const endDesire = (token) => {
    liveDesires.delete(token);
  };

  /** The descriptor derivation from the live-desire registry: a live
   * content desire carries the presentation priority over metadata, so
   * a metadata desire requested during a content GET stays queued/live
   * without replacing the content descriptor, and every desire
   * terminal re-derives the descriptor from the remaining live
   * desires. */
  const descriptorForPublish = () => {
    if (state === null) {
      return { requestKind: null, requestExecutionIndex: null };
    }
    let contentDesire = null;
    let metadataDesire = false;
    for (const live of liveDesires.values()) {
      if (live.kind === "content") {
        contentDesire = live;
      } else {
        metadataDesire = true;
      }
    }
    if (contentDesire !== null && contentDesire.executionIndex !== null) {
      return { requestKind: "content", requestExecutionIndex: contentDesire.executionIndex };
    }
    return metadataDesire
      ? { requestKind: "metadata", requestExecutionIndex: null }
      : { requestKind: null, requestExecutionIndex: null };
  };

  /** Whether a live content desire owns the presentation right now. */
  const hasLiveContentDesire = () => {
    for (const live of liveDesires.values()) {
      if (live.kind === "content") {
        return true;
      }
    }
    return false;
  };

  const publish = () => {
    if (state !== null) {
      state = Object.freeze({ ...state, ...descriptorForPublish() });
    }
    onState(state);
  };

  const enter = (runId) => {
    transport.invalidate();
    state = createWorkerLogsState(runId);
    decoder = createLogChunkDecoder();
    lastChunkDesire = null;
    requestedRevision = 0;
    liveDesires.clear();
    publish();
  };

  /** An endpoint failure envelope (error or failed command result) becomes
   * the state failure; any other kind is a contract violation. */
  const endpointFailure = (model) => {
    if (model !== null && typeof model === "object" && model.kind === "error") {
      return { ok: false, failure: { layer: model.layer, reason: model.reason } };
    }
    if (model !== null && typeof model === "object" && model.kind === "command_result") {
      return { ok: false, failure: { layer: "orchestrator", reason: model.reason ?? "failed" } };
    }
    return { ok: false, failure: { layer: "contract", reason: "unexpected_kind" } };
  };

  /** One desire-terminal failure: the token leaves the registry and the
   * descriptor re-derives from the remaining live desires together with
   * the failed state, so no row can stay pending over a settled desire
   * and a remaining live desire keeps its ownership. */
  const applyFailure = (result, token) => {
    liveDesires.delete(token);
    state = Object.freeze({
      ...state,
      requestState: "failed",
      error: Object.freeze({ layer: result.failure.layer, reason: result.failure.reason }),
    });
    publish();
  };

  /**
   * One metadata application: the list of the same run, applied only
   * while its desire is still live. The immutable binding of a loaded
   * execution is verified before the list is accepted; an unchanged
   * binding preserves the loaded content, the selection and the decoder
   * state. Every terminal — success, failure, a transport settlement
   * without a result — closes the desire: the token leaves the registry
   * and the descriptor re-derives from the remaining live desires (a
   * remaining live content desire keeps the content form), so a settled
   * list can never hold a row pending.
   */
  const applyMetadataResult = (result, requestedRunId, token) => {
    if (state === null || state.runId !== requestedRunId || !liveDesires.has(token)) {
      return; // the desire is already dead under a newer transition
    }
    if (result === undefined) {
      // the transport settled without a result; the descriptor still closes
      endDesire(token);
      publish();
      return;
    }
    if (!result.ok) {
      applyFailure(result, token);
      return;
    }
    const model = result.model;
    if (model.kind !== "worker_logs") {
      applyFailure(endpointFailure(model), token);
      return;
    }
    if (model.runId !== state.runId) {
      applyFailure({ ok: false, failure: { layer: "contract", reason: "unexpected_kind" } }, token);
      return;
    }
    if (state.content !== null) {
      const entry = model.logs.find((item) => item.executionIndex === state.content.executionIndex) ?? null;
      const bindingUnchanged = entry !== null
        && entry.status === state.content.status
        && entry.sha256 === state.content.sha256
        && entry.byteCount === state.content.byteCount
        && entry.truncatedDaemon === state.content.truncatedDaemon
        && entry.truncatedLocal === state.content.truncatedLocal;
      if (!bindingUnchanged) {
        applyFailure({ ok: false, failure: { layer: "viewer", reason: "invalid_log_binding" } }, token);
        return;
      }
    }
    requestedRevision = Math.max(requestedRevision, model.revision);
    endDesire(token);
    state = Object.freeze({
      ...state,
      snapshotRevision: model.revision,
      logs: model.logs,
      requestState: "ready",
      error: null,
    });
    publish();
  };

  /**
   * One content application: a validated chunk of the selected
   * execution, decoded through the one incremental decoder, applied
   * only while its desire is still live. A chunk that does not continue
   * the expected raw-byte cursor is stale or duplicated: it is settled
   * as a typed contract failure without touching the decoder, so a
   * retried offset can never duplicate already applied text. Every
   * terminal closes the desire: the token leaves the registry and the
   * descriptor re-derives from the remaining live desires — a remaining
   * live metadata desire takes the metadata form, so a settled chunk
   * can never hold a row pending.
   */
  const applyChunkResult = (result, requestedRunId, executionIndex, expectedOffset, token) => {
    if (state === null || state.runId !== requestedRunId || !liveDesires.has(token)) {
      return; // the desire is already dead under a newer transition
    }
    if (result === undefined) {
      // the transport settled without a result; the descriptor still closes
      endDesire(token);
      publish();
      return;
    }
    if (!result.ok) {
      applyFailure(result, token);
      return;
    }
    const model = result.model;
    if (model.kind !== "worker_log" || model.runId !== state.runId || model.executionIndex !== executionIndex) {
      applyFailure(model.kind === "worker_log" || model.kind === "error" || model.kind === "command_result"
        ? { ok: false, failure: { layer: "contract", reason: "unexpected_kind" } }
        : endpointFailure(model), token);
      return;
    }
    if (model.status !== "captured" && model.status !== "truncated") {
      applyFailure({ ok: false, failure: { layer: "contract", reason: "unexpected_kind" } }, token);
      return;
    }
    if (model.offset !== expectedOffset || (state.content !== null && state.content.executionIndex !== executionIndex)) {
      applyFailure({ ok: false, failure: { layer: "contract", reason: "unexpected_kind" } }, token);
      return;
    }
    const isFinal = model.nextOffset === null;
    const decodedPart = decoder.decodeChunk(model.dataBase64, isFinal);
    lastChunkDesire = null;
    endDesire(token);
    state = Object.freeze({
      ...state,
      snapshotRevision: model.revision,
      content: Object.freeze({
        executionIndex,
        status: model.status,
        sha256: model.sha256,
        byteCount: model.byteCount,
        truncatedDaemon: model.truncatedDaemon,
        truncatedLocal: model.truncatedLocal,
        offset: model.offset,
        nextOffset: model.nextOffset,
        text: (state.content === null ? "" : state.content.text) + decodedPart,
      }),
      requestState: "ready",
      error: null,
    });
    publish();
  };

  return Object.freeze({
    /** Selection change / reset: discards state and invalidates the wire. */
    transitionToLogsView(runId) {
      if (!isSafeId(runId)) {
        throw new UiContractError("invalid_run_id");
      }
      enter(runId);
    },
    dropView() {
      transport.invalidate();
      state = null;
      lastChunkDesire = null;
      liveDesires.clear();
    },
    /**
     * The explicit metadata load or refresh: one bounded GET of the
     * list endpoint. Never automatic on its own. The desire takes the
     * metadata form of the published descriptor (no execution named)
     * only while no live content desire owns it, so no row can render
     * a content pending over a metadata desire.
     */
    fetchLogsList() {
      if (state === null || state.requestState === "loading") {
        return;
      }
      const token = beginDesire("metadata");
      const requestedRunId = state.runId;
      state = Object.freeze({
        ...state,
        requestState: "loading",
        error: null,
      });
      publish();
      transport.request({
        runId: state.runId,
        kind: "metadata",
        path: workerLogsRequestPath(state.runId, null, 0, 0),
        onApplied: (result) => {
          if (!liveDesires.has(token)) {
            return; // a stale or superseded callback changes nothing
          }
          applyMetadataResult(result, requestedRunId, token);
        },
      });
    },
    /**
     * The automatic metadata refresh driven by the applied
     * authoritative inspect path. The first successful inspect of the
     * selected run issues exactly one list GET; every later applied
     * inspect issues one list GET only when its revision is strictly
     * greater than the last requested one. Never loads payload bytes.
     * The metadata desire never takes the published descriptor from a
     * live content desire: an automatic refresh during a content GET
     * stays queued/live without replacing the content descriptor, so it
     * never renders or lifts a row pending.
     */
    inspectApplied(model) {
      if (state === null || !isRecord(model) || model.runId !== state.runId) {
        return;
      }
      const revision = model.revision;
      if (!Number.isSafeInteger(revision) || revision <= requestedRevision) {
        return;
      }
      requestedRevision = revision;
      const token = beginDesire("metadata");
      const requestedRunId = state.runId;
      publish();
      transport.request({
        runId: state.runId,
        kind: "metadata",
        path: workerLogsRequestPath(state.runId, null, 0, 0),
        onApplied: (result) => {
          if (!liveDesires.has(token)) {
            return; // a stale or superseded callback changes nothing
          }
          applyMetadataResult(result, requestedRunId, token);
        },
      });
    },
    /**
     * One execution selection: fresh content and a fresh decoder; an
     * in-flight content desire of the previous selection is closed by
     * the selection transition itself (the §6 seam): the descriptor
     * returns to the null form, the published state settles and the
     * desire's late response is dropped before it can influence the
     * render. An unsettled metadata desire is untouched — its
     * application is selection-independent.
     */
    selectExecution(index) {
      if (state === null || !Number.isSafeInteger(index) || index < 1) {
        return;
      }
      const entry = state.logs.find((item) => item.executionIndex === index) ?? null;
      if (entry === null) {
        return;
      }
      if (state.selectedExecutionIndex === index) {
        return;
      }
      decoder.reset();
      const closesContentDesire = hasLiveContentDesire();
      for (const [token, live] of liveDesires) {
        if (live.kind === "content") {
          liveDesires.delete(token);
        }
      }
      state = Object.freeze({
        ...state,
        selectedExecutionIndex: index,
        content: null,
        error: null,
        requestState: closesContentDesire && state.requestState === "loading" ? "ready" : state.requestState,
      });
      publish(); // the descriptor re-derives: the killed content desire leaves only the remaining live desires
    },
    /**
     * The explicit `Load log` press of the selected execution: one
     * bounded chunk GET continuing the raw-byte cursor. Read-only over
     * the already authorized API: any boundary may read a durable log.
     * The desire takes the content form of the published descriptor,
     * naming exactly this execution for the duration: the row renders
     * the pending affordance in place.
     */
    fetchChunk() {
      if (state === null || state.selectedExecutionIndex === null || state.requestState === "loading") {
        return;
      }
      const entry = state.logs.find((item) => item.executionIndex === state.selectedExecutionIndex) ?? null;
      if (entry === null || (entry.status !== "captured" && entry.status !== "truncated")) {
        return;
      }
      if (state.content !== null && state.content.nextOffset === null) {
        return; // the artifact is fully read
      }
      const executionIndex = state.selectedExecutionIndex;
      const expectedOffset = state.content === null ? 0 : state.content.nextOffset;
      const token = beginDesire("content", executionIndex);
      const requestedRunId = state.runId;
      lastChunkDesire = { executionIndex, offset: expectedOffset };
      state = Object.freeze({
        ...state,
        requestState: "loading",
        error: null,
      });
      publish();
      transport.request({
        runId: state.runId,
        kind: "content",
        path: workerLogsRequestPath(state.runId, executionIndex, expectedOffset, 65536),
        onApplied: (result) => {
          if (!liveDesires.has(token)) {
            return; // a stale or superseded callback changes nothing
          }
          if (state === null || state.selectedExecutionIndex !== executionIndex) {
            return; // a foreign content desire of a previous selection
          }
          applyChunkResult(result, requestedRunId, executionIndex, expectedOffset, token);
        },
      });
    },
    /**
     * One retry click: exactly one re-request of the last failed chunk
     * desire of the still-selected execution. The failed chunk never
     * touched the decoder, so the retry cannot duplicate text. The
     * retry's own content desire re-derives the descriptor with the
     * exact prior execution identity before the re-GET.
     */
    retryLastChunkRequest() {
      if (state === null || state.requestState !== "failed" || lastChunkDesire === null) {
        return;
      }
      if (state.selectedExecutionIndex !== lastChunkDesire.executionIndex) {
        return;
      }
      const retryOffset = lastChunkDesire.offset;
      const retryExecutionIndex = lastChunkDesire.executionIndex;
      const token = beginDesire("content", retryExecutionIndex);
      const requestedRunId = state.runId;
      state = Object.freeze({
        ...state,
        requestState: "loading",
        error: null,
      });
      publish();
      transport.request({
        runId: state.runId,
        kind: "content",
        path: workerLogsRequestPath(state.runId, retryExecutionIndex, retryOffset, 65536),
        onApplied: (result) => {
          if (!liveDesires.has(token)) {
            return; // a stale or superseded callback changes nothing
          }
          if (state === null || state.selectedExecutionIndex !== retryExecutionIndex) {
            return;
          }
          applyChunkResult(result, requestedRunId, retryExecutionIndex, retryOffset, token);
        },
      });
    },
    /** The read-only projection for the DOM adapter. */
    currentState() {
      return state;
    },
  });
}

/* ------------------------------------------------------------------ */
/* Project changes (v6): the read-only panel of the run workspace      */
/* changes relative to the durable project baseline.                   */
/* ------------------------------------------------------------------ */

const PROJECT_CHANGE_KINDS = Object.freeze([
  "added",
  "deleted",
  "content_modified",
  "mode_modified",
  "content_and_mode_modified",
  "symlink_target_modified",
  "type_changed",
]);
const PROJECT_SOURCE_RELATIONS = Object.freeze(["matches_baseline", "matches_result", "diverged"]);
const PROJECT_CHANGE_KIND_SUMMARY_KEYS = Object.freeze([
  "added",
  "deleted",
  "content_modified",
  "mode_modified",
  "content_and_mode_modified",
  "symlink_target_modified",
  "type_changed",
]);
const PROJECT_RELATION_SUMMARY_KEYS = Object.freeze(["matches_baseline", "matches_result", "diverged"]);
const PROJECT_CHANGES_SUMMARY_KEYS = Object.freeze([
  "total",
  ...PROJECT_CHANGE_KIND_SUMMARY_KEYS,
  ...PROJECT_RELATION_SUMMARY_KEYS,
]);
const PROJECT_CHANGES_REQUEST_LIMIT = 100;

const HEX64_PATTERN = /^[0-9a-f]{64}$/;

/** One relative project path: non-empty, never absolute, no NUL and no
 * empty, dot or dot-dot component. The text is verbatim data and is
 * only ever rendered through `textContent`. */
function isRelativeProjectPath(value) {
  if (typeof value !== "string" || value === "" || value.includes("\0") || value.startsWith("/")) {
    return false;
  }
  for (const component of value.split("/")) {
    if (component === "" || component === "." || component === "..") {
      return false;
    }
  }
  return true;
}

function expectChangesNode(value) {
  if (!isRecord(value)) {
    throw new UiContractError("invalid_shape");
  }
  const kind = value["kind"];
  if (kind === "regular_file") {
    const own = Object.keys(value);
    const hasGroup = Object.hasOwn(value, "hardlink_group");
    if (own.length !== (hasGroup ? 6 : 5)) {
      throw new UiContractError("invalid_shape");
    }
    expectShape(value, hasGroup ? ["kind", "mode", "byte_count", "sha256", "multiple_links", "hardlink_group"] : ["kind", "mode", "byte_count", "sha256", "multiple_links"]);
    const mode = value["mode"];
    if (typeof mode !== "number" || !Number.isSafeInteger(mode) || mode < 0 || mode > 0o777) {
      throw new UiContractError("invalid_shape");
    }
    const byteCount = value["byte_count"];
    if (typeof byteCount !== "number" || !Number.isSafeInteger(byteCount) || byteCount < 0) {
      throw new UiContractError("invalid_shape");
    }
    const sha256 = value["sha256"];
    if (typeof sha256 !== "string" || !HEX64_PATTERN.test(sha256)) {
      throw new UiContractError("invalid_shape");
    }
    const multipleLinks = value["multiple_links"];
    if (typeof multipleLinks !== "boolean") {
      throw new UiContractError("invalid_shape");
    }
    if (hasGroup !== multipleLinks) {
      throw new UiContractError("invalid_shape");
    }
    const node = Object.freeze({
      kind: "regular_file",
      mode,
      byte_count: byteCount,
      sha256,
      multiple_links: multipleLinks,
    });
    return hasGroup
      ? Object.freeze({ ...node, hardlink_group: value["hardlink_group"] })
      : node;
  }
  if (kind === "directory") {
    expectShape(value, ["kind", "mode"]);
    const mode = value["mode"];
    if (typeof mode !== "number" || !Number.isSafeInteger(mode) || mode < 0 || mode > 0o777) {
      throw new UiContractError("invalid_shape");
    }
    return Object.freeze({ kind: "directory", mode });
  }
  if (kind === "symlink") {
    expectShape(value, ["kind", "target"]);
    const target = value["target"];
    if (typeof target !== "string") {
      throw new UiContractError("invalid_shape");
    }
    return Object.freeze({ kind: "symlink", target });
  }
  throw new UiContractError("invalid_shape");
}

/** The one variant coherence check of a change entry: the baseline and
 * result projections must agree with the change kind. */
function expectEntryCoherence(change, baseline, result) {
  if (change === "added") {
    if (baseline !== null || result === null) {
      throw new UiContractError("invalid_shape");
    }
    return;
  }
  if (change === "deleted") {
    if (baseline === null || result !== null) {
      throw new UiContractError("invalid_shape");
    }
    return;
  }
  if (baseline === null || result === null) {
    throw new UiContractError("invalid_shape");
  }
  if (change === "content_modified" || change === "content_and_mode_modified") {
    if (baseline.kind !== "regular_file" || result.kind !== "regular_file") {
      throw new UiContractError("invalid_shape");
    }
    return;
  }
  if (change === "mode_modified") {
    if (baseline.kind !== result.kind || (baseline.kind !== "regular_file" && baseline.kind !== "directory")) {
      throw new UiContractError("invalid_shape");
    }
    return;
  }
  if (change === "symlink_target_modified") {
    if (baseline.kind !== "symlink" || result.kind !== "symlink") {
      throw new UiContractError("invalid_shape");
    }
    return;
  }
  // type_changed: the two sides must be different kinds
  if (baseline.kind === result.kind) {
    throw new UiContractError("invalid_shape");
  }
}

/**
 * The dedicated parser of one project-changes document (HTTP schema
 * version 1): exact own keys at every level, the closed change-kind and
 * source-relation vocabularies, the node union with exact hardlink
 * metadata, the per-entry variant coherence, strictly increasing
 * indexes, unique paths and the internally coherent summary counts.
 * Unknown versions, kinds and shapes are never guessed. The general
 * `parseApiDocument` keeps rejecting this kind fail-closed — this
 * parser is bound to the changes endpoint only.
 */
export function parseProjectChangesDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "project_changes") {
    throw new UiContractError("unknown_kind");
  }
  expectShape(document, ["schema_version", "kind", "run_id", "revision", "observation", "summary", "changes", "next_after"]);
  const runId = expectString(document, "run_id");
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_shape");
  }
  if (typeof expectedRunId === "string" && runId !== expectedRunId) {
    throw new UiContractError("unexpected_run");
  }
  const observation = document["observation"];
  if (typeof observation !== "string" || !HEX64_PATTERN.test(observation)) {
    throw new UiContractError("invalid_shape");
  }
  const summaryRecord = expectRecord(document, "summary");
  expectShape(summaryRecord, PROJECT_CHANGES_SUMMARY_KEYS);
  const summary = {};
  for (const key of PROJECT_CHANGES_SUMMARY_KEYS) {
    const value = summaryRecord[key];
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
      throw new UiContractError("invalid_shape");
    }
    summary[key] = value;
  }
  let kindSum = 0;
  for (const key of PROJECT_CHANGE_KIND_SUMMARY_KEYS) {
    kindSum += summary[key];
  }
  if (kindSum !== summary["total"]) {
    throw new UiContractError("invalid_shape");
  }
  let relationSum = 0;
  for (const key of PROJECT_RELATION_SUMMARY_KEYS) {
    relationSum += summary[key];
  }
  if (relationSum !== summary["total"]) {
    throw new UiContractError("invalid_shape");
  }
  const rawChanges = expectArray(document, "changes");
  if (rawChanges.length > summary["total"]) {
    throw new UiContractError("invalid_shape");
  }
  const seenPaths = new Set();
  let previousIndex = 0;
  const changes = rawChanges.map((entry) => {
    if (!isRecord(entry)) {
      throw new UiContractError("invalid_shape");
    }
    expectShape(entry, ["index", "path", "change", "source_relation", "baseline", "result"]);
    const index = entry["index"];
    if (typeof index !== "number" || !Number.isSafeInteger(index) || index < 1 || index <= previousIndex) {
      throw new UiContractError("invalid_shape");
    }
    previousIndex = index;
    const path = entry["path"];
    if (!isRelativeProjectPath(path) || seenPaths.has(path)) {
      throw new UiContractError("invalid_shape");
    }
    seenPaths.add(path);
    const change = expectOneOf(entry, "change", PROJECT_CHANGE_KINDS);
    const sourceRelation = expectOneOf(entry, "source_relation", PROJECT_SOURCE_RELATIONS);
    const baselineValue = entry["baseline"];
    const resultValue = entry["result"];
    const baseline = baselineValue === null ? null : expectChangesNode(baselineValue);
    const result = resultValue === null ? null : expectChangesNode(resultValue);
    expectEntryCoherence(change, baseline, result);
    return Object.freeze({ index, path, change, source_relation: sourceRelation, baseline, result });
  });
  const nextAfter = document["next_after"];
  if (nextAfter !== null && (typeof nextAfter !== "number" || !Number.isSafeInteger(nextAfter) || nextAfter < 0)) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({
    kind: "project_changes",
    runId,
    revision: expectPositiveInteger(document, "revision"),
    observation,
    summary: Object.freeze(summary),
    changes: Object.freeze(changes),
    nextAfter,
  });
}

/** The empty project-changes state of one run selection. */
export function createProjectChangesState(runId) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  return Object.freeze({
    runId,
    inspectRevision: null,
    observation: null,
    summary: null,
    changes: Object.freeze([]),
    nextAfter: null,
    requestState: "idle",
    presentationState: "unavailable",
    error: null,
  });
}

function projectChangesPresentation(summary) {
  if (summary === null) {
    return "unavailable";
  }
  if (summary["total"] === 0) {
    return "no_changes";
  }
  if (summary["diverged"] > 0) {
    return "diverged";
  }
  if (summary["matches_baseline"] === summary["total"]) {
    return "pending";
  }
  if (summary["matches_result"] === summary["total"]) {
    return "already_present";
  }
  return "mixed";
}

function isProjectChangesState(state) {
  return (
    isRecord(state) &&
    isSafeId(state["runId"]) &&
    Array.isArray(state["changes"]) &&
    (state["observation"] === null || (typeof state["observation"] === "string" && HEX64_PATTERN.test(state["observation"])))
  );
}

/**
 * The one pure page-merge owner of the changes viewer. An initial page
 * (requestedAfter 0) replaces the previous snapshot only after the
 * whole page validated; a continuation extends the accepted snapshot
 * append-only under the exact observation, revision and cursor
 * contract. Every violation is a typed UI contract failure and never
 * mutates the input state or applies a partial page.
 */
export function applyProjectChangesPage(state, model, requestedAfter) {
  if (!isProjectChangesState(state)) {
    throw new UiContractError("invalid_state");
  }
  if (model === null || typeof model !== "object" || model.kind !== "project_changes" || model.runId !== state.runId) {
    throw new UiContractError("unexpected_run");
  }
  if (typeof requestedAfter !== "number" || !Number.isSafeInteger(requestedAfter) || requestedAfter < 0) {
    throw new UiContractError("invalid_cursor");
  }
  const merged = requestedAfter === 0 ? model.changes : [...state.changes, ...model.changes];
  if (requestedAfter === 0) {
    if (model.changes.length > 0 && model.changes[0].index !== 1) {
      throw new UiContractError("invalid_shape");
    }
  } else {
    if (requestedAfter !== state.changes.length) {
      throw new UiContractError("invalid_cursor");
    }
    if (state.nextAfter === null) {
      throw new UiContractError("invalid_cursor");
    }
    if (model.observation !== state.observation) {
      throw new UiContractError("observation_mismatch");
    }
    if (model.revision !== state.inspectRevision) {
      throw new UiContractError("invalid_shape");
    }
    let previousLast = state.changes.length === 0 ? 0 : state.changes[state.changes.length - 1].index;
    for (const entry of model.changes) {
      if (entry.index !== previousLast + 1) {
        throw new UiContractError("invalid_shape");
      }
      previousLast = entry.index;
    }
    const knownPaths = new Set(state.changes.map((entry) => entry.path));
    for (const entry of model.changes) {
      if (knownPaths.has(entry.path)) {
        throw new UiContractError("invalid_shape");
      }
    }
    const summary = model.summary;
    const previous = state.summary;
    for (const key of PROJECT_CHANGES_SUMMARY_KEYS) {
      if (summary[key] !== previous[key]) {
        throw new UiContractError("invalid_shape");
      }
    }
  }
  if (merged.length > model.summary["total"]) {
    throw new UiContractError("invalid_shape");
  }
  if (model.nextAfter !== null && model.nextAfter !== merged.length) {
    throw new UiContractError("invalid_shape");
  }
  if (model.nextAfter === null && merged.length !== model.summary["total"]) {
    throw new UiContractError("invalid_shape");
  }
  if (requestedAfter > 0 && model.nextAfter !== null && state.nextAfter !== null && model.nextAfter < state.nextAfter) {
    throw new UiContractError("invalid_cursor");
  }
  return Object.freeze({
    runId: state.runId,
    inspectRevision: model.revision,
    observation: model.observation,
    summary: model.summary,
    changes: Object.freeze(merged),
    nextAfter: model.nextAfter,
    requestState: "ready",
    presentationState: projectChangesPresentation(model.summary),
    error: null,
  });
}

/** The one URL builder of every project-changes request. Only the run
 * id, the page cursor/limit and the observation identity travel in the
 * URL: never a project path, a change path, a symlink target, a file
 * digest, node metadata, a project id or a run root. */
export function projectChangesRequestPath(request) {
  if (!isRecord(request)) {
    throw new UiContractError("invalid_request");
  }
  const runId = request["runId"];
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  const after = request["after"];
  if (typeof after !== "number" || !Number.isSafeInteger(after) || after < 0) {
    throw new UiContractError("invalid_cursor");
  }
  const limit = request["limit"];
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > 200) {
    throw new UiContractError("invalid_request");
  }
  const observation = request["observation"];
  if (after === 0) {
    if (observation !== undefined && observation !== null) {
      throw new UiContractError("invalid_request");
    }
    return `/v1/runs/${runId}/changes?after=0&limit=${limit}`;
  }
  if (typeof observation !== "string" || !HEX64_PATTERN.test(observation)) {
    throw new UiContractError("invalid_request");
  }
  return `/v1/runs/${runId}/changes?after=${after}&limit=${limit}&observation=${observation}`;
}

/**
 * The one document classifier of the changes endpoint: a well-formed
 * `project_changes` document of the selected run, or one of the
 * endpoint's documented failure envelopes (`error`, or the failed
 * `command_result` of a rejected changes read). A succeeded command
 * result is unsuitable and fails closed; every other kind fails closed.
 */
function parseProjectChangesEndpointDocument(document, expectedRunId) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "project_changes") {
    return parseProjectChangesDocument(document, expectedRunId);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/**
 * The changes endpoint client: it reuses the one bounded JSON response
 * primitive and classifies the document by kind, never by message
 * text. The raw Response is never retained and no body, path or target
 * is ever echoed into a diagnostic.
 */
export async function requestProjectChangesJson(fetchImpl, path, init, expectedRunId) {
  return requestJsonDocument(fetchImpl, path, init, (document) => parseProjectChangesEndpointDocument(document, expectedRunId));
}

/**
 * The single-flight transport of the changes viewer: at most one
 * unsettled changes GET exists at any instant. An initial/reload
 * desire supersedes the active request and coalesces as the one
 * follow-up; a continuation desire while a request is on the wire is
 * declined without a second fetch. There is no timer, no polling and
 * no retry loop of any kind.
 */
export function createProjectChangesTransport(io) {
  if (
    !isRecord(io) ||
    typeof io["fetchChanges"] !== "function" ||
    typeof io["onStarted"] !== "function" ||
    typeof io["onSettled"] !== "function"
  ) {
    throw new UiContractError("invalid_transport_io");
  }
  const fetchChanges = io["fetchChanges"];
  const onStarted = io["onStarted"];
  const onSettled = io["onSettled"];
  let active = null;
  let desired = null;
  let disposed = false;

  const startRequest = (request) => {
    if (active !== null) {
      throw new UiContractError("transport_slot_busy");
    }
    active = request;
    const controller = new AbortController();
    request.controller = controller;
    onStarted(request.runId);
    void (async () => {
      let result;
      try {
        result = await fetchChanges(request.runId, request.path, controller.signal);
      } catch {
        result = undefined;
      } finally {
        settleActive(request, result);
      }
    })();
  };

  const settleActive = (request, result) => {
    try {
      if (request.superseded) {
        if (desired === null) {
          onSettled(request.runId);
        }
      } else {
        request.onApplied(result);
        onSettled(request.runId);
      }
    } finally {
      active = null;
      const followUp = desired;
      desired = null;
      request.resolve(result);
      if (followUp !== null && !disposed) {
        startRequest(followUp);
      }
    }
  };

  return Object.freeze({
    request(desire) {
      const runId = isRecord(desire) && isSafeId(desire["runId"]) ? desire["runId"] : null;
      const path = isRecord(desire) && typeof desire["path"] === "string" ? desire["path"] : null;
      const kind = isRecord(desire) && (desire["kind"] === "initial" || desire["kind"] === "continuation") ? desire["kind"] : null;
      const after = isRecord(desire) && typeof desire["after"] === "number" && Number.isSafeInteger(desire["after"]) && desire["after"] >= 0 ? desire["after"] : null;
      const onApplied = isRecord(desire) && typeof desire["onApplied"] === "function" ? desire["onApplied"] : null;
      let resolveResult = null;
      const settled = new Promise((resolve) => {
        resolveResult = resolve;
      });
      if (disposed || runId === null || path === null || kind === null || after === null || onApplied === null) {
        resolveResult(undefined);
        return settled;
      }
      const request = { runId, path, kind, after, onApplied, superseded: false, controller: null, resolve: resolveResult };
      if (active === null) {
        startRequest(request);
      } else if (kind === "continuation") {
        // a continuation never displaces or queues behind another
        // request: the viewer refuses it anyway and no stale page is
        // ever fetched
        resolveResult(undefined);
        return settled;
      } else {
        active.superseded = true;
        active.controller.abort();
        const replaced = desired;
        desired = request;
        if (replaced !== null) {
          replaced.resolve(undefined);
        }
      }
      return settled;
    },
    invalidate() {
      if (active !== null) {
        active.superseded = true;
        active.controller.abort();
      }
      const withdrawn = desired;
      desired = null;
      if (withdrawn !== null) {
        withdrawn.resolve(undefined);
      }
    },
    dispose() {
      disposed = true;
      this.invalidate();
    },
  });
}

/**
 * The closed state machine of the Project changes panel over
 * run workspace changes relative to the durable project baseline:
 * a read-only view. It owns no fetch and no URL: every desire is
 * handed to the single-flight transport. The automatic path is exactly
 * one initial
 * GET after an applied authoritative terminal inspect of the selected
 * run (succeeded or failed) whose revision was not yet requested; every
 * other boundary, an inspect failure and a re-inspect of the same
 * revision issue nothing. An explicit Reload re-requests after=0
 * without an observation and keeps the previous snapshot visible until
 * a fully valid page replaces it; a stale_observation continuation
 * keeps the loaded snapshot and offers only the explicit Reload. The
 * panel never POSTs, never calls inspect, never starts a worker and
 * never changes the operator project.
 */
export function createProjectChangesViewer(io) {
  if (
    !isRecord(io) ||
    typeof io["transport"] !== "object" ||
    typeof io["onState"] !== "function"
  ) {
    throw new UiContractError("invalid_viewer_io");
  }
  const transport = io["transport"];
  const onState = io["onState"];
  let state = null;
  // The last inspect revision an automatic initial GET was issued for;
  // a repeated inspect of the same terminal revision never re-requests.
  let requestedRevision = 0;

  const publish = () => {
    onState(state);
  };

  const failureOf = (result) => {
    if (result !== undefined && result !== null && typeof result === "object" && result.ok === false) {
      return result.failure;
    }
    const model = result !== undefined && result !== null && typeof result === "object" ? result.model : null;
    if (model !== null && typeof model === "object" && model.kind === "error") {
      return Object.freeze({ layer: model.layer, reason: model.reason });
    }
    if (model !== null && typeof model === "object" && model.kind === "command_result") {
      return Object.freeze({ layer: "orchestrator", reason: model.reason ?? "failed" });
    }
    return Object.freeze({ layer: "contract", reason: "unexpected_kind" });
  };

  const applyFailure = (failure) => {
    if (state === null) {
      return;
    }
    const stale = failure !== null && typeof failure === "object" && failure.reason === "stale_observation";
    state = Object.freeze({
      ...state,
      requestState: stale ? "stale" : "error",
      error: failure,
    });
    publish();
  };

  const applyPage = (result, requestedRunId, requestedAfter) => {
    if (state === null || state.runId !== requestedRunId || result === undefined) {
      return;
    }
    if (!result.ok) {
      applyFailure(failureOf(result));
      return;
    }
    const model = result.model;
    if (model.kind !== "project_changes") {
      applyFailure(failureOf(result));
      return;
    }
    try {
      state = applyProjectChangesPage(state, model, requestedAfter);
    } catch (error) {
      state = Object.freeze({
        ...state,
        requestState: "error",
        error: Object.freeze({ layer: "contract", reason: error instanceof UiContractError ? error.reason : "invalid_shape" }),
      });
    }
    publish();
  };

  const requestInitial = () => {
    if (state === null) {
      return;
    }
    const requestedRunId = state.runId;
    const hasSnapshot = state.observation !== null;
    state = Object.freeze({ ...state, requestState: hasSnapshot ? "reloading" : "loading_initial", error: null });
    publish();
    transport.request({
      runId: requestedRunId,
      kind: "initial",
      after: 0,
      path: projectChangesRequestPath({ runId: requestedRunId, after: 0, limit: PROJECT_CHANGES_REQUEST_LIMIT }),
      onApplied: (result) => {
        applyPage(result, requestedRunId, 0);
      },
    });
  };

  return Object.freeze({
    /** Selection change: discards the state and invalidates the wire. */
    transitionToRun(runId) {
      if (!isSafeId(runId)) {
        throw new UiContractError("invalid_run_id");
      }
      transport.invalidate();
      state = createProjectChangesState(runId);
      requestedRevision = 0;
      publish();
    },
    dropView() {
      transport.invalidate();
      state = null;
      requestedRevision = 0;
    },
    /**
     * The automatic path driven by the applied authoritative inspect:
     * exactly one initial GET for a terminal (succeeded or failed)
     * inspect revision that was not yet requested. A nonterminal
     * boundary, an inspect failure and a re-inspect of the same
     * revision issue nothing; a changed terminal revision issues one
     * new initial request that keeps the previous snapshot visible
     * until a fully valid page replaces it.
     */
    inspectApplied(model) {
      if (state === null || !isRecord(model) || model.runId !== state.runId) {
        return;
      }
      if (model.status !== "succeeded" && model.status !== "failed") {
        return;
      }
      const revision = model.revision;
      if (!Number.isSafeInteger(revision) || revision <= requestedRevision) {
        return;
      }
      requestedRevision = revision;
      requestInitial();
    },
    /** The explicit Reload: one after=0 request without an observation;
     * the previous snapshot stays visible with the reloading state. */
    reload() {
      if (state === null || state.requestState === "loading_initial" || state.requestState === "reloading") {
        return;
      }
      requestInitial();
    },
    /**
     * The explicit Load more: one continuation GET carrying the exact
     * current observation. Refused while another request is in flight;
     * unavailable without a continuation cursor.
     */
    loadMore() {
      if (state === null || state.requestState !== "ready" || state.nextAfter === null) {
        return;
      }
      const requestedRunId = state.runId;
      const requestedAfter = state.nextAfter;
      state = Object.freeze({ ...state, requestState: "loading_more", error: null });
      publish();
      transport.request({
        runId: requestedRunId,
        kind: "continuation",
        after: requestedAfter,
        path: projectChangesRequestPath({ runId: requestedRunId, after: requestedAfter, limit: PROJECT_CHANGES_REQUEST_LIMIT, observation: state.observation }),
        onApplied: (result) => {
          applyPage(result, requestedRunId, requestedAfter);
        },
      });
    },
    /** The read-only projection for the DOM adapter. */
    currentState() {
      return state;
    },
  });
}

/* ------------------------------------------------------------------ */
/* Project apply (v8): explicit durable application of the reviewed    */
/* run workspace changes to the registered project.                    */
/* ------------------------------------------------------------------ */

/**
 * The UI mirror of the production operation status vocabulary (the
 * backend owner is `PIPELINE_V2_APPLY_OPERATION_STATUSES`; the parity
 * is pinned by tests). The durable statuses keep their exact names in
 * the UI state — there is no UI-only pseudo-status that could be
 * confused with a durable backend status.
 */
export const PROJECT_APPLY_OPERATION_STATUSES = Object.freeze([
  "preparing",
  "prepared",
  "applying",
  "applied",
  "conflict",
  "recovery_required",
  "failed",
]);

const PROJECT_APPLY_OPERATION_KEYS = [
  "schema_version",
  "kind",
  "run_id",
  "apply_id",
  "project_id",
  "observation",
  "revision",
  "status",
  "completed_step_count",
  "total_step_count",
];

/**
 * The one parser of the `project_apply_operation` document: exact own
 * keys at every level, the production status vocabulary, positive-safe
 * revision and step coherence, and the exact binding to the expected
 * run id, apply id and observation — a response that does not name
 * exactly the requested operation is never applied. No path, content,
 * temp name or host identity is accepted or echoed.
 */
export function parseProjectApplyOperationDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "project_apply_operation") {
    throw new UiContractError("unknown_kind");
  }
  expectShape(document, PROJECT_APPLY_OPERATION_KEYS);
  if (!isRecord(expected) || !isSafeId(expected["runId"]) || !isSafeId(expected["applyId"])) {
    throw new UiContractError("invalid_request");
  }
  const observation = expectString(document, "observation");
  if (typeof expected["observation"] !== "string" || !HEX64_PATTERN.test(expected["observation"])) {
    throw new UiContractError("invalid_request");
  }
  if (!HEX64_PATTERN.test(observation) || observation !== expected["observation"]) {
    throw new UiContractError("observation_mismatch");
  }
  const runId = expectString(document, "run_id");
  if (!isSafeId(runId) || runId !== expected["runId"]) {
    throw new UiContractError("unexpected_run");
  }
  const applyId = expectString(document, "apply_id");
  if (!isSafeId(applyId) || applyId !== expected["applyId"]) {
    throw new UiContractError("apply_id_mismatch");
  }
  const projectId = expectString(document, "project_id");
  if (!isSafeId(projectId)) {
    throw new UiContractError("invalid_shape");
  }
  const revision = expectPositiveInteger(document, "revision");
  const status = expectOneOf(document, "status", PROJECT_APPLY_OPERATION_STATUSES);
  const completedStepCount = expectNonNegativeInteger(document, "completed_step_count");
  const totalStepCount = expectNonNegativeInteger(document, "total_step_count");
  if (completedStepCount > totalStepCount) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({
    kind: "project_apply_operation",
    runId,
    applyId,
    projectId,
    observation,
    revision,
    status,
    completedStepCount,
    totalStepCount,
  });
}

/**
 * The one document classifier of the Project Apply endpoints: a
 * well-formed `project_apply_operation` document bound to the expected
 * operation, or one of the endpoints' documented failure envelopes
 * (`error`, or the failed `command_result` of a rejected apply). A
 * succeeded command result is unsuitable and fails closed; every other
 * kind fails closed.
 */
function parseProjectApplyEndpointDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "project_apply_operation") {
    return parseProjectApplyOperationDocument(document, expected);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/**
 * The Project Apply endpoint client: it reuses the one bounded JSON
 * response primitive and classifies the document by kind, never by
 * message text. The raw Response is never retained and no path,
 * content or diagnostic is ever echoed.
 */
export async function requestProjectApplyJson(fetchImpl, path, init, expected) {
  return requestJsonDocument(fetchImpl, path, init, (document) => parseProjectApplyEndpointDocument(document, expected));
}

/** The exact operation URL: only the run id and the apply id travel in
 * the URL — never a project path, apply store root or observation. */
export function projectApplyRequestPath(runId, applyId) {
  if (!isSafeId(runId) || !isSafeId(applyId)) {
    throw new UiContractError("invalid_request");
  }
  return `/v1/runs/${runId}/project-applies/${applyId}`;
}

/** The exact POST body of the explicit Apply API. */
export function projectApplyPostBody(observation) {
  if (typeof observation !== "string" || !HEX64_PATTERN.test(observation)) {
    throw new UiContractError("invalid_request");
  }
  return Object.freeze({ schema_version: SCHEMA_VERSION, observation });
}

/**
 * The closed eligibility vocabulary of the Review apply action. Every
 * reason is a UI contract fact, never a backend status.
 */
export const PROJECT_APPLY_INELIGIBILITY_REASONS = Object.freeze([
  "run_not_terminal",
  "capability_disabled",
  "changes_not_ready",
  "changes_stale",
  "changes_incomplete",
  "no_changes",
  "diverged_present",
  "no_pending_changes",
  "operation_active",
  "mutation_in_flight",
]);

/**
 * The one pure eligibility owner of the Review apply action. The apply
 * is offered only when every condition holds at once: the selected run
 * is terminal (succeeded or failed — the existing Changes contract),
 * the effective capability is on, the Project Changes snapshot is
 * ready, not stale and fully loaded (loaded === total and no
 * continuation), there is at least one change, none of them diverged,
 * at least one still pending (relation `matches_baseline` — an
 * all-already-present plan needs no apply), no operation is active for
 * the run and no other UI mutation POST is in flight. A partial page
 * out of a longer history never enables the apply.
 */
export function projectApplyEligibility(options) {
  if (!isRecord(options)) {
    throw new UiContractError("invalid_request");
  }
  const terminal = options["terminalStatus"];
  if (terminal !== "succeeded" && terminal !== "failed") {
    return ineligible("run_not_terminal");
  }
  if (options["capability"] !== true) {
    return ineligible("capability_disabled");
  }
  const changes = options["changesState"];
  if (isRecord(changes) && changes["requestState"] === "stale") {
    return ineligible("changes_stale");
  }
  if (
    !isRecord(changes) ||
    changes["requestState"] !== "ready" ||
    changes["summary"] === null ||
    !isRecord(changes["summary"])
  ) {
    return ineligible("changes_not_ready");
  }
  if (changes["nextAfter"] !== null || changes["changes"].length !== changes["summary"]["total"]) {
    return ineligible("changes_incomplete");
  }
  const summary = changes["summary"];
  if (summary["total"] === 0) {
    return ineligible("no_changes");
  }
  if (summary["diverged"] > 0) {
    return ineligible("diverged_present");
  }
  if (summary["matches_baseline"] === 0) {
    return ineligible("no_pending_changes");
  }
  const phase = options["applyState"];
  if (typeof phase !== "string" || phase !== "idle") {
    return ineligible("operation_active");
  }
  if (options["mutationInFlight"] === true) {
    return ineligible("mutation_in_flight");
  }
  return Object.freeze({ eligible: true, reason: null });

  function ineligible(reason) {
    if (!(PROJECT_APPLY_INELIGIBILITY_REASONS.indexOf(reason) >= 0)) {
      throw new UiContractError("invalid_request");
    }
    return Object.freeze({ eligible: false, reason });
  }
}

/* --- The browser recovery record: the one closed storage owner. ----- */

/** The single closed localStorage namespace of the recovery records. */
export const APPLY_RECORD_STORAGE_PREFIX = "orchestrator.apply.v1:";

const APPLY_RECORD_KEYS = ["schema_version", "run_id", "apply_id", "observation"];

/**
 * The parser of one stored recovery record: exact own keys, version 1,
 * safe run and apply ids and a 64-hex observation binding. Nothing else is
 * ever read from storage and no path, content or diagnostic is stored.
 */
export function parseApplyRecoveryRecord(document) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  expectShape(document, APPLY_RECORD_KEYS);
  const runId = expectString(document, "run_id");
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_shape");
  }
  const applyId = expectString(document, "apply_id");
  if (!isSafeId(applyId) || !HEX64_PATTERN.test(applyId)) {
    throw new UiContractError("invalid_shape");
  }
  const observation = expectString(document, "observation");
  if (!HEX64_PATTERN.test(observation)) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({ runId, applyId, observation });
}

/**
 * The one closed browser storage owner of the Apply recovery records:
 * one record per run id under one fixed namespace prefix. The record
 * is a browser recovery hint only — the authoritative operation status
 * is always the GET API. The store never deletes a corrupted record
 * silently (the error is surfaced), never stores anything beyond the
 * four safe metadata fields, and refuses to write a new record while
 * one already exists for the run (a retry never writes; only a
 * confirmed applied operation plus a confirming reload removes it).
 *
 * `storage` is the localStorage-like object (`getItem`/`setItem`/
 * `removeItem`) or `null` when browser storage is unavailable — an
 * unavailable store refuses every write so an apply never starts
 * without its durable recovery hint.
 */
export function createApplyRecoveryStore(storage) {
  if (storage !== null && (!isRecord(storage) || typeof storage["getItem"] !== "function" || typeof storage["setItem"] !== "function" || typeof storage["removeItem"] !== "function")) {
    throw new UiContractError("invalid_storage");
  }
  const keyFor = (runId) => {
    if (!isSafeId(runId)) {
      throw new UiContractError("invalid_run_id");
    }
    return `${APPLY_RECORD_STORAGE_PREFIX}${runId}`;
  };
  const readRaw = (key) => {
    try {
      return { ok: true, value: storage.getItem(key) };
    } catch {
      return { ok: false, value: undefined };
    }
  };
  return Object.freeze({
    /** The record of one run or `null`; `error` is `corrupt_record` or
     * `storage_unavailable` — a corrupted record is never deleted. */
    load(runId) {
      const key = keyFor(runId);
      if (storage === null) {
        return Object.freeze({ record: null, error: "storage_unavailable" });
      }
      const read = readRaw(key);
      if (!read.ok) {
        return Object.freeze({ record: null, error: "storage_unavailable" });
      }
      const raw = read.value;
      if (raw === undefined || raw === null || raw === "") {
        return Object.freeze({ record: null, error: null });
      }
      if (typeof raw !== "string") {
        return Object.freeze({ record: null, error: "corrupt_record" });
      }
      let document;
      try {
        document = JSON.parse(raw);
      } catch {
        return Object.freeze({ record: null, error: "corrupt_record" });
      }
      try {
        return Object.freeze({ record: parseApplyRecoveryRecord(document), error: null });
      } catch {
        return Object.freeze({ record: null, error: "corrupt_record" });
      }
    },
    /**
     * Writes the recovery hint for one run and reads it back for a
     * byte- and shape-equivalent verification; every failure refuses
     * the write so the caller never POSTs without a durable hint. A
     * record that already exists for the run is never overwritten.
     */
    save(runId, observation) {
      const key = keyFor(runId);
      if (typeof observation !== "string" || !HEX64_PATTERN.test(observation)) {
        throw new UiContractError("invalid_request");
      }
      if (storage === null) {
        return Object.freeze({ saved: false, error: "storage_unavailable" });
      }
      const existing = readRaw(key);
      if (existing.ok && existing.value !== null && existing.value !== undefined && existing.value !== "") {
        return Object.freeze({ saved: false, error: "record_exists" });
      }
      const wire = JSON.stringify({ schema_version: SCHEMA_VERSION, run_id: runId, apply_id: observation, observation });
      try {
        storage.setItem(key, wire);
      } catch {
        return Object.freeze({ saved: false, error: "record_unwritable" });
      }
      const readBack = readRaw(key);
      if (!readBack.ok || readBack.value !== wire) {
        return Object.freeze({ saved: false, error: "record_unwritable" });
      }
      try {
        const parsed = parseApplyRecoveryRecord(JSON.parse(String(readBack.value)));
        if (parsed.runId !== runId || parsed.applyId !== observation || parsed.observation !== observation) {
          return Object.freeze({ saved: false, error: "record_unwritable" });
        }
      } catch {
        return Object.freeze({ saved: false, error: "record_unwritable" });
      }
      return Object.freeze({ saved: true, error: null });
    },
    /** Removes the record of one run; a storage failure keeps it. */
    remove(runId) {
      const key = keyFor(runId);
      if (storage === null) {
        return false;
      }
      try {
        storage.removeItem(key);
        return true;
      } catch {
        return false;
      }
    },
  });
}

/* --- The single-flight transport of the operation GETs. ------------- */

/**
 * The one single-flight transport of the read-only operation GETs: at
 * most one unsettled GET exists at any instant. A new desire
 * supersedes the active request and coalesces as the one follow-up;
 * `invalidate()` supersedes the active result and withdraws the
 * pending desire; `dispose()` withdraws everything. The transport
 * never starts a POST and owns no timer, no polling and no retry
 * loop of any kind.
 */
export function createProjectApplyTransport(io) {
  if (!isRecord(io) || typeof io["fetchOperation"] !== "function") {
    throw new UiContractError("invalid_transport_io");
  }
  const fetchOperation = io["fetchOperation"];
  let active = null;
  let desired = null;
  let disposed = false;
  const settleActive = () => {
    const finished = active;
    active = null;
    return finished;
  };
  const startNext = () => {
    if (disposed || desired === null || active !== null) {
      return;
    }
    const next = desired;
    desired = null;
    active = next;
    fetchOperation(next.runId, next.applyId, next.observation, next.signal)
      .then(
        (result) => next.onSettled({ fulfilled: true, superseded: next.superseded, result }),
        (error) => next.onSettled({ fulfilled: false, superseded: next.superseded, error }),
      )
      .then(() => {
        const finished = settleActive();
        if (finished !== next) {
          return;
        }
        startNext();
      });
  };
  return Object.freeze({
    request(desire) {
      if (
        disposed ||
        !isRecord(desire) ||
        !isSafeId(desire["runId"]) ||
        !isSafeId(desire["applyId"]) ||
        typeof desire["observation"] !== "string" ||
        !HEX64_PATTERN.test(desire["observation"])
      ) {
        return;
      }
      if (active !== null) {
        active.superseded = true;
        if (typeof active.abort === "function") {
          try {
            active.abort();
          } catch {
            // best effort: a fetch that ignores the abort delays the follow-up
          }
        }
      }
      desired = {
        runId: desire["runId"],
        applyId: desire["applyId"],
        observation: typeof desire["observation"] === "string" ? desire["observation"] : "",
        signal: undefined,
        onSettled: typeof desire["onSettled"] === "function" ? desire["onSettled"] : () => undefined,
        superseded: false,
      };
      startNext();
    },
    invalidate() {
      if (active !== null) {
        active.superseded = true;
        if (typeof active.abort === "function") {
          try {
            active.abort();
          } catch {
            // best effort
          }
        }
      }
      desired = null;
    },
    dispose() {
      disposed = true;
      this.invalidate();
    },
  });
}

/* --- The apply state machine. --------------------------------------- */

/**
 * The closed UI state union of the apply panel. The durable operation
 * statuses keep their exact production names (`preparing`, `prepared`,
 * `applying`, `applied`, `conflict`, `recovery_required`, `failed`);
 * the UI-own states are `idle`, `confirming`, `submitting`,
 * `network_unknown` and `unavailable` — no pseudo-status that could be
 * confused with a durable backend status.
 */
export const PROJECT_APPLY_UI_STATES = Object.freeze([
  "idle",
  "confirming",
  "submitting",
  "network_unknown",
  "preparing",
  "prepared",
  "applying",
  "applied",
  "conflict",
  "recovery_required",
  "failed",
  "unavailable",
]);

function createApplyState(runId) {
  return Object.freeze({
    runId,
    uiState: "idle",
    applyId: null,
    observation: null,
    revision: null,
    completedSteps: null,
    totalSteps: null,
    error: null,
    recordError: null,
    recoveryIssued: false,
    submitKind: null,
  });
}

/**
 * The one state machine of the explicit Project Apply panel. It owns
 * no fetch and no gate: every POST desire is handed to the injected
 * `submit` capability (the wiring acquires the one shared UI mutation
 * gate before the wire) and every operation GET to the single-flight
 * transport. The recovery record is written synchronously before the
 * first POST and removed only after a confirmed applied operation plus
 * a fully loaded confirming reload.
 *
 * Recovery on load: after the terminal Project Changes snapshot of the
 * selected run is fully loaded, an existing recovery record triggers
 * exactly one operation GET per load — never a POST, never a timer.
 * A 404 shows the lost/missing durable operation and never erases the
 * record automatically; a lost POST shows `network_unknown` and offers
 * only the explicit same-identity retry and check.
 */
export function createProjectApplyViewer(io) {
  if (
    !isRecord(io) ||
    !isRecord(io["recoveryStore"]) ||
    typeof io["submit"] !== "function" ||
    typeof io["onState"] !== "function" ||
    typeof io["transport"]?.request !== "function"
  ) {
    throw new UiContractError("invalid_viewer_io");
  }
  const recoveryStore = io["recoveryStore"];
  const submit = io["submit"];
  const onState = io["onState"];
  const transport = io["transport"];
  let state = null;

  const publish = () => {
    onState(state);
  };

  const expectIdentity = (identity) => {
    if (
      !isRecord(identity) ||
      !isSafeId(identity["runId"]) ||
      typeof identity["observation"] !== "string" ||
      !HEX64_PATTERN.test(identity["observation"])
    ) {
      throw new UiContractError("invalid_request");
    }
    return Object.freeze({ runId: identity["runId"], observation: identity["observation"] });
  };

  const startSubmit = async (kind, identity) => {
    if (state === null) {
      return;
    }
    state = Object.freeze({
      ...state,
      uiState: "submitting",
      applyId: identity.applyId,
      observation: identity.observation,
      error: null,
      submitKind: kind,
    });
    publish();
    const result = await submit(identity);
    applySubmitResult(identity, result, kind);
  };

  const applySubmitResult = (identity, result, kind) => {
    if (state === null || state.runId !== identity.runId || state.observation !== identity.observation) {
      // A selection change reset the panel while the POST was in
      // flight: the response never applies to another run. The record
      // stays on disk; a later selection recovers through the GET.
      return;
    }
    if (result === undefined || result === null || typeof result !== "object") {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "unexpected_result" } });
      publish();
      return;
    }
    if (result.ok === false) {
      if (result.lost === true) {
        state = Object.freeze({ ...state, uiState: "network_unknown", error: null });
        publish();
        return;
      }
      const failure = result.failure ?? { layer: "contract", reason: "unexpected_result" };
      applyOperationFailure(identity, failure, kind);
      return;
    }
    const model = result.model;
    if (model === null || typeof model !== "object") {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "unexpected_result" } });
      publish();
      return;
    }
    if (model.kind === "error" || model.kind === "command_result") {
      const failure = model.kind === "error"
        ? { layer: model.layer, reason: model.reason }
        : { layer: "orchestrator", reason: model.reason ?? "failed" };
      applyOperationFailure(identity, failure, kind);
      return;
    }
    if (model.kind !== "project_apply_operation") {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "unexpected_kind" } });
      publish();
      return;
    }
    applyOperationModel(identity, model, kind);
  };

  const applyOperationFailure = (identity, failure, kind) => {
    // `stale_observation` on a fresh or continued submit proves the
    // reviewed plan no longer matches the world; the backend refused
    // before any durable effect, so the recovery hint of an operation
    // that was never created is cleared with an explicit note — the
    // operator re-reviews the fresh observation instead of wedging on
    // an unappliable identity. Every other failure keeps the record.
    if (failure !== null && typeof failure === "object" && failure.reason === "stale_observation") {
      recoveryStore.remove(identity.runId);
      state = Object.freeze({
        ...state,
        uiState: "idle",
        applyId: null,
        observation: null,
        revision: null,
        completedSteps: null,
        totalSteps: null,
        error: failure,
        submitKind: null,
        recordError: null,
      });
      publish();
      return;
    }
    state = Object.freeze({ ...state, uiState: "unavailable", error: failure, submitKind: null });
    publish();
  };

  const applyOperationModel = (identity, model, kind) => {
    if (model.runId !== identity.runId || model.applyId !== identity.applyId || model.observation !== identity.observation) {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "operation_mismatch" } });
      publish();
      return;
    }
    const applied = model.status === "applied";
    state = Object.freeze({
      ...state,
      uiState: model.status,
      applyId: model.applyId,
      observation: model.observation,
      revision: model.revision,
      completedSteps: model.completedStepCount,
      totalSteps: model.totalStepCount,
      error: null,
      submitKind: null,
    });
    publish();
    if (applied && typeof io["onApplied"] === "function") {
      // The one post-apply hook: the wiring runs the explicit Project
      // Changes reload (after=0, no observation); the record removal
      // waits for the confirming reload through `changesSnapshotReady`.
      io["onApplied"](identity);
    }
  };

  const applyOperationGet = (identity, result) => {
    if (state === null || state.runId !== identity.runId) {
      return;
    }
    if (result === undefined || result === null || typeof result !== "object") {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "unexpected_result" } });
      publish();
      return;
    }
    if (result.ok === false) {
      const failure = result.failure ?? { layer: "contract", reason: "unexpected_result" };
      state = Object.freeze({ ...state, uiState: "unavailable", error: failure });
      publish();
      return;
    }
    const model = result.model;
    if (model === null || typeof model !== "object") {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "unexpected_result" } });
      publish();
      return;
    }
    if (model.kind === "error" || model.kind === "command_result") {
      const failure = model.kind === "error"
        ? { layer: model.layer, reason: model.reason }
        : { layer: "orchestrator", reason: model.reason ?? "failed" };
      state = Object.freeze({ ...state, uiState: "unavailable", error: failure });
      publish();
      return;
    }
    if (model.kind !== "project_apply_operation" || model.runId !== identity.runId || model.applyId !== identity.applyId) {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "operation_mismatch" } });
      publish();
      return;
    }
    const observationBinding = state.observation === null || state.observation === model.observation;
    if (!observationBinding) {
      state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "observation_mismatch" } });
      publish();
      return;
    }
    state = Object.freeze({
      ...state,
      uiState: model.status,
      applyId: model.applyId,
      observation: model.observation,
      revision: model.revision,
      completedSteps: model.completedStepCount,
      totalSteps: model.totalStepCount,
      error: null,
    });
    publish();
    if (model.status === "applied" && typeof io["onApplied"] === "function") {
      io["onApplied"](identity);
    }
  };

  const issueRecoveryGet = () => {
    if (state === null || state.recoveryIssued) {
      return;
    }
    const loaded = recoveryStore.load(state.runId);
    if (loaded.error === "storage_unavailable" || loaded.error === "corrupt_record") {
      state = Object.freeze({ ...state, recordError: loaded.error, recoveryIssued: true });
      publish();
      return;
    }
    if (loaded.record === null) {
      state = Object.freeze({ ...state, recoveryIssued: true });
      publish();
      return;
    }
    state = Object.freeze({
      ...state,
      recoveryIssued: true,
      applyId: loaded.record.applyId,
      observation: loaded.record.observation,
    });
    const requestedRunId = state.runId;
    const requestedApplyId = state.applyId;
    publish();
    transport.request({
      runId: requestedRunId,
      applyId: requestedApplyId,
      observation: state.observation,
      onSettled: (settlement) => {
        if (state === null || state.runId !== requestedRunId || settlement.superseded === true) {
          return;
        }
        const raw = settlement.fulfilled ? settlement.result : {
          ok: false,
          lost: false,
          failure: { layer: "transport", reason: "network_error" },
        };
        applyOperationGet({ runId: requestedRunId, applyId: requestedApplyId, observation: state.observation }, raw);
      },
    });
  };

  return Object.freeze({
    /** Selection change: resets the panel, invalidates the wire and
     * reloads the record presence — the record itself is never
     * removed by a selection change. */
    transitionToRun(runId) {
      if (!isSafeId(runId)) {
        throw new UiContractError("invalid_run_id");
      }
      transport.invalidate();
      state = createApplyState(runId);
      const loaded = recoveryStore.load(runId);
      if (loaded.error !== null) {
        state = Object.freeze({ ...state, recordError: loaded.error });
      }
      publish();
    },
    dropView() {
      transport.invalidate();
      state = null;
    },
    /**
     * The hook fired by the wiring when a fully loaded, ready Project
     * Changes snapshot of the selected run lands. It drives exactly
     * one recovery GET per load (when a record exists) and the record
     * removal after a confirmed applied operation plus a fully loaded
     * reload without pending or diverged entries.
     */
    changesSnapshotReady(changesState) {
      if (state === null || !isRecord(changesState) || changesState["runId"] !== state.runId) {
        return;
      }
      if (state.uiState === "applied" && state.applyId !== null) {
        const summary = changesState["summary"];
        if (
          isRecord(summary) &&
          changesState["nextAfter"] === null &&
          changesState["changes"].length === summary["total"] &&
          summary["diverged"] === 0 &&
          summary["matches_baseline"] === 0
        ) {
          const removed = recoveryStore.remove(state.runId);
          state = Object.freeze({
            ...state,
            uiState: removed ? "idle" : "applied",
            applyId: removed ? null : state.applyId,
            observation: removed ? null : state.observation,
            revision: removed ? null : state.revision,
            completedSteps: removed ? null : state.completedSteps,
            totalSteps: removed ? null : state.totalSteps,
            error: null,
            recordError: removed ? null : "record_unwritable",
            recoveryIssued: true,
          });
          publish();
        }
        return;
      }
      issueRecoveryGet();
    },
    /** The first Review apply press: confirmation only, zero POST. A
     * run with an existing recovery record never reaches confirming —
     * its operation identity is already pinned. */
    review() {
      if (state === null || state.uiState !== "idle") {
        return;
      }
      if (recoveryStore.load(state.runId).record !== null) {
        return;
      }
      state = Object.freeze({ ...state, uiState: "confirming" });
      publish();
    },
    /** Cancel: zero effects, back to idle. */
    cancel() {
      if (state === null || state.uiState !== "confirming") {
        return;
      }
      state = Object.freeze({ ...state, uiState: "idle" });
      publish();
    },
    /**
     * The confirm press of a fresh apply: the recovery record is
     * written synchronously first; a failed write never POSTs.
     */
    confirm(observation) {
      if (state === null || state.uiState !== "confirming") {
        return;
      }
      const identity = expectIdentity({ runId: state.runId, observation });
      if (state.applyId !== null && state.applyId !== identity.observation) {
        state = Object.freeze({ ...state, uiState: "unavailable", error: { layer: "contract", reason: "record_conflict" } });
        publish();
        return;
      }
      const saved = recoveryStore.save(state.runId, identity.observation);
      if (!saved.saved) {
        state = Object.freeze({
          ...state,
          uiState: "unavailable",
          error: null,
          recordError: saved.error,
        });
        publish();
        return;
      }
      void startSubmit("fresh", { runId: identity.runId, applyId: identity.observation, observation: identity.observation });
    },
    /** The explicit retry of the same operation after a lost POST. */
    retry() {
      if (state === null || (state.uiState !== "network_unknown" && state.uiState !== "unavailable") || state.applyId === null) {
        return;
      }
      void startSubmit("retry", { runId: state.runId, applyId: state.applyId, observation: state.observation });
    },
    /** The explicit continuation of a durable in-progress operation. */
    continueRecovery() {
      if (
        state === null ||
        (state.uiState !== "prepared" && state.uiState !== "applying" && state.uiState !== "recovery_required" && state.uiState !== "unavailable") ||
        state.applyId === null
      ) {
        return;
      }
      void startSubmit("continue", { runId: state.runId, applyId: state.applyId, observation: state.observation });
    },
    /** The explicit status check of the known operation identity. */
    checkStatus() {
      if (state === null || state.applyId === null || state.uiState === "submitting") {
        return;
      }
      const requestedRunId = state.runId;
      const requestedApplyId = state.applyId;
      transport.request({
        runId: requestedRunId,
        applyId: requestedApplyId,
        observation: state.observation,
        onSettled: (settlement) => {
          if (state === null || state.runId !== requestedRunId || settlement.superseded === true) {
            return;
          }
          const raw = settlement.fulfilled ? settlement.result : {
            ok: false,
            lost: false,
            failure: { layer: "transport", reason: "network_error" },
          };
          applyOperationGet({ runId: requestedRunId, applyId: requestedApplyId, observation: state.observation }, raw);
        },
      });
    },
    /** The read-only projection for the DOM adapter. */
    currentState() {
      return state;
    },
  });
}

/* ------------------------------------------------------------------ */
/* Project change content (v7): bounded read-only content preview and  */
/* client-side bounded diff over the Project Change Content API.       */
/* ------------------------------------------------------------------ */

/** The raw-byte request chunk of one content GET (the API default). */
export const CONTENT_PREVIEW_CHUNK_LIMIT = 65536;
/** The maximum raw bytes the viewer retains per side (the bounded
 * preview cap; beyond it the side stays a truncated preview). */
export const CONTENT_PREVIEW_MAX_BYTES = 262144;
/** The maximum bytes of one side rendered through the bounded hex
 * view (the DOM stays small; the retained bytes may be larger). */
export const CONTENT_HEX_DISPLAY_BYTES = 4096;
/** The bounded line-diff limits: lines per side and LCS work cells. */
export const DIFF_MAX_LINES_PER_SIDE = 2000;
export const DIFF_MAX_WORK_CELLS = 250000;

const CONTENT_STATUS_KEYS = ["available", "implicit_empty", "metadata_only", "unavailable"];
const CONTENT_SIDE_KEYS = ["baseline", "result"];
/** The closed unavailable-reason vocabulary of the production content
 * API; the UI parser owns this pinned copy (one element). */
const CONTENT_UNAVAILABLE_REASONS = ["source_no_longer_matches_baseline"];
const CONTENT_COMMON_KEYS = ["schema_version", "kind", "run_id", "revision", "observation", "change_index", "side", "status"];
const CONTENT_STATUS_KEYS_MAP = {
  available: [...CONTENT_COMMON_KEYS, "byte_count", "sha256", "offset", "data_base64", "next_offset"],
  implicit_empty: [...CONTENT_COMMON_KEYS, "byte_count", "offset", "data_base64", "next_offset"],
  metadata_only: [...CONTENT_COMMON_KEYS, "node"],
  unavailable: [...CONTENT_COMMON_KEYS, "reason", "node"],
};

function isLowercaseSha256(value) {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function isSafeIndex(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isPositiveSafeIndex(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1;
}

/** The exact snake_case node union of the changes metadata, validated
 * for the content binding check. */
function expectChangeNode(node) {
  if (!isRecord(node)) {
    throw new UiContractError("invalid_shape");
  }
  if (node["kind"] === "directory") {
    expectShape(node, ["kind", "mode"]);
  } else if (node["kind"] === "symlink") {
    expectShape(node, ["kind", "target"]);
    if (typeof node["target"] !== "string") {
      throw new UiContractError("invalid_shape");
    }
  } else if (node["kind"] === "regular_file") {
    expectShape(node, node["multiple_links"] === true ? ["kind", "mode", "byte_count", "sha256", "multiple_links", "hardlink_group"] : ["kind", "mode", "byte_count", "sha256", "multiple_links"]);
    if (
      !isSafeIndex(node["mode"]) ||
      !isSafeIndex(node["byte_count"]) ||
      !isLowercaseSha256(node["sha256"]) ||
      (node["multiple_links"] !== false && node["multiple_links"] !== true) ||
      (node["multiple_links"] === true && !isPositiveSafeIndex(node["hardlink_group"]))
    ) {
      throw new UiContractError("invalid_shape");
    }
  } else {
    throw new UiContractError("invalid_shape");
  }
  return node;
}

function expectContentBinding(expected) {
  if (!isRecord(expected)) {
    throw new UiContractError("invalid_request");
  }
  const runId = expected["runId"];
  const observation = expected["observation"];
  const changeIndex = expected["changeIndex"];
  const side = expected["side"];
  if (
    !isSafeId(runId) ||
    typeof observation !== "string" || !HEX64_PATTERN.test(observation) ||
    !isPositiveSafeIndex(changeIndex) ||
    typeof side !== "string" || !CONTENT_SIDE_KEYS.includes(side)
  ) {
    throw new UiContractError("invalid_request");
  }
  return { runId, observation, changeIndex, side };
}

/** The decoded bytes of one canonical base64 string; a malformed,
 * non-canonical or length-mismatching value is a contract failure.
 * The decoded bytes are the only carrier of content in the model. */
function expectBase64Bytes(value) {
  if (typeof value !== "string") {
    throw new UiContractError("invalid_shape");
  }
  const length = base64ByteLength(value);
  if (length < 0) {
    throw new UiContractError("invalid_shape");
  }
  let decoded;
  try {
    decoded = atob(value);
  } catch {
    throw new UiContractError("invalid_shape");
  }
  if (decoded.length !== length) {
    throw new UiContractError("invalid_shape");
  }
  // a fresh private array; typed arrays cannot be frozen in the
  // runtime, the model object freezing covers the reference
  return Uint8Array.from(decoded, (char) => char.charCodeAt(0));
}

/**
 * The dedicated parser of one project-change-content document (HTTP
 * schema version 1): exact own keys per status variant, the closed
 * status/side/reason vocabularies, canonical safe integers, lowercase
 * SHA-256 digests, base64/byte-count/offset/next-offset coherence and
 * the exact binding to the requested run id, observation, change index
 * and side. Only the `available` status carries content bytes, and
 * they live exclusively in the frozen `data` byte array — never in a
 * diagnostic, never in a URL. The general `parseApiDocument` keeps
 * rejecting this kind fail-closed.
 */
export function parseProjectChangeContentDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "project_change_content") {
    throw new UiContractError("unknown_kind");
  }
  const binding = expected === null ? null : expectContentBinding(expected);
  const status = document["status"];
  if (typeof status !== "string" || !CONTENT_STATUS_KEYS.includes(status)) {
    throw new UiContractError("invalid_shape");
  }
  expectShape(document, CONTENT_STATUS_KEYS_MAP[status]);
  const runId = expectString(document, "run_id");
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_shape");
  }
  if (binding !== null && runId !== binding.runId) {
    throw new UiContractError("unexpected_run");
  }
  const revision = document["revision"];
  if (!isPositiveSafeIndex(revision)) {
    throw new UiContractError("invalid_shape");
  }
  const observation = expectString(document, "observation");
  if (!HEX64_PATTERN.test(observation)) {
    throw new UiContractError("invalid_shape");
  }
  if (binding !== null && observation !== binding.observation) {
    throw new UiContractError("observation_mismatch");
  }
  const changeIndex = document["change_index"];
  if (!isPositiveSafeIndex(changeIndex)) {
    throw new UiContractError("invalid_shape");
  }
  if (binding !== null && changeIndex !== binding.changeIndex) {
    throw new UiContractError("invalid_shape");
  }
  const side = document["side"];
  if (typeof side !== "string" || !CONTENT_SIDE_KEYS.includes(side)) {
    throw new UiContractError("invalid_shape");
  }
  if (binding !== null && side !== binding.side) {
    throw new UiContractError("invalid_shape");
  }
  const base = {
    kind: "project_change_content",
    runId,
    revision,
    observation,
    changeIndex,
    side,
    status,
  };
  if (status === "implicit_empty") {
    if (document["byte_count"] !== 0 || document["offset"] !== 0 || document["data_base64"] !== "" || document["next_offset"] !== null) {
      throw new UiContractError("invalid_shape");
    }
    return deepFreezeModel({ ...base, status, byteCount: 0, offset: 0, data: new Uint8Array(0), nextOffset: null });
  }
  if (status === "metadata_only") {
    const node = expectChangeNode(document["node"]);
    if (node["kind"] === "regular_file") {
      throw new UiContractError("invalid_shape");
    }
    return deepFreezeModel({ ...base, status, node });
  }
  if (status === "unavailable") {
    const reason = document["reason"];
    if (typeof reason !== "string" || !CONTENT_UNAVAILABLE_REASONS.includes(reason)) {
      throw new UiContractError("invalid_shape");
    }
    const node = expectChangeNode(document["node"]);
    if (node["kind"] !== "regular_file") {
      throw new UiContractError("invalid_shape");
    }
    return deepFreezeModel({ ...base, status, reason, node });
  }
  const byteCount = document["byte_count"];
  const sha256 = document["sha256"];
  const offset = document["offset"];
  const nextOffset = document["next_offset"];
  if (!isSafeIndex(byteCount) || !isLowercaseSha256(sha256) || !isSafeIndex(offset)) {
    throw new UiContractError("invalid_shape");
  }
  if (nextOffset !== null && !isSafeIndex(nextOffset)) {
    throw new UiContractError("invalid_shape");
  }
  const data = expectBase64Bytes(document["data_base64"]);
  if (offset > byteCount || offset + data.length > byteCount) {
    throw new UiContractError("invalid_shape");
  }
  if (nextOffset === null) {
    // the terminal chunk: either the empty page at the exact end or the
    // final slice reaching the end
    if (data.length === 0 ? offset !== byteCount : offset + data.length !== byteCount) {
      throw new UiContractError("invalid_shape");
    }
  } else {
    if (nextOffset !== offset + data.length || nextOffset >= byteCount) {
      throw new UiContractError("invalid_shape");
    }
    if (data.length === 0) {
      throw new UiContractError("invalid_shape");
    }
  }
  return deepFreezeModel({ ...base, status, byteCount, sha256, offset, data, nextOffset });
}

function deepFreezeModel(model) {
  return Object.freeze(model);
}

/**
 * The one document classifier of the content endpoint: a well-formed
 * `project_change_content` document bound to the expected request, or
 * one of the endpoint's documented failure envelopes (`error`, or the
 * failed `command_result` of a rejected content read). A succeeded
 * command result is unsuitable and fails closed.
 */
function parseProjectChangeContentEndpointDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "project_change_content") {
    return parseProjectChangeContentDocument(document, expected);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/**
 * The content endpoint client: it reuses the one bounded JSON response
 * primitive and classifies the document by kind, never by message
 * text. Content bytes never appear in a diagnostic.
 */
export async function requestProjectChangeContentJson(fetchImpl, path, init, expected) {
  return requestJsonDocument(fetchImpl, path, init, (document) => parseProjectChangeContentEndpointDocument(document, expected));
}

/**
 * The one URL builder of every content request. Only the run id, the
 * change index, the fixed side, the observation identity and the
 * raw-byte cursor travel in the URL: never a project path, a change
 * path, a digest, node metadata, a project id or a run root.
 */
export function projectChangeContentRequestPath(request) {
  if (!isRecord(request)) {
    throw new UiContractError("invalid_request");
  }
  const runId = request["runId"];
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  const changeIndex = request["changeIndex"];
  if (!isPositiveSafeIndex(changeIndex)) {
    throw new UiContractError("invalid_cursor");
  }
  const side = request["side"];
  if (typeof side !== "string" || !CONTENT_SIDE_KEYS.includes(side)) {
    throw new UiContractError("invalid_request");
  }
  const observation = request["observation"];
  if (typeof observation !== "string" || !HEX64_PATTERN.test(observation)) {
    throw new UiContractError("invalid_request");
  }
  const offset = request["offset"];
  if (offset !== undefined && (offset === null || !isSafeIndex(offset))) {
    throw new UiContractError("invalid_cursor");
  }
  const limit = request["limit"];
  if (limit !== undefined && (limit === null || !isPositiveSafeIndex(limit) || limit > 262144)) {
    throw new UiContractError("invalid_request");
  }
  let path = `/v1/runs/${runId}/changes/${changeIndex}/content?side=${side}&observation=${observation}`;
  if (offset !== undefined && offset !== 0) {
    path += `&offset=${offset}`;
  }
  if (limit !== undefined && limit !== CONTENT_PREVIEW_CHUNK_LIMIT) {
    path += `&limit=${limit}`;
  }
  return path;
}

/**
 * The single-flight transport of the content viewer: at most one
 * unsettled content GET exists at any instant. A new explicit Review
 * supersedes the active request and coalesces as the one follow-up; a
 * chunk continuation while a request is on the wire is declined
 * without a second GET. invalidate/dispose withdraw the active and
 * queued desires. There is no timer, no polling and no retry loop.
 */
export function createProjectChangeContentTransport(io) {
  if (
    !isRecord(io) ||
    typeof io["fetchContent"] !== "function" ||
    typeof io["onStarted"] !== "function" ||
    typeof io["onSettled"] !== "function"
  ) {
    throw new UiContractError("invalid_transport_io");
  }
  const fetchContent = io["fetchContent"];
  const onStarted = io["onStarted"];
  const onSettled = io["onSettled"];
  let active = null;
  let desired = null;
  let disposed = false;

  const startRequest = (request) => {
    if (active !== null) {
      throw new UiContractError("transport_slot_busy");
    }
    active = request;
    const controller = new AbortController();
    request.controller = controller;
    onStarted(request.runId);
    void (async () => {
      let result;
      try {
        result = await fetchContent(request.runId, request.path, controller.signal, request.expected);
      } catch {
        result = undefined;
      } finally {
        settleActive(request, result);
      }
    })();
  };

  const settleActive = (request, result) => {
    try {
      if (request.superseded) {
        if (desired === null) {
          onSettled(request.runId);
        }
      } else {
        request.onApplied(result);
        onSettled(request.runId);
      }
    } finally {
      active = null;
      const followUp = desired;
      desired = null;
      request.resolve(result);
      if (followUp !== null && !disposed) {
        startRequest(followUp);
      }
    }
  };

  return Object.freeze({
    request(desire) {
      const runId = isRecord(desire) && isSafeId(desire["runId"]) ? desire["runId"] : null;
      const path = isRecord(desire) && typeof desire["path"] === "string" ? desire["path"] : null;
      const kind = isRecord(desire) && (desire["kind"] === "initial" || desire["kind"] === "continuation") ? desire["kind"] : null;
      const side = isRecord(desire) && typeof desire["side"] === "string" && CONTENT_SIDE_KEYS.includes(desire["side"]) ? desire["side"] : null;
      const expected = isRecord(desire) && isRecord(desire["expected"]) ? desire["expected"] : null;
      const onApplied = isRecord(desire) && typeof desire["onApplied"] === "function" ? desire["onApplied"] : null;
      let resolveResult = null;
      const settled = new Promise((resolve) => {
        resolveResult = resolve;
      });
      if (disposed || runId === null || path === null || kind === null || side === null || expected === null || onApplied === null) {
        resolveResult(undefined);
        return settled;
      }
      const request = { runId, path, kind, side, expected, onApplied, superseded: false, controller: null, resolve: resolveResult };
      if (active === null) {
        startRequest(request);
      } else if (kind === "continuation") {
        // a chunk continuation never displaces or queues behind another
        // request: the viewer declines it and no stale bytes are fetched
        resolveResult(undefined);
        return settled;
      } else {
        active.superseded = true;
        active.controller.abort();
        const replaced = desired;
        desired = request;
        if (replaced !== null) {
          replaced.resolve(undefined);
        }
      }
      return settled;
    },
    invalidate() {
      if (active !== null) {
        active.superseded = true;
        active.controller.abort();
      }
      const withdrawn = desired;
      desired = null;
      if (withdrawn !== null) {
        withdrawn.resolve(undefined);
      }
    },
    dispose() {
      disposed = true;
      this.invalidate();
    },
  });
}

/* The bounded UTF-8 scanner: the one strict validator of raw preview
 * bytes. It returns the offset of the first invalid byte (-1 when the
 * bytes are valid), whether a trailing incomplete multi-byte sequence
 * exists, and the offset where that trailing sequence starts (-1 when
 * there is none). Overlong forms, surrogates and out-of-range code
 * points are invalid; a truncated sequence is invalid unless it sits
 * exactly at the end of an unfinished preview. */
function scanUtf8(bytes) {
  const length = bytes.length;
  let index = 0;
  while (index < length) {
    const lead = bytes[index];
    let continuationCount = 0;
    let minimum = 0;
    let maximum = 0x10ffff;
    if (lead < 0x80) {
      index += 1;
      continue;
    }
    if (lead >= 0xc2 && lead <= 0xdf) {
      continuationCount = 1;
    } else if (lead === 0xe0) {
      continuationCount = 2;
      minimum = 0xa0;
    } else if ((lead >= 0xe1 && lead <= 0xec) || lead === 0xee || lead === 0xef) {
      continuationCount = 2;
      minimum = 0x80;
    } else if (lead === 0xed) {
      continuationCount = 2;
      minimum = 0x80;
      maximum = 0x9f;
    } else if (lead === 0xf0) {
      continuationCount = 3;
      minimum = 0x90;
    } else if ((lead >= 0xf1 && lead <= 0xf3)) {
      continuationCount = 3;
      minimum = 0x80;
    } else if (lead === 0xf4) {
      continuationCount = 3;
      minimum = 0x80;
      maximum = 0x8f;
    } else {
      return { invalidAt: index, incompleteTailAt: -1 };
    }
    if (index + continuationCount >= length && index + continuationCount === length) {
      // a truncated sequence sits exactly at the end: report it as the
      // trailing incomplete tail, not as an invalid byte
      return { invalidAt: -1, incompleteTailAt: index };
    }
    for (let k = 1; k <= continuationCount; k += 1) {
      const byte = bytes[index + k];
      const lower = k === 1 ? minimum : 0x80;
      const upper = k === 1 ? maximum : 0xbf;
      if (byte === undefined || byte < lower || byte > upper) {
        return { invalidAt: index, incompleteTailAt: -1 };
      }
    }
    index += continuationCount + 1;
  }
  return { invalidAt: -1, incompleteTailAt: -1 };
}

/**
 * The text/binary classification of one bounded preview. Raw bytes are
 * the source of truth: any NUL byte or an invalid UTF-8 sequence makes
 * the side binary; an incomplete trailing sequence is tolerated only
 * while the preview is unfinished (`incompleteTail`) and is excluded
 * from the rendered text. ANSI escapes and markup-looking text stay
 * plain text — the render path is `textContent` only.
 */
export function decodePreviewText(bytes, options) {
  const incompleteTail = isRecord(options) && options["incompleteTail"] === true;
  for (let index = 0; index < bytes.length; index += 1) {
    if (bytes[index] === 0) {
      return Object.freeze({ kind: "binary" });
    }
  }
  const scan = scanUtf8(bytes);
  if (scan.invalidAt !== -1) {
    return Object.freeze({ kind: "binary" });
  }
  let textBytes = bytes;
  if (scan.incompleteTailAt !== -1) {
    if (!incompleteTail) {
      return Object.freeze({ kind: "binary" });
    }
    textBytes = bytes.subarray(0, scan.incompleteTailAt);
  }
  const text = new TextDecoder("utf-8", { fatal: false }).decode(textBytes);
  return Object.freeze({ kind: "text", text });
}

/** The bounded hex rows of one preview: fixed raw-byte offsets, hex
 * pairs and a safe ASCII column; every non-printable byte renders as a
 * dot and nothing is ever interpreted or executed. */
export function hexPreviewRows(bytes, maxBytes) {
  const displayLimit = isSafeIndex(maxBytes) ? maxBytes : CONTENT_HEX_DISPLAY_BYTES;
  const shown = bytes.length > displayLimit ? bytes.subarray(0, displayLimit) : bytes;
  const rows = [];
  for (let offset = 0; offset < shown.length; offset += 16) {
    const slice = shown.subarray(offset, Math.min(offset + 16, shown.length));
    let hex = "";
    let ascii = "";
    for (let index = 0; index < 16; index += 1) {
      if (index < slice.length) {
        const byte = slice[index];
        hex += byte.toString(16).padStart(2, "0") + (index === 7 ? "  " : " ");
        ascii += byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : ".";
      } else {
        hex += index === 7 ? "   " : "   ";
        ascii += " ";
      }
    }
    rows.push(Object.freeze({
      offset: offset.toString(16).padStart(8, "0"),
      hex: hex.trimEnd(),
      ascii: ascii.trimEnd(),
    }));
  }
  return Object.freeze(rows);
}

function splitDiffLines(bytes) {
  const lines = [];
  let start = 0;
  for (let index = 0; index < bytes.length; index += 1) {
    if (bytes[index] === 0x0a) {
      lines.push(Object.freeze({ text: new TextDecoder("utf-8", { fatal: false }).decode(bytes.subarray(start, index)), noEol: false }));
      start = index + 1;
    }
  }
  if (start < bytes.length) {
    lines.push(Object.freeze({ text: new TextDecoder("utf-8", { fatal: false }).decode(bytes.subarray(start, bytes.length)), noEol: true }));
  }
  return lines;
}

/** The diff comparison key of one line: the missing final newline is a
 * real difference, so the no-EOL flag participates in the key. */
function diffLineKey(line) {
  return `${line.text}\u0000${line.noEol ? "n" : "y"}`;
}

/**
 * The one deterministic bounded line-diff owner. Both inputs are
 * complete raw byte arrays; a non-UTF-8 or NUL-bearing side refuses
 * with `not_text`, more than 2000 lines per side with
 * `too_many_lines` and an LCS work table above 250000 cells with
 * `work_budget` — never a hang and never a partial misleading diff.
 * Identical inputs short-circuit. The output rows are plain text and
 * render through `textContent` only.
 */
export function computeBoundedLineDiff(leftBytes, rightBytes) {
  if (!(leftBytes instanceof Uint8Array) || !(rightBytes instanceof Uint8Array)) {
    throw new UiContractError("invalid_shape");
  }
  for (const bytes of [leftBytes, rightBytes]) {
    for (let index = 0; index < bytes.length; index += 1) {
      if (bytes[index] === 0) {
        return Object.freeze({ kind: "refusal", reason: "not_text" });
      }
    }
    if (scanUtf8(bytes).invalidAt !== -1) {
      return Object.freeze({ kind: "refusal", reason: "not_text" });
    }
  }
  if (leftBytes.length === rightBytes.length) {
    let identical = true;
    for (let index = 0; index < leftBytes.length; index += 1) {
      if (leftBytes[index] !== rightBytes[index]) {
        identical = false;
        break;
      }
    }
    if (identical) {
      return Object.freeze({ kind: "diff", identical: true, rows: Object.freeze([]) });
    }
  }
  const left = splitDiffLines(leftBytes);
  const right = splitDiffLines(rightBytes);
  if (left.length > DIFF_MAX_LINES_PER_SIDE || right.length > DIFF_MAX_LINES_PER_SIDE) {
    return Object.freeze({ kind: "refusal", reason: "too_many_lines" });
  }
  const rowsCount = left.length;
  const columnsCount = right.length;
  if (rowsCount * columnsCount > DIFF_MAX_WORK_CELLS) {
    return Object.freeze({ kind: "refusal", reason: "work_budget" });
  }
  // the bounded LCS table; the work budget bounds its size
  const table = new Uint32Array((rowsCount + 1) * (columnsCount + 1));
  const width = columnsCount + 1;
  for (let i = rowsCount - 1; i >= 0; i -= 1) {
    for (let j = columnsCount - 1; j >= 0; j -= 1) {
      table[i * width + j] = diffLineKey(left[i]) === diffLineKey(right[j])
        ? table[(i + 1) * width + j + 1] + 1
        : Math.max(table[(i + 1) * width + j], table[i * width + j + 1]);
    }
  }
  const rows = [];
  let i = 0;
  let j = 0;
  while (i < rowsCount && j < columnsCount) {
    if (diffLineKey(left[i]) === diffLineKey(right[j])) {
      rows.push(Object.freeze({ type: "context", text: left[i].text, noEol: left[i].noEol }));
      i += 1;
      j += 1;
    } else if (table[(i + 1) * width + j] >= table[i * width + j + 1]) {
      rows.push(Object.freeze({ type: "delete", text: left[i].text, noEol: left[i].noEol }));
      i += 1;
    } else {
      rows.push(Object.freeze({ type: "add", text: right[j].text, noEol: right[j].noEol }));
      j += 1;
    }
  }
  while (i < rowsCount) {
    rows.push(Object.freeze({ type: "delete", text: left[i].text, noEol: left[i].noEol }));
    i += 1;
  }
  while (j < columnsCount) {
    rows.push(Object.freeze({ type: "add", text: right[j].text, noEol: right[j].noEol }));
    j += 1;
  }
  return Object.freeze({ kind: "diff", identical: false, rows: Object.freeze(rows) });
}

/** The structural equality of two change nodes: kind plus every
 * metadata field the node kind carries. */
function sameChangeNode(left, right) {
  if (left === right) {
    return true;
  }
  if (left === null || right === null || left.kind !== right.kind) {
    return false;
  }
  if (left.kind === "directory") {
    return left.mode === right.mode;
  }
  if (left.kind === "symlink") {
    return left.target === right.target;
  }
  return left.mode === right.mode && left.byte_count === right.byte_count && left.sha256 === right.sha256;
}

/**
 * The closed state machine of the inline content review panel. It owns
 * no fetch and no URL: every desire is handed to the single-flight
 * content transport. An explicit Review is the only path that starts a
 * content GET — up to two sequential requests (baseline, then result)
 * for one selected change entry of the current observation; chunk
 * continuations and retries stay explicit user actions. The viewer
 * verifies every response against the selected change entry's metadata
 * (digest, byte count, node kind, status coherence) and fails closed
 * with zero rendered bytes on any mismatch. A stale observation
 * discards the loaded bytes of BOTH sides and offers only the explicit
 * Project Changes reload. The panel never POSTs, never touches
 * inspect/events/logs/mutations and never starts a worker.
 */
export function createProjectChangeContentViewer(io) {
  if (
    !isRecord(io) ||
    typeof io["transport"] !== "object" ||
    typeof io["onState"] !== "function"
  ) {
    throw new UiContractError("invalid_viewer_io");
  }
  const transport = io["transport"];
  const onState = io["onState"];
  let state = null;

  const publish = () => {
    onState(state);
  };

  const unrequestedSide = () => Object.freeze({
    phase: "unrequested",
    byteCount: null,
    sha256: null,
    loadedBytes: 0,
    nextOffset: null,
    capped: false,
    data: null,
    error: null,
  });

  const failureOf = (result) => {
    if (result !== undefined && result !== null && typeof result === "object" && result.ok === false) {
      return result.failure;
    }
    const model = result !== undefined && result !== null && typeof result === "object" ? result.model : null;
    if (model !== null && typeof model === "object" && model.kind === "error") {
      return Object.freeze({ layer: model.layer, reason: model.reason });
    }
    if (model !== null && typeof model === "object" && model.kind === "command_result") {
      return Object.freeze({ layer: "orchestrator", reason: model.reason ?? "failed" });
    }
    return Object.freeze({ layer: "contract", reason: "unexpected_kind" });
  };

  const bothSides = () => ({ baseline: state.baseline, result: state.result });

  const markStale = () => {
    transport.invalidate();
    state = Object.freeze({
      ...state,
      baseline: unrequestedSide(),
      result: unrequestedSide(),
      requestState: "stale",
      stale: true,
      error: null,
    });
    publish();
  };

  /** The coherence of one content response against the selected
   * change entry's metadata; any mismatch is a contract failure with
   * zero rendered bytes. */
  const verifyAgainstEntry = (model, side) => {
    const node = state.entry[side];
    if (model.runId !== state.runId || model.observation !== state.observation || model.changeIndex !== state.changeIndex || model.side !== side) {
      throw new UiContractError("invalid_shape");
    }
    if (model.status === "available") {
      if (node === null || node.kind !== "regular_file" || node.byte_count !== model.byteCount || node.sha256 !== model.sha256) {
        throw new UiContractError("invalid_shape");
      }
      return;
    }
    if (model.status === "implicit_empty") {
      if (node !== null) {
        throw new UiContractError("invalid_shape");
      }
      return;
    }
    if (model.status === "metadata_only") {
      if (node === null || node.kind === "regular_file" || !sameChangeNode(node, model.node)) {
        throw new UiContractError("invalid_shape");
      }
      return;
    }
    // unavailable: only the baseline side of a regular-file node whose
    // metadata still matches the verified entry
    if (side !== "baseline" || node === null || node.kind !== "regular_file" || !sameChangeNode(node, model.node)) {
      throw new UiContractError("invalid_shape");
    }
  };

  const applySideModel = (model, side) => {
    const retained = model.status === "available" ? model.data : null;
    // the loading phase preserves the accumulated prefix bytes; a
    // continuation appends when the slice continues exactly at the end
    const previous = offsetLoadsAccumulate(state[side], model) ? state[side].data : null;
    const merged = retained === null
      ? null
      : previous === null || previous.length === 0
        ? retained
        : concatBytes(previous, retained);
    const loadedBytes = merged === null ? 0 : merged.length;
    const capped = model.status === "available" && model.byteCount > CONTENT_PREVIEW_MAX_BYTES && loadedBytes >= CONTENT_PREVIEW_MAX_BYTES;
    state = Object.freeze({
      ...state,
      requestState: "ready",
      [side]: Object.freeze({
        phase: model.status,
        byteCount: model.status === "available" || model.status === "implicit_empty" ? model.byteCount : null,
        sha256: model.status === "available" ? model.sha256 : null,
        loadedBytes,
        nextOffset: model.status === "available" ? model.nextOffset : null,
        capped,
        data: merged,
        error: null,
      }),
    });
    publish();
  };

  /** A continuation appends to the retained bytes only when the new
   * slice continues exactly where the retained prefix ended. */
  function offsetLoadsAccumulate(sideState, model) {
    return model.offset === sideState.loadedBytes && sideState.loadedBytes > 0;
  }

  function concatBytes(left, right) {
    const merged = new Uint8Array(left.length + right.length);
    merged.set(left, 0);
    merged.set(right, left.length);
    return merged;
  }

  const applySideFailure = (failure, side) => {
    state = Object.freeze({
      ...state,
      requestState: "ready",
      [side]: Object.freeze({
        ...state[side],
        phase: "error",
        data: null,
        error: failure,
      }),
    });
    publish();
  };

  const settleSide = (result, side, kind, requestedRunId, requestedObservation, requestedChangeIndex, generation) => {
    if (state === null || generation !== state.generation) {
      return;
    }
    if (state.runId !== requestedRunId || state.observation !== requestedObservation || state.changeIndex !== requestedChangeIndex) {
      return;
    }
    if (result === undefined) {
      // superseded: a new review or an invalidation already reset the
      // state; a declined continuation is restored below
      return;
    }
    if (!result.ok) {
      const failure = failureOf(result);
      if (failure.reason === "stale_observation") {
        markStale();
        return;
      }
      applySideFailure(failure, side);
      return;
    }
    const model = result.model;
    if (model.kind !== "project_change_content") {
      const failure = failureOf(result);
      if (failure.reason === "stale_observation") {
        markStale();
        return;
      }
      applySideFailure(failure, side);
      return;
    }
    try {
      verifyAgainstEntry(model, side);
    } catch (error) {
      applySideFailure(Object.freeze({ layer: "contract", reason: error instanceof UiContractError ? error.reason : "invalid_shape" }), side);
      return;
    }
    applySideModel(model, side);
  };

  const restoreDeclinedSide = (side, generation, previousPhase) => {
    if (state === null || generation !== state.generation) {
      return;
    }
    const sideState = state[side];
    if (sideState.phase !== "loading") {
      return;
    }
    state = Object.freeze({
      ...state,
      requestState: "ready",
      [side]: Object.freeze({ ...sideState, phase: previousPhase }),
    });
    publish();
  };

  const requestSide = (side, kind, generation) => {
    if (state === null || generation !== state.generation) {
      return;
    }
    const sideState = state[side];
    if (sideState.phase === "loading" || sideState.capped) {
      return;
    }
    if (sideState.phase !== "unrequested" && sideState.phase !== "error" && !(sideState.phase === "available" && sideState.nextOffset !== null)) {
      return;
    }
    const requestedRunId = state.runId;
    const requestedObservation = state.observation;
    const requestedChangeIndex = state.changeIndex;
    const offset = sideState.phase === "available" && sideState.nextOffset !== null ? sideState.nextOffset : 0;
    const remaining = Math.max(0, CONTENT_PREVIEW_MAX_BYTES - offset);
    if (remaining === 0) {
      return;
    }
    const limit = Math.min(CONTENT_PREVIEW_CHUNK_LIMIT, remaining);
    const previousPhase = sideState.phase;
    const expected = { runId: requestedRunId, observation: requestedObservation, changeIndex: requestedChangeIndex, side };
    state = Object.freeze({
      ...state,
      requestState: "loading",
      [side]: Object.freeze({ ...state[side], phase: "loading" }),
    });
    publish();
    const settled = transport.request({
      runId: requestedRunId,
      kind,
      side,
      expected,
      path: projectChangeContentRequestPath({ runId: requestedRunId, changeIndex: requestedChangeIndex, side, observation: requestedObservation, offset, limit }),
      onApplied: (result) => {
        settleSide(result, side, kind, requestedRunId, requestedObservation, requestedChangeIndex, generation);
        if (generation === (state === null ? -1 : state.generation) && kind === "initial" && side === "baseline") {
          const follow = state === null ? null : state.result;
          if (state !== null && state.requestState !== "stale" && follow !== null && follow.phase === "unrequested") {
            requestSide("result", "initial", generation);
          }
        }
      },
    });
    // a declined continuation (the slot was busy) never calls
    // onApplied: restore the side instead of leaving it loading
    void settled.then((result) => {
      if (result === undefined && kind === "continuation") {
        restoreDeclinedSide(side, generation, previousPhase);
      }
    });
  };

  return Object.freeze({
    /** One explicit Review of one selected change entry: resets both
     * sides, then requests baseline and result sequentially (up to two
     * GETs for one user action). */
    review(entry) {
      if (
        state === null ||
        !isRecord(entry) ||
        !isPositiveSafeIndex(entry["index"]) ||
        typeof entry["path"] !== "string" ||
        typeof entry["change"] !== "string" ||
        state.observation === null
      ) {
        throw new UiContractError("invalid_request");
      }
      const index = entry["index"];
      if (index > state.changesCount) {
        throw new UiContractError("invalid_request");
      }
      const baseline = entry["baseline"] === null ? null : expectChangeNode(entry["baseline"]);
      const result = entry["result"] === null ? null : expectChangeNode(entry["result"]);
      transport.invalidate();
      state = Object.freeze({
        runId: state.runId,
        observation: state.observation,
        changesCount: state.changesCount,
        changeIndex: index,
        changePath: entry["path"],
        changeKind: entry["change"],
        entry: Object.freeze({ baseline, result }),
        generation: (state.generation ?? 0) + 1,
        requestState: "loading",
        stale: false,
        error: null,
        baseline: unrequestedSide(),
        result: unrequestedSide(),
      });
      publish();
      requestSide("baseline", "initial", state.generation);
    },
    /** The explicit chunk continuation of one side; declined while the
     * transport is busy and refused once the preview cap is reached. */
    loadMoreSide(side) {
      if (typeof side !== "string" || !CONTENT_SIDE_KEYS.includes(side)) {
        throw new UiContractError("invalid_request");
      }
      if (state === null) {
        return;
      }
      const sideState = state[side];
      if (sideState.phase !== "available" || sideState.nextOffset === null || sideState.capped) {
        return;
      }
      requestSide(side, "continuation", state.generation);
    },
    /** The explicit retry of one failed side; only a transport failure
     * is recoverable, and only by this click. */
    retrySide(side) {
      if (typeof side !== "string" || !CONTENT_SIDE_KEYS.includes(side)) {
        throw new UiContractError("invalid_request");
      }
      if (state === null) {
        return;
      }
      const sideState = state[side];
      if (sideState.phase !== "error" || sideState.error === null || sideState.error.layer !== "transport") {
        return;
      }
      requestSide(side, "continuation", state.generation);
    },
    /** Closes the review: the selection and the loaded secret-bearing
     * bytes are discarded and the transport is invalidated; a late
     * response can never apply. */
    closeReview() {
      if (state === null) {
        return;
      }
      transport.invalidate();
      state = Object.freeze({
        ...state,
        generation: state.generation + 1,
        changeIndex: null,
        changePath: null,
        changeKind: null,
        entry: null,
        requestState: "idle",
        stale: false,
        error: null,
        baseline: unrequestedSide(),
        result: unrequestedSide(),
      });
      publish();
    },
    /** The metadata-driven invalidation: a changed run, a changed
     * observation or a disposed view discards the loaded secret-bearing
     * bytes and withdraws the wire; nothing is re-fetched. */
    invalidateMetadata() {
      if (state === null) {
        return;
      }
      transport.invalidate();
      state = Object.freeze({
        ...state,
        generation: state.generation + 1,
        changeIndex: null,
        changePath: null,
        changeKind: null,
        entry: null,
        requestState: "idle",
        stale: false,
        error: null,
        baseline: unrequestedSide(),
        result: unrequestedSide(),
      });
      publish();
    },
    transitionToRun(runId) {
      if (!isSafeId(runId)) {
        throw new UiContractError("invalid_run_id");
      }
      transport.invalidate();
      state = Object.freeze({
        runId,
        observation: null,
        changesCount: 0,
        changeIndex: null,
        changePath: null,
        changeKind: null,
        entry: null,
        generation: state === null ? 1 : state.generation + 1,
        requestState: "idle",
        stale: false,
        error: null,
        baseline: unrequestedSide(),
        result: unrequestedSide(),
      });
      publish();
    },
    /** The changes metadata was applied: the viewer records the run
     * binding, the observation and the change count it may later review
     * against. No content GET is ever issued here. */
    metadataApplied(changesState) {
      if (
        state === null ||
        !isRecord(changesState) ||
        changesState.runId !== state.runId ||
        typeof changesState.observation !== "string" ||
        !HEX64_PATTERN.test(changesState.observation)
      ) {
        return;
      }
      const changed = state.observation !== changesState.observation;
      state = Object.freeze({
        ...state,
        observation: changesState.observation,
        changesCount: Array.isArray(changesState.changes) ? changesState.changes.length : state.changesCount,
      });
      if (changed) {
        this.invalidateMetadata();
        return;
      }
      publish();
    },
    currentState() {
      return state;
    },
  });
}

/* Live observation (v3): bounded read-only polling of one selected    */
/* run; the corrective worker-logs viewer sits before this region and  */
/* carries no scheduler, timer or mutation capability of its own.      */

export const LIVE_OBSERVATION_POLL_DELAY_MS = 2000;

function noEffect() {
  return Object.freeze({ effect: "no_op" });
}

function cancelTimerEffect() {
  return Object.freeze({ effect: "cancel_timer" });
}

function startInspectEffect() {
  return Object.freeze({ effect: "start_inspect" });
}

function renderStateEffect() {
  return Object.freeze({ effect: "render_state" });
}

function scheduleTimerEffect() {
  return Object.freeze({ effect: "schedule_timer", delayMs: LIVE_OBSERVATION_POLL_DELAY_MS });
}

function isObservationSnapshot(value) {
  return (
    isRecord(value) &&
    typeof value["status"] === "string" &&
    typeof value["boundaryKind"] === "string" &&
    typeof value["revision"] === "number" &&
    Number.isSafeInteger(value["revision"]) &&
    value["revision"] >= 0
  );
}

/**
 * The one live-observation scheduling owner. Inputs are semantic
 * events; outputs are declarative effects only (schedule_timer,
 * cancel_timer, start_inspect, render_state, no_op) — the machine never
 * touches the network, the DOM or timers itself and never produces a
 * mutating request. Polling is eligible exactly while the last applied
 * authoritative inspect shows `running` with boundary kind `in_flight`
 * and the tab is visible, live updates are enabled and no mutation is
 * in flight. Every other boundary, an error or a lost response stops
 * the observation; only an explicit user action (refresh, re-enable,
 * re-selection) runs a new inspect whose success may restart it.
 */
export function createLiveObservationScheduler() {
  let selectedRunId = null;
  let snapshot = null;
  let documentVisible = true;
  let liveEnabled = true;
  let mutationInFlight = false;
  let inspectInFlight = false;
  let timerPending = false;
  let stoppedAfterError = false;
  let disposed = false;

  const pollingEligible = () =>
    snapshot !== null && snapshot["status"] === "running" && snapshot["boundaryKind"] === "in_flight";

  const canSchedule = () =>
    !disposed && pollingEligible() && !stoppedAfterError && documentVisible && liveEnabled && !mutationInFlight;

  const immediateInspectAllowed = (afterError) =>
    !disposed &&
    pollingEligible() &&
    documentVisible &&
    !mutationInFlight &&
    !inspectInFlight &&
    selectedRunId !== null &&
    (afterError || !stoppedAfterError);

  const leaveTimer = () => {
    if (!timerPending) {
      return null;
    }
    timerPending = false;
    return cancelTimerEffect();
  };

  const presentationState = () => {
    if (disposed || selectedRunId === null) {
      return "off";
    }
    if (stoppedAfterError) {
      return "stopped";
    }
    if (pollingEligible()) {
      if (timerPending || inspectInFlight) {
        return "live";
      }
      if (!documentVisible || !liveEnabled || mutationInFlight) {
        return "paused";
      }
    }
    return "off";
  };

  return Object.freeze({
    presentationState,
    /**
     * The single-flight owner: at most one active inspect of the
     * selected run. A user-initiated inspect takes the slot over — an
     * inspect already in flight is invalidated (its response is dropped
     * by the adapter and never applied or scheduled from).
     */
    admitInspect(runId) {
      return !disposed && typeof runId === "string" && runId === selectedRunId && !inspectInFlight;
    },
    takeOverInspect(runId) {
      if (disposed || runId !== selectedRunId) {
        return noEffect();
      }
      const cancel = leaveTimer();
      inspectInFlight = true;
      return cancel ?? noEffect();
    },
    runSelected(runId) {
      if (disposed || typeof runId !== "string" || runId === "") {
        return noEffect();
      }
      const cancel = leaveTimer();
      selectedRunId = runId;
      snapshot = null;
      inspectInFlight = false;
      stoppedAfterError = false;
      return cancel ?? renderStateEffect();
    },
    inspectStarted(runId) {
      if (disposed || runId !== selectedRunId) {
        return noEffect();
      }
      inspectInFlight = true;
      return leaveTimer() ?? noEffect();
    },
    inspectSucceeded(runId, applied) {
      if (disposed || runId !== selectedRunId || !isObservationSnapshot(applied)) {
        return noEffect();
      }
      inspectInFlight = false;
      stoppedAfterError = false;
      snapshot = Object.freeze({ ...applied, runId });
      if (canSchedule()) {
        timerPending = true;
        return scheduleTimerEffect();
      }
      return leaveTimer() ?? renderStateEffect();
    },
    inspectFailed(runId) {
      if (disposed || runId !== selectedRunId) {
        return noEffect();
      }
      inspectInFlight = false;
      stoppedAfterError = true;
      return leaveTimer() ?? renderStateEffect();
    },
    inspectCancelled(runId) {
      if (disposed || runId !== selectedRunId) {
        return noEffect();
      }
      inspectInFlight = false;
      return leaveTimer() ?? noEffect();
    },
    timerFired() {
      if (disposed || !timerPending) {
        return noEffect();
      }
      timerPending = false;
      if (!canSchedule() || inspectInFlight) {
        return noEffect();
      }
      return startInspectEffect();
    },
    visibilityChanged(visible) {
      if (disposed) {
        return noEffect();
      }
      documentVisible = visible === true;
      if (!documentVisible) {
        return leaveTimer() ?? renderStateEffect();
      }
      if (immediateInspectAllowed(false)) {
        return startInspectEffect();
      }
      return renderStateEffect();
    },
    liveToggleChanged(enabled) {
      if (disposed) {
        return noEffect();
      }
      liveEnabled = enabled === true;
      if (!liveEnabled) {
        return leaveTimer() ?? renderStateEffect();
      }
      // Re-enabling is an explicit user action: it may resume even after
      // an error, but only through one inspect whose success re-confirms
      // the polling-eligible boundary.
      if (immediateInspectAllowed(true)) {
        return startInspectEffect();
      }
      return renderStateEffect();
    },
    mutationStarted() {
      if (disposed) {
        return noEffect();
      }
      mutationInFlight = true;
      // An inspect already in flight is invalidated: its response is
      // dropped by the adapter and the single-flight slot is released
      // for the authoritative post-mutation inspect.
      inspectInFlight = false;
      return leaveTimer() ?? renderStateEffect();
    },
    mutationCompleted() {
      if (disposed) {
        return noEffect();
      }
      mutationInFlight = false;
      if (canSchedule() && !inspectInFlight) {
        timerPending = true;
        return scheduleTimerEffect();
      }
      return renderStateEffect();
    },
    disposed() {
      if (disposed) {
        return noEffect();
      }
      disposed = true;
      timerPending = false;
      inspectInFlight = false;
      return cancelTimerEffect();
    },
  });
}

/**
 * The one timer adapter: the single setTimeout call site and the single
 * clearTimeout call site of the whole module. It enforces the at-most-
 * one-pending-timer invariant regardless of the effect sequence.
 */
export function createSchedulerTimerAdapter(timers, onFire) {
  if (
    !isRecord(timers) ||
    typeof timers["setTimeout"] !== "function" ||
    typeof timers["clearTimeout"] !== "function" ||
    typeof onFire !== "function"
  ) {
    throw new UiContractError("invalid_timer_source");
  }
  let handle = null;
  return Object.freeze({
    apply(effect) {
      if (!isRecord(effect)) {
        throw new UiContractError("invalid_effect");
      }
      if (effect["effect"] === "schedule_timer") {
        const delay = effect["delayMs"];
        if (typeof delay !== "number" || !Number.isSafeInteger(delay) || delay < 1) {
          throw new UiContractError("invalid_effect");
        }
        if (handle !== null) {
          timers.clearTimeout(handle);
        }
        handle = timers.setTimeout(() => {
          handle = null;
          onFire();
        }, delay);
        return;
      }
      if (effect["effect"] === "cancel_timer") {
        if (handle !== null) {
          timers.clearTimeout(handle);
          handle = null;
        }
        return;
      }
      if (effect["effect"] === "start_inspect" || effect["effect"] === "render_state" || effect["effect"] === "no_op") {
        return;
      }
      throw new UiContractError("invalid_effect");
    },
    hasPendingTimer() {
      return handle !== null;
    },
  });
}

/**
 * The change key of one applied run document: two identical inspect
 * projections share one key, so an unchanged poll never rebuilds the
 * detail DOM (focus and selection survive) and never announces.
 */
export function liveObservationDocumentKey(model) {
  if (!isRecord(model) || model["kind"] !== "run") {
    throw new UiContractError("invalid_state");
  }
  return JSON.stringify({
    runId: model["runId"],
    revision: model["revision"],
    status: model["status"],
    boundary: model["boundary"],
    outputs: model["outputs"],
  });
}

/**
 * The aria-live policy of one scheduled poll: an unchanged document is
 * silent; a changed document announces once with the new revision.
 */
export function livePollAnnouncement(previousDocumentKey, model) {
  if (liveObservationDocumentKey(model) === previousDocumentKey) {
    return null;
  }
  return `Run ${model["runId"]} updated (revision ${model["revision"]}).`;
}

/** The presentation label of the live-observation state line. */
export function liveStateLabel(state) {
  if (state === "live") {
    return "Live";
  }
  if (state === "paused") {
    return "Paused";
  }
  if (state === "stopped") {
    return "Stopped after error";
  }
  return "off";
}

/* ------------------------------------------------------------------ */
/* Lifecycle timeline (v4): cursor state, dense merge and the one      */
/* single-flight events transport.                                     */
/* ------------------------------------------------------------------ */

/** The fresh timeline of one selected run: nothing loaded, cursor 0. */
export function createTimelineState(runId) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  return Object.freeze({
    runId,
    cursor: 0,
    snapshotRevision: 0,
    events: Object.freeze([]),
    nextAfter: null,
    stopped: false,
    error: null,
  });
}

function isTimelineState(state) {
  return isRecord(state) && isSafeId(state["runId"]) && typeof state["cursor"] === "number" && Number.isSafeInteger(state["cursor"]) && state["cursor"] >= 0 && Array.isArray(state["events"]);
}

/**
 * The one dense merge of one validated events page into the timeline.
 * `requestedAfter` is the exclusive cursor the request was built with:
 * it must equal the applied cursor (a stale or misrouted page is a
 * contract error, never a silent drop-in). The first entry — when
 * present — must have `revision === after + 1`, every later revision
 * must be exactly `previous + 1`; duplicates, gaps, regressions and
 * out-of-order pages are contract errors. A non-null `nextAfter` must
 * equal the last event revision of the page; a null `nextAfter` means
 * the page reached the document revision, so an empty page is legal
 * only when `after === revision`. The cursor advances only after the
 * whole page validated; events keep their verbatim history order and
 * are never merged, grouped or reordered.
 */
export function applyEventsPage(state, page, requestedAfter) {
  if (!isTimelineState(state) || !isRecord(page) || page["kind"] !== "events") {
    throw new UiContractError("invalid_state");
  }
  if (typeof requestedAfter !== "number" || !Number.isSafeInteger(requestedAfter) || requestedAfter < 0) {
    throw new UiContractError("invalid_cursor");
  }
  if (page["runId"] !== state["runId"]) {
    throw new UiContractError("unexpected_run");
  }
  if (requestedAfter !== state["cursor"]) {
    throw new UiContractError("stale_page");
  }
  if (page["revision"] < requestedAfter) {
    throw new UiContractError("invalid_page");
  }
  const events = page["events"];
  if (events.length === 0) {
    if (page["nextAfter"] !== null || requestedAfter !== page["revision"]) {
      throw new UiContractError("invalid_page");
    }
    return Object.freeze({
      runId: state["runId"],
      cursor: state["cursor"],
      snapshotRevision: page["revision"],
      events: state["events"],
      nextAfter: null,
      stopped: false,
      error: null,
    });
  }
  let expected = requestedAfter + 1;
  for (const entry of events) {
    if (!isRecord(entry) || entry["revision"] !== expected) {
      throw new UiContractError("invalid_page");
    }
    expected += 1;
  }
  const lastRevision = events[events.length - 1]["revision"];
  if (page["nextAfter"] !== null) {
    if (page["nextAfter"] !== lastRevision) {
      throw new UiContractError("invalid_page");
    }
  } else if (lastRevision !== page["revision"]) {
    throw new UiContractError("invalid_page");
  }
  const cursor = page["nextAfter"] === null ? page["revision"] : page["nextAfter"];
  return Object.freeze({
    runId: state["runId"],
    cursor,
    snapshotRevision: page["revision"],
    events: Object.freeze([...state["events"], ...events]),
    nextAfter: page["nextAfter"],
    stopped: false,
    error: null,
  });
}

/**
 * Stops the timeline fail-closed on one classified failure. The state,
 * cursor and rendered history stay untouched; only the automatic tail
 * observation stops until an explicit user action.
 */
export function stopTimeline(state, failure) {
  if (!isTimelineState(state)) {
    throw new UiContractError("invalid_state");
  }
  const layer = isRecord(failure) && typeof failure["layer"] === "string" && failure["layer"] !== "" ? failure["layer"] : null;
  const reason = isRecord(failure) && typeof failure["reason"] === "string" && failure["reason"] !== "" ? failure["reason"] : null;
  if (layer === null || reason === null) {
    throw new UiContractError("invalid_failure");
  }
  return Object.freeze({ ...state, stopped: true, error: Object.freeze({ layer, reason }) });
}

/**
 * `invalid_cursor` stops the timeline without a retry path — the cursor
 * contract itself is broken and only an explicit reload may recover;
 * every other failure keeps the explicit `Retry timeline` action.
 */
export function timelineRetryAvailable(state) {
  if (!isTimelineState(state) || state["stopped"] !== true || !isRecord(state["error"])) {
    return false;
  }
  return !(state["error"]["layer"] === "request" && state["error"]["reason"] === "invalid_cursor");
}

/**
 * The separated revision summary of the timeline: the last applied
 * authoritative inspect revision and the event snapshot revision may
 * briefly differ (a concurrent append makes the read-only events
 * snapshot newer than the last inspect); both are shown separately.
 */
export function timelineSummary(state, inspectRevision) {
  if (!isTimelineState(state)) {
    throw new UiContractError("invalid_state");
  }
  const loaded = state["snapshotRevision"] > 0 || state["events"].length > 0;
  return Object.freeze({
    detailRevision: typeof inspectRevision === "number" && Number.isSafeInteger(inspectRevision) && inspectRevision >= 0 ? inspectRevision : null,
    eventsRevision: state["snapshotRevision"],
    cursor: state["cursor"],
    caughtUp: loaded && state["nextAfter"] === null && state["stopped"] !== true,
    hasMore: loaded && state["stopped"] !== true && state["nextAfter"] !== null,
    stopped: state["stopped"] === true,
  });
}

/**
 * The aria-live policy of one applied page: the number of added
 * events, reaching the caught-up state and the timeline error are
 * announced; individual rows never are, and an unchanged page is
 * silent.
 */
export function timelineAnnouncement(previous, next) {
  if (!isTimelineState(previous) || !isTimelineState(next)) {
    throw new UiContractError("invalid_state");
  }
  if (next["stopped"] === true) {
    if (previous["stopped"] !== true) {
      const error = isRecord(next["error"]) ? next["error"] : null;
      const layer = error !== null && typeof error["layer"] === "string" && error["layer"] !== "" ? error["layer"] : "unknown";
      const reason = error !== null && typeof error["reason"] === "string" && error["reason"] !== "" ? error["reason"] : "unknown_failure";
      return `Lifecycle events error: ${layer}: ${reason}.`;
    }
    return null;
  }
  const added = next["events"].length - previous["events"].length;
  const wasLoaded = previous["snapshotRevision"] > 0 || previous["events"].length > 0;
  const isLoaded = next["snapshotRevision"] > 0 || next["events"].length > 0;
  const wasCaughtUp = wasLoaded && previous["nextAfter"] === null;
  const isCaughtUp = isLoaded && next["nextAfter"] === null;
  if (added > 0) {
    return isCaughtUp && !wasCaughtUp ? `Added ${added} lifecycle events. Caught up.` : `Added ${added} lifecycle events.`;
  }
  if (isCaughtUp && !wasCaughtUp) {
    return "Lifecycle events caught up.";
  }
  return null;
}

/**
 * The one single-flight admission owner of every events GET of the
 * module: at most one unsettled events request for the selected run.
 * A desire that arrives while a request is on the wire is refused
 * without a second fetch (no queue, no coalesced follow-up, no
 * retries); the automatic tail is re-derived from the next applied
 * inspect, and an explicit user action may be repeated. Selection
 * changes and `dispose()` invalidate the active result (its response
 * is never applied); the abort is best effort and never frees the wire
 * slot by itself — only the actual settlement does.
 */
export function createEventsTransport(io) {
  if (
    !isRecord(io) ||
    typeof io["fetchEvents"] !== "function" ||
    typeof io["onStarted"] !== "function" ||
    typeof io["onSettled"] !== "function"
  ) {
    throw new UiContractError("invalid_transport_io");
  }
  const fetchEvents = io["fetchEvents"];
  const onStarted = io["onStarted"];
  const onSettled = io["onSettled"];
  let active = null;
  let disposed = false;

  const startRequest = (request) => {
    if (active !== null) {
      throw new UiContractError("transport_slot_busy");
    }
    active = request;
    const controller = new AbortController();
    request.controller = controller;
    onStarted(request.runId);
    void (async () => {
      let result;
      try {
        result = await fetchEvents(request.runId, request.after, controller.signal);
      } catch {
        result = undefined;
      } finally {
        settleActive(request, result);
      }
    })();
  };

  const settleActive = (request, result) => {
    try {
      if (!request.superseded) {
        request.onApplied(result);
      }
    } finally {
      active = null;
      onSettled(request.runId);
      request.resolve(result);
    }
  };

  return Object.freeze({
    /**
     * Admits one events desire: started requests return
     * `{started: true, settled}`; refused desires resolve without any
     * fetch and without effects.
     */
    request(desire) {
      const runId = isRecord(desire) && isSafeId(desire["runId"]) ? desire["runId"] : null;
      const after = isRecord(desire) && typeof desire["after"] === "number" && Number.isSafeInteger(desire["after"]) && desire["after"] >= 0 ? desire["after"] : null;
      const onApplied = isRecord(desire) && typeof desire["onApplied"] === "function" ? desire["onApplied"] : null;
      let resolveResult = null;
      const settled = new Promise((resolve) => {
        resolveResult = resolve;
      });
      if (disposed || runId === null || after === null || onApplied === null || active !== null) {
        resolveResult(undefined);
        return Object.freeze({ started: false, settled });
      }
      const request = { runId, after, onApplied, superseded: false, controller: null, resolve: resolveResult, settledPromise: settled };
      startRequest(request);
      return Object.freeze({ started: true, settled });
    },
    /**
     * Supersedes the active result and aborts it best effort; the wire
     * slot is freed only by the actual settlement. Returns the settled
     * promise of the superseded request, or null when none was active.
     */
    invalidate() {
      if (active === null) {
        return null;
      }
      active.superseded = true;
      active.controller.abort();
      return active.settledPromise ?? null;
    },
    dispose() {
      disposed = true;
      if (active !== null) {
        active.superseded = true;
        active.controller.abort();
      }
    },
  });
}

/* ------------------------------------------------------------------ */
/* Lifecycle history (v9): the two stream transports, the merged state  */
/* and the derivation of the frozen view. Both streams are revision-    */
/* pinned to the applied inspect revision; a stale snapshot on either   */
/* stream clears the whole semantic view. No POST, no timers, no        */
/* browser storage.                                                     */
/* ------------------------------------------------------------------ */

/**
 * The one single-flight stream transport maker shared by both lifecycle
 * streams (the changes/logs governance: at most one unsettled request,
 * an initial desire supersedes the active one and coalesces as the
 * single follow-up, a continuation while busy is refused without a
 * second fetch, superseded responses are dropped, aborts are best
 * effort and never free the slot themselves).
 */
function createStreamTransport(fetchKey, io) {
  if (!isRecord(io) || typeof io[fetchKey] !== "function" || typeof io["onStarted"] !== "function" || typeof io["onSettled"] !== "function") {
    throw new UiContractError("invalid_transport_io");
  }
  const fetchPage = io[fetchKey];
  const onStarted = io["onStarted"];
  const onSettled = io["onSettled"];
  let active = null;
  let desired = null;
  let disposed = false;

  const startRequest = (request) => {
    if (active !== null) {
      throw new UiContractError("transport_slot_busy");
    }
    active = request;
    const controller = new AbortController();
    request.controller = controller;
    onStarted(request.runId);
    void (async () => {
      let result;
      try {
        result = await fetchPage(request.path, request.runId, controller.signal, request.binding);
      } catch {
        result = undefined;
      } finally {
        settleActive(request, result);
      }
    })();
  };

  const settleActive = (request, result) => {
    try {
      if (request.superseded) {
        if (desired === null) {
          onSettled(request.runId);
        }
      } else {
        request.onApplied(result);
        onSettled(request.runId);
      }
    } finally {
      active = null;
      const followUp = desired;
      desired = null;
      request.resolve(result);
      if (followUp !== null && !disposed) {
        startRequest(followUp);
      }
    }
  };

  return Object.freeze({
    request(desire) {
      const runId = isRecord(desire) && isSafeId(desire["runId"]) ? desire["runId"] : null;
      const path = isRecord(desire) && typeof desire["path"] === "string" ? desire["path"] : null;
      const kind = isRecord(desire) && (desire["kind"] === "initial" || desire["kind"] === "continuation") ? desire["kind"] : null;
      const onApplied = isRecord(desire) && typeof desire["onApplied"] === "function" ? desire["onApplied"] : null;
      let resolveResult = null;
      const settled = new Promise((resolve) => {
        resolveResult = resolve;
      });
      if (disposed || runId === null || path === null || kind === null || onApplied === null) {
        resolveResult(undefined);
        return settled;
      }
      const binding = isRecord(desire) && desire["binding"] !== undefined && (desire["binding"] === null || isRecord(desire["binding"])) ? desire["binding"] : undefined;
      if (isRecord(desire) && Object.hasOwn(desire, "binding") && binding === undefined) {
        resolveResult(undefined);
        return settled;
      }
      const request = { runId, path, kind, binding, onApplied, superseded: false, controller: null, resolve: resolveResult };
      if (active === null) {
        startRequest(request);
      } else if (kind === "continuation") {
        resolveResult(undefined);
        return settled;
      } else {
        active.superseded = true;
        active.controller.abort();
        const replaced = desired;
        desired = request;
        if (replaced !== null) {
          replaced.resolve(undefined);
        }
      }
      return settled;
    },
    invalidate() {
      if (active !== null) {
        active.superseded = true;
        active.controller.abort();
      }
      const withdrawn = desired;
      desired = null;
      if (withdrawn !== null) {
        withdrawn.resolve(undefined);
      }
    },
    dispose() {
      disposed = true;
      this.invalidate();
    },
  });
}

export function createIdentityTransport(io) {
  return createStreamTransport("fetchIdentity", io);
}

export function createExecutionBindingsTransport(io) {
  return createStreamTransport("fetchBindings", io);
}

/* The merged two-stream state (functional, frozen rebuilds). */

export function createLifecycleHistoryState(runId, pin) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  if (typeof pin !== "number" || !Number.isSafeInteger(pin) || pin < 1) {
    throw new UiContractError("invalid_shape");
  }
  const emptyStream = Object.freeze({
    records: Object.freeze([]),
    cursor: 0,
    nextAfter: null,
    loaded: false,
  });
  return Object.freeze({
    runId,
    pin,
    identity: emptyStream,
    bindings: emptyStream,
    error: null,
  });
}

function isLifecycleHistoryState(state) {
  return (
    isRecord(state) &&
    isSafeId(state["runId"]) &&
    typeof state["pin"] === "number" &&
    Number.isSafeInteger(state["pin"]) &&
    state["pin"] >= 1 &&
    isRecord(state["identity"]) &&
    isRecord(state["bindings"]) &&
    Array.isArray(state["identity"]["records"]) &&
    Array.isArray(state["bindings"]["records"])
  );
}

function acceptStreamPage(stream, page, requestedAfter) {
  if (typeof requestedAfter !== "number" || !Number.isSafeInteger(requestedAfter) || requestedAfter < 0) {
    throw new UiContractError("invalid_cursor");
  }
  if (requestedAfter !== stream["cursor"]) {
    throw new UiContractError("stale_page");
  }
  if (stream["loaded"] && requestedAfter === 0) {
    throw new UiContractError("stale_page");
  }
  const records = page["records"];
  const nextAfter = page["nextAfter"];
  return Object.freeze({
    records: Object.freeze([...stream["records"], ...records]),
    cursor: nextAfter === null ? requestedAfter + records.length : nextAfter,
    nextAfter,
    loaded: true,
  });
}

/**
 * The one merge of one validated identity page into the state. The page
 * must carry exactly the pinned revision (stale handling is global);
 * the requested cursor must equal the applied cursor.
 */
export function applyIdentityPage(state, page, requestedAfter) {
  if (!isLifecycleHistoryState(state) || !isRecord(page) || page["kind"] !== "identity") {
    throw new UiContractError("invalid_state");
  }
  if (page["revision"] !== state["pin"]) {
    throw new UiContractError("stale_page");
  }
  return Object.freeze({
    ...state,
    identity: acceptStreamPage(state["identity"], page, requestedAfter),
  });
}

/** The one merge of one validated execution-bindings page (mirrors the
 * identity merge; both streams of one state share the same pin). */
export function applyBindingsPage(state, page, requestedAfter) {
  if (!isLifecycleHistoryState(state) || !isRecord(page) || page["kind"] !== "execution_bindings") {
    throw new UiContractError("invalid_state");
  }
  if (page["revision"] !== state["pin"]) {
    throw new UiContractError("stale_page");
  }
  return Object.freeze({
    ...state,
    bindings: acceptStreamPage(state["bindings"], page, requestedAfter),
  });
}

/** A stream failure stops both streams with the typed failure (global
 * stale handling: either stream's failure clears the whole view). */
function applyStreamFailure(state, which, failure) {
  return Object.freeze({ ...state, error: Object.freeze({ which, ...failure }) });
}

function streamComplete(stream) {
  return stream["loaded"] === true && stream["nextAfter"] === null;
}

/** The execution view of an identity execution record with its cluster
 * members (decision id lists, agent outputs) and the placement from the
 * binding record (the only allowed binding). */
function executionView(execution, binding, cluster) {
  return Object.freeze({
    record: execution,
    binding: binding === undefined ? null : Object.freeze(binding),
    outputs: cluster.outputs,
    activeConstraints: cluster.activeConstraints,
    violatedRelations: cluster.violatedRelations,
  });
}

/** The raw history pass: groups the identity records into generation
 * cards, iteration rows, waits/actions, grants and per-execution
 * clusters; returns the typed structural error on an unresolvable
 * reference. */
function lifecycleHistoryRawView(state) {
  const raw = {
    generations: [],
    grantList: [],
    waits: [],
    actionByWait: new Map(),
    generationByIndex: new Map(),
    bindingByExecution: new Map(),
    clusterByExecution: new Map(),
    agentsByIndex: new Map(),
    decisionsByIndex: new Map(),
  };
  for (const record of state["identity"]["records"]) {
    const kind = record.record;
    if (kind === "generation") {
      const card = { generation: record, iterations: [], executions: [], };
      raw.generations.push(card);
      raw.generationByIndex.set(record.index, card);
    } else if (kind === "iteration") {
      const card = raw.generationByIndex.get(record.generationIndex);
      if (card === undefined) {
        return { error: { layer: "contract", reason: "orphan_iteration" } };
      }
      card.iterations.push(record);
    } else if (kind === "grant") {
      raw.grantList.push(record);
    } else if (kind === "wait") {
      raw.waits.push(record);
    } else if (kind === "wait_action") {
      const list = raw.actionByWait.get(record.waitIndex) ?? [];
      list.push(record);
      raw.actionByWait.set(record.waitIndex, list);
    } else if (kind === "execution_agent") {
      raw.agentsByIndex.set(record.index, record);
    } else if (kind === "execution_decision") {
      raw.decisionsByIndex.set(record.index, record);
    } else {
      const cluster =
        raw.clusterByExecution.get(record.executionIndex) ??
        { outputs: [], activeConstraints: [], violatedRelations: [] };
      if (kind === "agent_output") {
        cluster.outputs.push(record);
      } else if (kind === "decision_active_constraint") {
        cluster.activeConstraints.push(record);
      } else {
        cluster.violatedRelations.push(record);
      }
      raw.clusterByExecution.set(record.executionIndex, cluster);
    }
  }
  for (const record of state["bindings"]["records"]) {
    raw.bindingByExecution.set(record.executionIndex, record);
  }
  return raw;
}

function emptyCluster() {
  return { outputs: [], activeConstraints: [], violatedRelations: [] };
}

/**
 * The one derivation of the rendered view. `terminal` (true for a
 * succeeded or failed applied inspect) gates the historical
 * interventions group; the placed/unplaced relationship errors are
 * typed contract failures when both streams are complete, and
 * `not loaded yet` placeholders while any stream is partial.
 */
export function deriveLifecycleHistory(state, terminal) {
  if (!isLifecycleHistoryState(state)) {
    throw new UiContractError("invalid_state");
  }
  if (state["error"] !== null) {
    return Object.freeze({
      complete: false,
      generations: Object.freeze([]),
      unplacedExecutions: Object.freeze([]),
      historical: null,
      grants: Object.freeze([]),
      error: state["error"],
    });
  }
  const complete = streamComplete(state["identity"]) && streamComplete(state["bindings"]);
  const raw = lifecycleHistoryRawView(state);
  if (raw.error !== undefined) {
    return Object.freeze({
      complete: false,
      generations: Object.freeze([]),
      unplacedExecutions: Object.freeze([]),
      historical: null,
      grants: Object.freeze([]),
      error: Object.freeze(raw.error),
    });
  }
  const unplaced = [];
  const orphanBindings = [];
  for (const [index, execution] of raw.agentsByIndex) {
    placeExecution(execution, raw, index, unplaced, orphanBindings, complete);
  }
  for (const [index, decision] of raw.decisionsByIndex) {
    placeExecution(decision, raw, index, unplaced, orphanBindings, complete);
  }
  if (complete && orphanBindings.length > 0) {
    return Object.freeze({
      complete: false,
      generations: Object.freeze([]),
      unplacedExecutions: Object.freeze([]),
      historical: null,
      grants: Object.freeze([]),
      error: Object.freeze({ layer: "contract", reason: orphanBindings[0] }),
    });
  }
  const grantsByGeneration = new Map();
  for (const grant of raw.grantList) {
    const list = grantsByGeneration.get(grant.generationIndex) ?? [];
    list.push(grant);
    grantsByGeneration.set(grant.generationIndex, list);
  }
  const generations = Object.freeze(
    raw.generations.map((card) =>
      Object.freeze({
        generation: card.generation,
        iterations: Object.freeze([...card.iterations]),
        executions: Object.freeze([...card.executions]),
        grants: Object.freeze(grantsByGeneration.get(card.generation.index) ?? []),
      }),
    ),
  );
  let historical = null;
  if (terminal === true) {
    const actions = [];
    for (const [waitIndex, list] of raw.actionByWait) {
      for (const action of list) {
        actions.push(Object.freeze({ waitIndex, action }));
      }
    }
    historical = Object.freeze({
      waits: Object.freeze([...raw.waits]),
      actions: Object.freeze(actions),
      grants: Object.freeze([...raw.grantList]),
    });
  }
  return Object.freeze({
    complete,
    generations,
    unplacedExecutions: Object.freeze(unplaced),
    historical,
    grants: Object.freeze([...raw.grantList]),
    error: null,
  });
}

function placeExecution(execution, raw, index, unplaced, orphanBindings, complete) {
  const cluster = raw.clusterByExecution.get(index) ?? emptyCluster();
  const binding = raw.bindingByExecution.get(index);
  if (binding === undefined) {
    if (complete) {
      orphanBindings.push("missing_binding");
      return;
    }
    unplaced.push(executionView(execution, undefined, cluster));
    return;
  }
  if (binding.generationIndex === null) {
    unplaced.push(executionView(execution, binding, cluster));
    return;
  }
  const card = raw.generationByIndex.get(binding.generationIndex);
  if (card === undefined) {
    if (complete) {
      orphanBindings.push("unresolved_generation");
      unplaced.push(executionView(execution, binding, cluster));
      return;
    }
    unplaced.push(executionView(execution, binding, cluster));
    return;
  }
  if (card.executions === undefined) {
    card.executions = [];
  }
  card.executions.push(executionView(execution, binding, cluster));
}

/**
 * The closed state machine of the Lifecycle history section. It owns no
 * fetch and no URL: every desire goes to one of the two single-flight
 * transports. Both streams are pinned to the revision of the applied
 * authoritative inspect; a revision change and a run change reset both
 * streams and the compare selection; a stale snapshot on either stream
 * clears the whole semantic view and offers only the explicit reload.
 * The viewer never POSTs, never starts a timer and never touches
 * browser storage.
 */
export function createLifecycleHistoryViewer(io) {
  if (
    !isRecord(io) ||
    !isRecord(io["identityTransport"]) ||
    !isRecord(io["bindingsTransport"]) ||
    typeof io["onState"] !== "function"
  ) {
    throw new UiContractError("invalid_transport_io");
  }
  const identityTransport = io["identityTransport"];
  const bindingsTransport = io["bindingsTransport"];
  const onState = io["onState"];
  let state = null;
  let disposed = false;

  const publish = () => {
    onState(state);
  };

  const requestIdentity = (runId, after, kind, pin) => {
    identityTransport.request({
      runId,
      kind,
      path: identityRequestPath(runId, after, pin),
      onApplied: (result) => {
        applyIdentityOutcome(result, after);
      },
    });
  };

  const requestBindings = (runId, after, kind, pin) => {
    bindingsTransport.request({
      runId,
      kind,
      path: executionBindingsRequestPath(runId, after, pin),
      onApplied: (result) => {
        applyBindingsOutcome(result, after);
      },
    });
  };

  const streamOutcomeFailure = (which, stateNow, result) => {
    if (result === undefined) {
      return applyStreamFailure(stateNow, which, { layer: "transport", reason: "network_error" });
    }
    if (result.ok === false) {
      return applyStreamFailure(stateNow, which, result.failure);
    }
    const model = result.model;
    if (model.kind === "error") {
      if (model.reason === "stale_snapshot") {
        return { stale: true };
      }
      return applyStreamFailure(stateNow, which, { layer: model.layer, reason: model.reason });
    }
    if (model.kind === "command_result") {
      return applyStreamFailure(
        stateNow,
        which,
        model.runId === stateNow["runId"]
          ? { layer: "orchestrator", reason: model.reason ?? "unknown_failure" }
          : { layer: "contract", reason: "unexpected_run" },
      );
    }
    return null;
  };

  const applyIdentityOutcome = (result, requestedAfter) => {
    if (disposed || state === null) {
      return;
    }
    const outcome = streamOutcomeFailure("identity", state, result);
    if (outcome !== null) {
      if (outcome.stale === true) {
        restartStreams(state);
      } else {
        state = outcome;
      }
      publish();
      return;
    }
    try {
      state = applyIdentityPage(state, result.model, requestedAfter);
    } catch (error) {
      if (error instanceof UiContractError) {
        state = applyStreamFailure(state, "identity", { layer: "contract", reason: error.reason });
        publish();
        return;
      }
      throw error;
    }
    publish();
  };

  const applyBindingsOutcome = (result, requestedAfter) => {
    if (disposed || state === null) {
      return;
    }
    const outcome = streamOutcomeFailure("bindings", state, result);
    if (outcome !== null) {
      if (outcome.stale === true) {
        restartStreams(state);
      } else {
        state = outcome;
      }
      publish();
      return;
    }
    try {
      state = applyBindingsPage(state, result.model, requestedAfter);
    } catch (error) {
      if (error instanceof UiContractError) {
        state = applyStreamFailure(state, "bindings", { layer: "contract", reason: error.reason });
        publish();
        return;
      }
      throw error;
    }
    publish();
  };

  /** The restart of both streams from their existing pin (the stale
   * snapshot path never invents a revision: only an applied inspect
   * establishes a new pin). */
  const restartStreams = (previous) => {
    state = createLifecycleHistoryState(previous["runId"], previous["pin"]);
    // request again without invalidating: an initial desire superseded
    // over an in-flight page still converges (the superseded response
    // is dropped), so the restart never depends on invalidate timing
    startStreams(previous["runId"], previous["pin"]);
  };

  const startStreams = (runId, pin) => {
    requestIdentity(runId, 0, "initial", pin);
    requestBindings(runId, 0, "initial", pin);
  };

  return Object.freeze({
    /**
     * The inspect-driven hook: the first inspect of a run and every
     * changed revision reset both streams to the new pin and issue the
     * two initial GETs; an unchanged pin issues nothing (the streams
     * continue their own cursors). A stream error short-circuits until
     * an explicit reload or a revision change.
     */
    inspectApplied(model) {
      if (disposed || !isRecord(model) || model["kind"] !== "run") {
        return;
      }
      const runId = model["runId"];
      const revision = model["revision"];
      if (state === null || state["runId"] !== runId) {
        identityTransport.invalidate();
        bindingsTransport.invalidate();
        state = createLifecycleHistoryState(runId, revision);
        startStreams(runId, revision);
        publish();
        return;
      }
      if (state["pin"] !== revision) {
        identityTransport.invalidate();
        bindingsTransport.invalidate();
        state = createLifecycleHistoryState(runId, revision);
        startStreams(runId, revision);
        publish();
        return;
      }
      if (state["error"] !== null) {
        return;
      }
      // a first page transport-refused at admission time is retried by
      // the following applied inspect of the same revision
      if (!state["identity"]["loaded"]) {
        startStreams(runId, state["pin"]);
        publish();
      }
    },
    loadMoreIdentity() {
      if (disposed || state === null || state["error"] !== null) {
        return;
      }
      const stream = state["identity"];
      if (!stream["loaded"] || stream["nextAfter"] === null) {
        return;
      }
      requestIdentity(state["runId"], stream["nextAfter"], "continuation", state["pin"]);
    },
    loadMoreBindings() {
      if (disposed || state === null || state["error"] !== null) {
        return;
      }
      const stream = state["bindings"];
      if (!stream["loaded"] || stream["nextAfter"] === null) {
        return;
      }
      requestBindings(state["runId"], stream["nextAfter"], "continuation", state["pin"]);
    },
    reload() {
      if (disposed || state === null) {
        return;
      }
      restartStreams(state);
      startStreams(state["runId"], state["pin"]);
      publish();
    },
    reset() {
      identityTransport.invalidate();
      bindingsTransport.invalidate();
      state = null;
      publish();
    },
    currentState() {
      return state;
    },
    dispose() {
      disposed = true;
      identityTransport.invalidate();
      bindingsTransport.invalidate();
      state = null;
      publish();
    },
  });
}

/* ------------------------------------------------------------------ */
/* ------------------------------------------------------------------ */
/* Plan and tasks (S6): the second consumer of the P-3 identity         */
/* ledger — its own pinned stream instance — plus the two bounded P-1   */
/* content surfaces (the byte-exact task-body chunk stream and the      */
/* paged semantic plan projection). Read-only: no POST, no timers, no   */
/* browser storage, no cross-domain digest claims. The cursor           */
/* invariants and the fail-closed body publication policy implement the */
/* accepted S6 preflight contract (increment 6 preflight r4).          */
/* ------------------------------------------------------------------ */

const S6_TASK_CHUNK_LIMIT_DEFAULT = 65536;
const S6_TASK_CHUNK_LIMIT_MAX = 193536;
const S6_PLAN_PAGE_LIMIT_DEFAULT = 100;
const S6_PLAN_PAGE_LIMIT_MAX = 200;
export const S6_BODY_RETAINED_BYTES_CAP = 262144;
export const S6_PLAN_RECORDS_RETAINED_CAP = 4096;
const S6_TASK_ORIGIN_CLASSES = ["planning_proposal", "user_response"];
const S6_TASK_BODY_DIGEST_DOMAIN = "pipeline-v2-stage-task-body\0";

/** The exact shape of one content request binding (what the viewer
 * issued); a hostile caller object is refused before any other read. */
function s6TaskExpectation(expected) {
  if (!isRecord(expected) || !isSafeId(expected["runId"]) || !isSafeId(expected["taskId"])) {
    throw new UiContractError("invalid_request");
  }
  if (!isPositiveSafeIndex(expected["revision"])) {
    throw new UiContractError("invalid_request");
  }
  return expected;
}

function s6PlanExpectation(expected) {
  if (!isRecord(expected) || !isSafeId(expected["runId"]) || !isPositiveSafeIndex(expected["revision"])) {
    throw new UiContractError("invalid_request");
  }
  return expected;
}

/**
 * The dedicated parser of one task-content chunk document (schema 1,
 * kind `task_revision`): exact own keys at every level, the closed
 * origin vocabulary, a strict canonical base64 payload with
 * `data.byteLength === returned_byte_count`, and the full cursor
 * matrix: the returned bytes never exceed the page limit;
 * `end = offset + returned_byte_count` is a safe integer at or below
 * `byte_count`; zero progress is impossible (a chunk with
 * `end < byte_count` carries a positive `returned_byte_count` and a
 * strictly moving `next_offset`; a zero-length chunk exists only as
 * the terminal chunk of a complete body); `next_offset` is `null`
 * exactly when `end === byte_count` and otherwise exactly `end`. The
 * bytes live only in the frozen `data` byte array — never in a
 * diagnostic or a URL.
 */
export function parseTaskContentDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "task_revision") {
    throw new UiContractError("unknown_kind");
  }
  expectShape(document, ["schema_version", "kind", "run_id", "task_id", "revision", "manifest_sha256", "previous_sha256", "origin", "body"]);
  const binding = expected === null ? null : s6TaskExpectation(expected);
  if (binding !== null) {
    if (!isSafeId(expectString(document, "run_id")) || document["run_id"] !== binding["runId"]) {
      throw new UiContractError("unexpected_run");
    }
  } else if (!isSafeId(expectString(document, "run_id"))) {
    throw new UiContractError("invalid_shape");
  }
  const runId = document["run_id"];
  const taskId = expectSafeIdString(document, "task_id");
  const revision = expectPositiveInteger(document, "revision");
  const manifestSha256 = expectDigest(document["manifest_sha256"]);
  const previousSha256 = expectDigestOrNull(document["previous_sha256"]);
  const origin = expectOneOf(document, "origin", S6_TASK_ORIGIN_CLASSES);
  if (!isRecord(document["body"])) {
    throw new UiContractError("invalid_shape");
  }
  const body = document["body"];
  expectShape(body, ["byte_count", "sha256", "offset", "limit", "returned_byte_count", "next_offset", "data_base64"]);
  const byteCount = expectNonNegativeInteger(body, "byte_count");
  const bodySha256 = expectDigest(body["sha256"]);
  const offset = expectNonNegativeInteger(body, "offset");
  const limit = expectPositiveInteger(body, "limit");
  const returnedByteCount = expectNonNegativeInteger(body, "returned_byte_count");
  const nextOffsetValue = body["next_offset"];
  if (nextOffsetValue !== null && !isPositiveSafeIndex(nextOffsetValue)) {
    throw new UiContractError("invalid_shape");
  }
  if (typeof body["data_base64"] !== "string") {
    throw new UiContractError("invalid_shape");
  }
  const data = expectBase64Bytes(body["data_base64"]);
  if (data.byteLength !== returnedByteCount) {
    throw new UiContractError("invalid_shape");
  }
  if (returnedByteCount > limit) {
    throw new UiContractError("invalid_shape");
  }
  const end = offset + returnedByteCount;
  if (!Number.isSafeInteger(end) || end > byteCount) {
    throw new UiContractError("invalid_shape");
  }
  if (returnedByteCount === 0 && (end !== byteCount || nextOffsetValue !== null)) {
    throw new UiContractError("invalid_shape");
  }
  if (end < byteCount && (nextOffsetValue === null || nextOffsetValue !== end || nextOffsetValue <= offset)) {
    throw new UiContractError("invalid_shape");
  }
  if (nextOffsetValue !== null && nextOffsetValue !== end) {
    throw new UiContractError("invalid_shape");
  }
  if (nextOffsetValue !== null && end === byteCount) {
    throw new UiContractError("invalid_shape");
  }
  if (binding !== null && (taskId !== binding["taskId"] || revision !== binding["revision"])) {
    throw new UiContractError("unexpected_selection");
  }
  return Object.freeze({
    kind: "task_revision",
    runId,
    taskId,
    revision,
    manifestSha256,
    previousSha256,
    origin,
    body: Object.freeze({
      byteCount,
      bodySha256,
      offset,
      limit,
      returnedByteCount,
      nextOffset: nextOffsetValue,
      data,
    }),
  });
}

/** One projected plan record; exact own keys per kind, positions
 * positive-safe, ids safe, digests lowercase 64-hex. */
function s6ParsePlanRecord(entry) {
  if (!isRecord(entry)) {
    throw new UiContractError("invalid_shape");
  }
  const kind = entry["record"];
  if (kind === "stage") {
    expectShape(entry, ["record", "stage_position", "id", "template"]);
    return Object.freeze({
      record: kind,
      stagePosition: expectPositiveInteger(entry, "stage_position"),
      id: expectSafeIdString(entry, "id"),
      template: expectSafeIdString(entry, "template"),
    });
  }
  if (kind === "task") {
    expectShape(entry, ["record", "stage_position", "task_position", "id", "revision", "sha256"]);
    return Object.freeze({
      record: kind,
      stagePosition: expectPositiveInteger(entry, "stage_position"),
      taskPosition: expectPositiveInteger(entry, "task_position"),
      id: expectSafeIdString(entry, "id"),
      revision: expectPositiveInteger(entry, "revision"),
      sha256: expectDigest(entry["sha256"]),
    });
  }
  if (kind === "task_dependency") {
    expectShape(entry, ["record", "stage_position", "task_position", "dependency_position", "task_id"]);
    return Object.freeze({
      record: kind,
      stagePosition: expectPositiveInteger(entry, "stage_position"),
      taskPosition: expectPositiveInteger(entry, "task_position"),
      dependencyPosition: expectPositiveInteger(entry, "dependency_position"),
      taskId: expectSafeIdString(entry, "task_id"),
    });
  }
  throw new UiContractError("invalid_shape");
}

/**
 * The dedicated parser of one plan-content page (schema 1, kind
 * `plan_revision`): exact own keys, the closed 3-record-kind
 * vocabulary, and the dense cursors with the identical no-progress
 * rule (`next_after !== null` requires `records.length > 0` and equals
 * `after + records.length`; an empty page is valid only with
 * `next_after === null`).
 */
export function parsePlanContentDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] !== "plan_revision") {
    throw new UiContractError("unknown_kind");
  }
  expectShape(document, ["schema_version", "kind", "run_id", "revision", "manifest_sha256", "previous_sha256", "origin_execution", "after", "records", "next_after"]);
  const binding = expected === null ? null : s6PlanExpectation(expected);
  if (binding !== null) {
    if (!isSafeId(expectString(document, "run_id")) || document["run_id"] !== binding["runId"]) {
      throw new UiContractError("unexpected_run");
    }
  } else if (!isSafeId(expectString(document, "run_id"))) {
    throw new UiContractError("invalid_shape");
  }
  const runId = document["run_id"];
  const revision = expectPositiveInteger(document, "revision");
  const manifestSha256 = expectDigest(document["manifest_sha256"]);
  const previousSha256 = expectDigestOrNull(document["previous_sha256"]);
  const originExecution = expectPositiveInteger(document, "origin_execution");
  const after = expectNonNegativeInteger(document, "after");
  const records = expectArray(document, "records").map(s6ParsePlanRecord);
  const nextAfter = document["next_after"];
  if (nextAfter !== null) {
    if (records.length === 0 || !isPositiveSafeIndex(nextAfter) || nextAfter !== after + records.length) {
      throw new UiContractError("invalid_shape");
    }
  }
  if (binding !== null && revision !== binding["revision"]) {
    throw new UiContractError("unexpected_selection");
  }
  return Object.freeze({
    kind: "plan_revision",
    runId,
    revision,
    manifestSha256,
    previousSha256,
    originExecution,
    after,
    records: Object.freeze(records),
    nextAfter,
  });
}

/** The task-content endpoint classifier: one well-formed chunk of the
 * requested selection, or one of the endpoint's documented failure
 * envelopes. */
export function parseTaskContentEndpointDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "task_revision") {
    return parseTaskContentDocument(document, expected);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/** The plan-content endpoint classifier (mirrors the task one). */
export function parsePlanContentEndpointDocument(document, expected) {
  if (!isRecord(document)) {
    throw new UiContractError("not_a_document");
  }
  if (document["schema_version"] !== IDENTITY_SCHEMA_VERSION) {
    throw new UiContractError("unknown_schema_version");
  }
  if (document["kind"] === "plan_revision") {
    return parsePlanContentDocument(document, expected);
  }
  if (document["kind"] === "error") {
    return parseErrorDocument(document);
  }
  if (document["kind"] === "command_result") {
    const model = parseCommandResultDocument(document);
    if (model.outcome !== "failed") {
      throw new UiContractError("unexpected_kind");
    }
    return model;
  }
  throw new UiContractError("unknown_kind");
}

/** The one URL builder of every task-content request. */
export function taskContentRequestPath(runId, taskId, revision, offset, limit) {
  if (!isSafeId(runId) || !isSafeId(taskId) || !isPositiveSafeIndex(revision)) {
    throw new UiContractError("invalid_request");
  }
  if (!isSafeIndex(offset) || !isPositiveSafeIndex(limit) || limit > S6_TASK_CHUNK_LIMIT_MAX) {
    throw new UiContractError("invalid_cursor");
  }
  return `/v1/runs/${runId}/tasks/${taskId}/revisions/${revision}?offset=${offset}&limit=${limit}`;
}

/** The one URL builder of every plan-content request. */
export function planContentRequestPath(runId, revision, after, limit) {
  if (!isSafeId(runId) || !isPositiveSafeIndex(revision)) {
    throw new UiContractError("invalid_request");
  }
  if (!isSafeIndex(after) || !isPositiveSafeIndex(limit) || limit > S6_PLAN_PAGE_LIMIT_MAX) {
    throw new UiContractError("invalid_cursor");
  }
  return `/v1/runs/${runId}/plans/${revision}?after=${after}&limit=${limit}`;
}

/** The task-content client: the expected response type stays bound to
 * the requested selection. */
export async function requestTaskContentJson(fetcher, path, init, expected) {
  return requestJsonDocument(fetcher, path, init, (document) => parseTaskContentEndpointDocument(document, expected));
}

/** The plan-content client (mirrors the task one). */
export async function requestPlanContentJson(fetcher, path, init, expected) {
  return requestJsonDocument(fetcher, path, init, (document) => parsePlanContentEndpointDocument(document, expected));
}

/* The two content transports over the shared stream factory; the
 * optional fourth argument carries the request binding to the client. */

export function createTaskContentTransport(io) {
  return createStreamTransport("fetchTaskContent", io);
}

export function createPlanContentTransport(io) {
  return createStreamTransport("fetchPlanContent", io);
}

/** Byte helpers of the body verification: the exact framing of the
 * single production owner (`stageTaskBodyDigest`), reproduced
 * byte-for-byte in the browser. */
function s6ConcatBytes(parts) {
  let total = 0;
  for (const part of parts) {
    if (!(part instanceof Uint8Array)) {
      throw new UiContractError("invalid_shape");
    }
    total += part.byteLength;
  }
  if (!Number.isSafeInteger(total)) {
    throw new UiContractError("invalid_shape");
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const part of parts) {
    out.set(part, at);
    at += part.byteLength;
  }
  return out;
}

function s6LatinBytes(text) {
  const bytes = new Uint8Array(text.length);
  for (let index = 0; index < text.length; index += 1) {
    const code = text.charCodeAt(index);
    if (code > 255) {
      throw new UiContractError("invalid_shape");
    }
    bytes[index] = code;
  }
  return bytes;
}

/**
 * One browser-side recomputation of the stage-task body digest over the
 * exact reassembled bytes: resolves the recomputed lowercase hex digest
 * on a match and `null` on a mismatch (the none/some result). The
 * caller owns the publication policy and the epoch/token handling.
 */
export async function verifyTaskBodyDigest(bytes, expectedDigest) {
  if (!(bytes instanceof Uint8Array) || typeof expectedDigest !== "string" || !/^[0-9a-f]{64}$/.test(expectedDigest)) {
    throw new UiContractError("invalid_shape");
  }
  const lengthBytes = new Uint8Array(8);
  let value = bytes.byteLength;
  for (let index = 7; index >= 0; index -= 1) {
    lengthBytes[index] = value % 256;
    value = Math.floor(value / 256);
  }
  const framed = s6ConcatBytes([
    s6LatinBytes(S6_TASK_BODY_DIGEST_DOMAIN),
    lengthBytes,
    bytes,
  ]);
  const subtle = typeof crypto === "undefined" ? undefined : crypto.subtle;
  if (subtle === undefined || typeof subtle.digest !== "function") {
    throw new Error("unavailable: webcrypto digest");
  }
  const digest = await subtle.digest("SHA-256", framed);
  const hex = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
  return hex === expectedDigest ? hex : null;
}

/* ------------------------------------------------------------------ */
/* The merged S6 state: the pinned identity-ledger stream (the same     */
/* record kinds the section reads) plus the two content targets.       */
/* ------------------------------------------------------------------ */

export function createPlanTasksState(runId, pin) {
  if (!isSafeId(runId)) {
    throw new UiContractError("invalid_run_id");
  }
  if (!isPositiveSafeIndex(pin)) {
    throw new UiContractError("invalid_shape");
  }
  return Object.freeze({
    runId,
    pin,
    identity: Object.freeze({ records: Object.freeze([]), cursor: 0, nextAfter: null, loaded: false }),
    error: null,
    staleCleared: false,
    taskTarget: null,
    planTarget: null,
  });
}

function isPlanTasksState(state) {
  return (
    isRecord(state) &&
    isSafeId(state["runId"]) &&
    isPositiveSafeIndex(state["pin"]) &&
    isRecord(state["identity"]) &&
    Array.isArray(state["identity"]["records"]) &&
    (state["taskTarget"] === null || isRecord(state["taskTarget"])) &&
    (state["planTarget"] === null || isRecord(state["planTarget"]))
  );
}

/** The one merge of one validated ledger page; the requested `after`
 * must equal the stream cursor and the revision must equal the pin
 * (the identical continuation rules of the first stream). */
export function applyPlanTasksIdentityPage(state, page, requestedAfter) {
  if (!isPlanTasksState(state) || !isRecord(page) || page["kind"] !== "identity") {
    throw new UiContractError("invalid_state");
  }
  if (page["revision"] !== state["pin"]) {
    throw new UiContractError("stale_page");
  }
  if (requestedAfter !== state["identity"]["cursor"]) {
    throw new UiContractError("stale_page");
  }
  return Object.freeze({
    ...state,
    identity: Object.freeze({
      records: Object.freeze([...state["identity"]["records"], ...page["records"]]),
      cursor: page["nextAfter"] === null ? state["identity"]["cursor"] + page["records"].length : page["nextAfter"],
      nextAfter: page["nextAfter"],
      loaded: true,
    }),
  });
}

function s6WithTaskTarget(state, taskTarget) {
  return Object.freeze({ ...state, taskTarget });
}

function s6WithPlanTarget(state, planTarget) {
  return Object.freeze({ ...state, planTarget });
}

function s6NewTaskTarget(taskId, revision, ledgerSha256, ledgerPreviousSha256, epoch, initialLimit) {
  return {
    kind: "task",
    key: `task:${taskId}:${revision}`,
    taskId,
    revision,
    epoch,
    ledgerSha256,
    ledgerPreviousSha256,
    status: "requesting",
    envelope: null,
    cursor: 0,
    inFlightLimit: initialLimit,
    truncatedPending: initialLimit >= S6_BODY_RETAINED_BYTES_CAP,
    chunkCount: 0,
    retained: Object.freeze([]),
    retainedBytes: 0,
    decodedText: "",
    decoder: null,
    terminal: false,
    truncated: false,
    integrity: "pending",
    verifiedDigest: null,
    failure: null,
  };
}

function s6NewPlanTarget(revision, ledgerSha256, ledgerPreviousSha256, ledgerOriginExecution, epoch, initialLimit) {
  return {
    kind: "plan",
    key: `plan:${revision}`,
    revision,
    epoch,
    ledgerSha256,
    ledgerPreviousSha256,
    ledgerOriginExecution,
    status: "requesting",
    envelope: null,
    cursor: 0,
    inFlightLimit: initialLimit,
    records: Object.freeze([]),
    terminal: false,
    truncated: false,
    failure: null,
  };
}

function s6TaskFailure(target, layer, reason) {
  return Object.freeze({
    ...target,
    status: "failed",
    failure: Object.freeze({ layer, reason }),
    decoder: null,
    decodedText: "",
    retained: Object.freeze([]),
  });
}

function s6PlanFailure(target, layer, reason) {
  return Object.freeze({
    ...target,
    status: "failed",
    failure: Object.freeze({ layer, reason }),
  });
}

/**
 * The one merge of one accepted task chunk. The request binding (the
 * triple cursor equality, the document offsets/limit vs the requested
 * pair), the ledger binding of the first chunk, the envelope
 * immutability of every later chunk and the caps run BEFORE any decode
 * or merge; every violation is a typed contract failure (never a
 * TypeError). The streaming decoder is created on the first chunk and
 * survives the merges; the terminal chunk flushes it.
 */
export function applyTaskChunk(state, model, requestedOffset, requestedLimit, requestReaderId) {
  if (!isPlanTasksState(state) || state["taskTarget"] === null || !isRecord(model) || model["kind"] !== "task_revision") {
    throw new UiContractError("invalid_state");
  }
  const target = state["taskTarget"];
  if (requestedOffset !== target["cursor"]) {
    throw new UiContractError("stale_request");
  }
  if (requestReaderId !== target["readerId"]) {
    throw new UiContractError("stale_reader");
  }
  if (
    typeof requestedOffset !== "number" ||
    !Number.isSafeInteger(requestedOffset) ||
    requestedOffset < 0 ||
    typeof requestedLimit !== "number" ||
    !Number.isSafeInteger(requestedLimit) ||
    requestedLimit < 1 ||
    requestedLimit > S6_TASK_CHUNK_LIMIT_MAX
  ) {
    throw new UiContractError("invalid_cursor");
  }
  const body = model["body"];
  if (body["offset"] !== requestedOffset || body["limit"] !== requestedLimit) {
    throw new UiContractError("request_mismatch");
  }
  if (target["envelope"] === null) {
    if (model["manifestSha256"] !== target["ledgerSha256"] || model["previousSha256"] !== target["ledgerPreviousSha256"]) {
      throw new UiContractError("artifact_modified");
    }
  } else if (
    target["envelope"]["manifestSha256"] !== model["manifestSha256"] ||
    target["envelope"]["previousSha256"] !== model["previousSha256"] ||
    target["envelope"]["origin"] !== model["origin"] ||
    target["envelope"]["byteCount"] !== body["byteCount"] ||
    target["envelope"]["bodySha256"] !== body["bodySha256"]
  ) {
    throw new UiContractError("envelope_drift");
  }
  if (target["retainedBytes"] + body["returnedByteCount"] > S6_BODY_RETAINED_BYTES_CAP) {
    throw new UiContractError("retention_cap");
  }
  const chunk = body["data"];
  const terminal = body["nextOffset"] === null;
  if (!terminal && target["truncatedPending"]) {
    throw new UiContractError("retention_cap");
  }
  let decoder = target["decoder"];
  if (decoder === null) {
    try {
      decoder = new TextDecoder("utf-8", { fatal: true });
    } catch {
      throw new UiContractError("unsupported_environment");
    }
  }
  let decodedText = target["decodedText"];
  try {
    decodedText += decoder.decode(chunk, { stream: !terminal });
  } catch {
    throw new UiContractError("decode_failed");
  }
  const retained = Object.freeze([...target["retained"], chunk]);
  const retainedBytes = target["retainedBytes"] + body["returnedByteCount"];
  const envelope =
    target["envelope"] === null
      ? Object.freeze({
          manifestSha256: model["manifestSha256"],
          previousSha256: model["previousSha256"],
          origin: model["origin"],
          byteCount: body["byteCount"],
          bodySha256: body["bodySha256"],
        })
      : target["envelope"];
  const nextTarget = Object.freeze({
    ...target,
    status: terminal ? (target["truncatedPending"] ? "truncated" : "verifying") : "streaming",
    envelope,
    cursor: terminal ? target["cursor"] + body["returnedByteCount"] : body["nextOffset"],
    readerId: target["readerId"] + 1,
    chunkCount: target["chunkCount"] + 1,
    retained,
    retainedBytes,
    decodedText,
    decoder,
    terminal,
    truncated: target["truncatedPending"] && terminal,
  });
  return s6WithTaskTarget(state, nextTarget);
}

/**
 * The one merge of one accepted plan page (the identical guard order:
 * the request binding, the uncapped retention bound, the ledger
 * binding on the first page, the envelope immutability afterwards).
 */
export function applyPlanPage(state, model, requestedAfter, requestedLimit, requestReaderId) {
  if (!isPlanTasksState(state) || state["planTarget"] === null || !isRecord(model) || model["kind"] !== "plan_revision") {
    throw new UiContractError("invalid_state");
  }
  const target = state["planTarget"];
  if (requestedAfter !== target["cursor"]) {
    throw new UiContractError("stale_request");
  }
  if (requestReaderId !== target["readerId"]) {
    throw new UiContractError("stale_reader");
  }
  if (typeof requestedAfter !== "number" || !Number.isSafeInteger(requestedAfter) || requestedAfter < 0) {
    throw new UiContractError("invalid_cursor");
  }
  if (
    typeof requestedLimit !== "number" ||
    !Number.isSafeInteger(requestedLimit) ||
    requestedLimit < 1 ||
    requestedLimit > S6_PLAN_PAGE_LIMIT_MAX
  ) {
    throw new UiContractError("invalid_cursor");
  }
  if (model["after"] !== requestedAfter || model["records"].length > requestedLimit) {
    throw new UiContractError("request_mismatch");
  }
  if (target["records"].length + model["records"].length > S6_PLAN_RECORDS_RETAINED_CAP) {
    throw new UiContractError("retention_cap");
  }
  if (target["envelope"] === null) {
    if (
      model["manifestSha256"] !== target["ledgerSha256"] ||
      model["previousSha256"] !== target["ledgerPreviousSha256"] ||
      model["originExecution"] !== target["ledgerOriginExecution"]
    ) {
      throw new UiContractError("artifact_modified");
    }
  } else if (
    target["envelope"]["manifestSha256"] !== model["manifestSha256"] ||
    target["envelope"]["previousSha256"] !== model["previousSha256"] ||
    target["envelope"]["originExecution"] !== model["originExecution"]
  ) {
    throw new UiContractError("envelope_drift");
  }
  const terminal = model["nextAfter"] === null;
  const retained = target["records"].length + model["records"].length;
  const envelope =
    target["envelope"] === null
      ? Object.freeze({
          manifestSha256: model["manifestSha256"],
          previousSha256: model["previousSha256"],
          originExecution: model["originExecution"],
        })
      : target["envelope"];
  const nextTarget = Object.freeze({
    ...target,
    status: terminal ? "ready" : retained >= S6_PLAN_RECORDS_RETAINED_CAP ? "truncated" : "streaming",
    envelope,
    cursor: terminal ? target["cursor"] + model["records"].length : model["nextAfter"],
    readerId: target["readerId"] + 1,
    records: Object.freeze([...target["records"], ...model["records"]]),
    terminal,
    truncated: !terminal && retained >= S6_PLAN_RECORDS_RETAINED_CAP,
  });
  return s6WithPlanTarget(state, nextTarget);
}

function s6TaskIntegrityOf(target) {
  if (target["failure"] !== null) {
    return "failed";
  }
  if (target["truncated"]) {
    return "unverified_truncated";
  }
  return target["integrity"];
}

/**
 * One derivation of the rendered view; the cross-checks attach claims
 * of complete inputs only (the durable task evidence requires the
 * complete identity stream plus a verified integrity, and the plan
 * pointer check requires the complete plan target over the complete
 * ledger); every partial state renders its honest pending form. Pure:
 * recomputed from the state records on every call.
 */
export function derivePlanTasksView(state) {
  if (!isPlanTasksState(state)) {
    throw new UiContractError("invalid_state");
  }
  const identity = state["identity"];
  const complete = identity["loaded"] === true && identity["nextAfter"] === null;
  const ledgerByTaskPair = new Map();
  const taskLedger = [];
  const planLedger = [];
  for (const record of identity["records"]) {
    if (record["record"] === "task_revision") {
      taskLedger.push(record);
      ledgerByTaskPair.set(`${record["taskId"]}@${record["revision"]}`, record);
    } else if (record["record"] === "plan_revision") {
      planLedger.push(record);
    }
  }
  let taskTarget = null;
  if (state["taskTarget"] !== null) {
    const target = state["taskTarget"];
    const matching = [];
    if (complete) {
      for (const record of identity["records"]) {
        const stageTask = record["record"] === "execution_agent" ? record["stageTask"] : null;
        if (stageTask !== null && stageTask !== undefined && stageTask["taskId"] === target["taskId"] && stageTask["taskRevision"] === target["revision"]) {
          matching.push(stageTask);
        }
      }
    }
    const integrity = s6TaskIntegrityOf(target);
    let durable;
    if (!complete) {
      durable = "pending_stream";
    } else if (integrity !== "verified" || target["envelope"] === null) {
      durable = "not_verified";
    } else {
      const matchingManifest = matching.filter((stageTask) => stageTask["taskManifestSha256"] === target["envelope"]["manifestSha256"]);
      if (matching.length === 0 || matchingManifest.length === 0) {
        durable = "absent";
      } else if (matchingManifest.some((stageTask) => stageTask["bodySha256"] === target["verifiedDigest"])) {
        durable = "verified";
      } else {
        durable = "failed";
      }
    }
    const envelope = target["envelope"];
    const publish = target["status"] === "truncated" ? integrity === "unverified_truncated" : target["status"] === "ready" && integrity === "verified";
    taskTarget = Object.freeze({
      taskId: target["taskId"],
      revision: target["revision"],
      status: target["status"],
      failure: target["failure"],
      byteCount: envelope === null ? null : envelope["byteCount"],
      retainedBytes: target["retainedBytes"],
      truncated: target["truncated"],
      integrity,
      durable,
      verifiedDigest: target["verifiedDigest"],
      manifestSha256: envelope === null ? null : envelope["manifestSha256"],
      bodySha256: envelope === null ? null : envelope["bodySha256"],
      text: publish ? target["decodedText"] : null,
    });
  }
  let planTarget = null;
  if (state["planTarget"] !== null) {
    const target = state["planTarget"];
    let pointerStatus;
    if (!complete || target["terminal"] === false || target["truncated"]) {
      pointerStatus = "pending";
    } else {
      let resolved = true;
      for (const record of target["records"]) {
        if (record["record"] !== "task") {
          continue;
        }
        const ledgerRecord = ledgerByTaskPair.get(`${record["id"]}@${record["revision"]}`);
        if (ledgerRecord === undefined || ledgerRecord["sha256"] !== record["sha256"]) {
          resolved = false;
          break;
        }
      }
      pointerStatus = resolved ? "verified" : "failed";
    }
    const envelope = target["envelope"];
    planTarget = Object.freeze({
      revision: target["revision"],
      status: target["status"],
      failure: target["failure"],
      manifestSha256: envelope === null ? null : envelope["manifestSha256"],
      originExecution: envelope === null ? null : envelope["originExecution"],
      records: target["records"],
      pointerStatus,
    });
  }
  return Object.freeze({
    complete,
    error: state["error"],
    staleCleared: state["staleCleared"],
    taskLedger: Object.freeze(taskLedger),
    planLedger: Object.freeze(planLedger),
    taskTarget,
    planTarget,
  });
}

/**
 * The closed state machine of the Plan-and-tasks section: three
 * single-flight transports, one checksum selection per axis, the same
 * continuation contract, the epoch-gated body verification and the
 * fail-closed publication policy. A stale snapshot or a typed failure
 * clears the whole view and waits for the next applied authoritative
 * revision (no automatic re-issue, no same-pin restart). Never POSTs,
 * never timers, never browser storage.
 */
export function createPlanTasksViewer(io) {
  if (
    !isRecord(io) ||
    !isRecord(io["identityTransport"]) ||
    !isRecord(io["taskTransport"]) ||
    !isRecord(io["planTransport"]) ||
    typeof io["onState"] !== "function"
  ) {
    throw new UiContractError("invalid_transport_io");
  }
  const identityTransport = io["identityTransport"];
  const taskTransport = io["taskTransport"];
  const planTransport = io["planTransport"];
  const onState = io["onState"];
  let state = null;
  let disposed = false;
  let nextEpoch = 1;
  let nextReaderId = 1;
  let identityInFlight = false;
  let taskInFlight = false;
  let planInFlight = false;

  const publish = () => {
    onState(state);
  };

  const alive = () => !disposed;

  const clearForRefresh = () => {
    if (!alive() || state === null) {
      return;
    }
    state = Object.freeze({
      ...createPlanTasksState(state["runId"], state["pin"]),
      staleCleared: true,
    });
    publish();
  };

  const failTaskTarget = (layer, reason) => {
    if (!alive() || state === null || state["taskTarget"] === null) {
      return;
    }
    state = s6WithTaskTarget(state, s6TaskFailure(state["taskTarget"], layer, reason));
    publish();
  };

  const failPlanTarget = (layer, reason) => {
    if (!alive() || state === null || state["planTarget"] === null) {
      return;
    }
    state = s6WithPlanTarget(state, s6PlanFailure(state["planTarget"], layer, reason));
    publish();
  };

  /** Any non-chunk/page outcome (protocol error, orchestrator failure,
   * transport loss) clears the whole view and waits. */
  const clearFromOutcome = (result, runId) => {
    if (result === undefined || result.ok === false) {
      clearForRefresh();
      return;
    }
    const model = result.model;
    if (model.kind === "error" && model.reason === "stale_snapshot") {
      clearForRefresh();
      return;
    }
    clearForRefresh();
  };

  const requestIdentity = (runId, after, kind, pin) => {
    if (!alive() || state === null) {
      return;
    }
    identityInFlight = true;
    identityTransport.request({
      runId,
      kind,
      path: identityRequestPath(runId, after, pin, IDENTITY_PAGE_LIMIT),
      onApplied: (result) => {
        identityInFlight = false;
        const live = state;
        if (!alive() || live === null || live["runId"] !== runId) {
          return;
        }
        if (result === undefined || result.ok === false || result.model.kind !== "identity") {
          clearFromOutcome(result, runId);
          return;
        }
        try {
          const next = applyPlanTasksIdentityPage(live, result.model, after);
          state = next;
        } catch (error) {
          if (error instanceof UiContractError) {
            clearForRefresh();
            return;
          }
          throw error;
        }
        publish();
      },
    });
  };

  const ledgerTaskRecord = (taskId, revision) => {
    const live = state;
    if (live === null) {
      return null;
    }
    for (const record of live["identity"]["records"]) {
      if (record["record"] === "task_revision" && record["taskId"] === taskId && record["revision"] === revision) {
        return record;
      }
    }
    return null;
  };

  const ledgerPlanRecord = (revision) => {
    const live = state;
    if (live === null) {
      return null;
    }
    for (const record of live["identity"]["records"]) {
      if (record["record"] === "plan_revision" && record["revision"] === revision) {
        return record;
      }
    }
    return null;
  };

  const scheduleTaskVerification = (capturedKey, capturedEpoch) => {
    const live = state;
    if (
      live === null ||
      live["taskTarget"] === null ||
      live["taskTarget"]["key"] !== capturedKey ||
      live["taskTarget"]["epoch"] !== capturedEpoch ||
      live["taskTarget"]["terminal"] !== true
    ) {
      return;
    }
    const target = live["taskTarget"];
    if (target["envelope"] === null || target["status"] !== "verifying") {
      return;
    }
    const bytes = s6ConcatBytes([...target["retained"]]);
    const envelope = target["envelope"];
    void verifyTaskBodyDigest(bytes, envelope["bodySha256"]).then(
      (hex) => {
        const nowLive = state;
        if (!alive() || nowLive === null || nowLive["taskTarget"] === null) {
          return;
        }
        const nowTarget = nowLive["taskTarget"];
        if (nowTarget["key"] !== capturedKey || nowTarget["epoch"] !== capturedEpoch || nowTarget["envelope"] !== envelope) {
          return;
        }
        if (hex === null) {
          failTaskTarget("contract", "artifact_modified");
          return;
        }
        state = s6WithTaskTarget(
          nowLive,
          Object.freeze({
            ...nowTarget,
            status: "ready",
            integrity: "verified",
            verifiedDigest: hex,
          }),
        );
        publish();
      },
      () => {
        const nowLive = state;
        if (!alive() || nowLive === null || nowLive["taskTarget"] === null) {
          return;
        }
        const nowTarget = nowLive["taskTarget"];
        if (nowTarget["key"] !== capturedKey || nowTarget["epoch"] !== capturedEpoch) {
          return;
        }
        failTaskTarget("contract", "verification_unavailable");
      },
    );
  };

  /** Maps one settled task request failure to the target-failure
   * presentation with its manual Retry (transport loss, error envelope
   * or failed command result); the identity stream keeps its separate
   * clear discipline. */
  const s6OutcomeFailure = (result) => {
    if (result === undefined || result.ok !== true) {
      const failure = result !== undefined && result.ok === false ? result.failure : null;
      return failure === null ? { layer: "transport", reason: "network_error" } : { layer: failure.layer, reason: failure.reason };
    }
    const model = result.model;
    if (model !== null && typeof model === "object") {
      if (model.kind === "error") {
        return { layer: model.layer, reason: model.reason };
      }
      if (model.kind === "command_result") {
        return { layer: "orchestrator", reason: model.reason ?? "failed" };
      }
    }
    return null;
  };

  const requestTask = (targetAtIssue, offset, limit, kind) => {
    if (!alive() || state === null || state["taskTarget"] === null) {
      return;
    }
    const live = state["taskTarget"];
    if (live["key"] !== targetAtIssue["key"] || live["epoch"] !== targetAtIssue["epoch"]) {
      return;
    }
    taskInFlight = true;
    taskTransport.request({
      runId: state["runId"],
      kind,
      binding: Object.freeze({ runId: state["runId"], taskId: live["taskId"], revision: live["revision"] }),
      path: taskContentRequestPath(state["runId"], live["taskId"], live["revision"], offset, limit),
      onApplied: (result) => {
        taskInFlight = false;
        const nowLive = state;
        if (
          !alive() ||
          nowLive === null ||
          nowLive["taskTarget"] === null ||
          nowLive["taskTarget"]["key"] !== targetAtIssue["key"] ||
          nowLive["taskTarget"]["epoch"] !== targetAtIssue["epoch"]
        ) {
          return;
        }
        const endpointFailure = s6OutcomeFailure(result);
        if (endpointFailure !== null) {
          failTaskTarget(endpointFailure.layer, endpointFailure.reason);
          return;
        }
        try {
          state = applyTaskChunk(state, result.model, offset, limit, targetAtIssue["readerId"]);
        } catch (error) {
          if (error instanceof UiContractError) {
            failTaskTarget("contract", error.reason);
            return;
          }
          throw error;
        }
        publish();
        scheduleTaskVerification(targetAtIssue["key"], targetAtIssue["epoch"]);
      },
    });
  };

  const requestPlan = (targetAtIssue, after, limit, kind) => {
    if (!alive() || state === null || state["planTarget"] === null) {
      return;
    }
    const live = state["planTarget"];
    if (live["key"] !== targetAtIssue["key"] || live["epoch"] !== targetAtIssue["epoch"]) {
      return;
    }
    planInFlight = true;
    planTransport.request({
      runId: state["runId"],
      kind,
      binding: Object.freeze({ runId: state["runId"], revision: live["revision"] }),
      path: planContentRequestPath(state["runId"], live["revision"], after, limit),
      onApplied: (result) => {
        planInFlight = false;
        const nowLive = state;
        if (
          !alive() ||
          nowLive === null ||
          nowLive["planTarget"] === null ||
          nowLive["planTarget"]["key"] !== targetAtIssue["key"] ||
          nowLive["planTarget"]["epoch"] !== targetAtIssue["epoch"]
        ) {
          return;
        }
        const endpointFailure = s6OutcomeFailure(result);
        if (endpointFailure !== null) {
          failPlanTarget(endpointFailure.layer, endpointFailure.reason);
          return;
        }
        try {
          state = applyPlanPage(state, result.model, after, limit, targetAtIssue["readerId"]);
        } catch (error) {
          if (error instanceof UiContractError) {
            failPlanTarget("contract", error.reason);
            return;
          }
          throw error;
        }
        publish();
      },
    });
  };

  const initialTaskLimit = () => Math.min(S6_TASK_CHUNK_LIMIT_DEFAULT, S6_BODY_RETAINED_BYTES_CAP);
  const continuationTaskLimit = (target) => Math.min(S6_TASK_CHUNK_LIMIT_DEFAULT, S6_BODY_RETAINED_BYTES_CAP - target["retainedBytes"]);
  const initialPlanLimit = () => Math.min(S6_PLAN_PAGE_LIMIT_DEFAULT, S6_PLAN_RECORDS_RETAINED_CAP);
  const continuationPlanLimit = (target) => Math.min(S6_PLAN_PAGE_LIMIT_DEFAULT, S6_PLAN_RECORDS_RETAINED_CAP - target["records"].length);

  return Object.freeze({
    inspectApplied(runModel) {
      if (disposed || !isRecord(runModel) || runModel["kind"] !== "run" || !isSafeId(runModel["runId"]) || !isPositiveSafeIndex(runModel["revision"])) {
        return;
      }
      const runId = runModel["runId"];
      const revision = runModel["revision"];
      if (state === null || state["runId"] !== runId || state["pin"] !== revision) {
        identityTransport.invalidate();
        taskTransport.invalidate();
        planTransport.invalidate();
        identityInFlight = false;
        taskInFlight = false;
        planInFlight = false;
        state = createPlanTasksState(runId, revision);
        if (!identityInFlight) {
          requestIdentity(runId, 0, "initial", revision);
        }
        publish();
        return;
      }
      if (state["staleCleared"] || state["error"] !== null) {
        // the stale-snapshot discipline: wait for the next applied
        // revision or the explicit reload
        return;
      }
      if (!state["identity"]["loaded"] && !identityInFlight) {
        requestIdentity(runId, 0, "initial", state["pin"]);
        publish();
      }
    },
    selectTask(taskId, revision) {
      if (!alive() || state === null) {
        throw new UiContractError("invalid_state");
      }
      if (!isSafeId(taskId) || !isPositiveSafeIndex(revision)) {
        throw new UiContractError("invalid_request");
      }
      const record = ledgerTaskRecord(taskId, revision);
      if (record === null) {
        // D2: the stream has not completed yet — the row stays inert
        return;
      }
      const live = state["taskTarget"];
      if (taskInFlight && live !== null && live["status"] === "requesting" && live["taskId"] === taskId && live["revision"] === revision) {
        // D1: the identical selection is already in flight
        return;
      }
      taskTransport.invalidate();
      taskInFlight = false;
      const readerId = nextReaderId;
      nextReaderId += 1;
      const target = Object.freeze({
        ...s6NewTaskTarget(taskId, revision, record["sha256"], record["previousSha256"], nextEpoch, initialTaskLimit()),
        readerId,
      });
      nextEpoch += 1;
      state = s6WithTaskTarget(state, target);
      publish();
      requestTask(target, 0, initialTaskLimit(), "initial");
    },
    selectPlan(revision) {
      if (!alive() || state === null) {
        throw new UiContractError("invalid_state");
      }
      if (!isPositiveSafeIndex(revision)) {
        throw new UiContractError("invalid_request");
      }
      const record = ledgerPlanRecord(revision);
      if (record === null) {
        return;
      }
      const live = state["planTarget"];
      if (planInFlight && live !== null && live["status"] === "requesting" && live["revision"] === revision) {
        return;
      }
      planTransport.invalidate();
      planInFlight = false;
      const readerId = nextReaderId;
      nextReaderId += 1;
      const target = Object.freeze({
        ...s6NewPlanTarget(revision, record["sha256"], record["previousSha256"], record["originExecution"], nextEpoch, initialPlanLimit()),
        readerId,
      });
      nextEpoch += 1;
      state = s6WithPlanTarget(state, target);
      publish();
      requestPlan(target, 0, initialPlanLimit(), "initial");
    },
    loadMoreIdentity() {
      if (!alive() || state === null || state["error"] !== null || state["staleCleared"] || identityInFlight) {
        return;
      }
      const stream = state["identity"];
      if (!stream["loaded"] || stream["nextAfter"] === null) {
        return;
      }
      requestIdentity(state["runId"], stream["nextAfter"], "continuation", state["pin"]);
    },
    loadMoreTaskBody() {
      if (!alive() || state === null || state["taskTarget"] === null) {
        return;
      }
      const target = state["taskTarget"];
      if (target["failure"] !== null || target["terminal"] || target["truncatedPending"] || taskInFlight) {
        return;
      }
      const remaining = S6_BODY_RETAINED_BYTES_CAP - target["retainedBytes"];
      if (remaining <= 0 || target["cursor"] >= S6_BODY_RETAINED_BYTES_CAP) {
        return;
      }
      const limit = continuationTaskLimit(target);
      const readerId = nextReaderId;
      nextReaderId += 1;
      state = s6WithTaskTarget(state, Object.freeze({ ...target, inFlightLimit: limit, readerId }));
      requestTask(state["taskTarget"], target["cursor"], limit, "continuation");
    },
    retryTaskBody() {
      if (!alive() || state === null || state["taskTarget"] === null || taskInFlight) {
        return;
      }
      taskTransport.invalidate();
      taskInFlight = false;
      const previous = state["taskTarget"];
      const record = ledgerTaskRecord(previous["taskId"], previous["revision"]);
      if (record === null) {
        failTaskTarget("contract", "contract_row_gone");
        return;
      }
      const readerId = nextReaderId;
      nextReaderId += 1;
      const target = Object.freeze({
        ...s6NewTaskTarget(previous["taskId"], previous["revision"], record["sha256"], record["previousSha256"], nextEpoch, initialTaskLimit()),
        readerId,
      });
      nextEpoch += 1;
      state = s6WithTaskTarget(state, target);
      publish();
      requestTask(target, 0, initialTaskLimit(), "initial");
    },
    loadMorePlanPage() {
      if (!alive() || state === null || state["planTarget"] === null) {
        return;
      }
      const target = state["planTarget"];
      if (target["failure"] !== null || target["terminal"] || target["truncated"] || planInFlight) {
        return;
      }
      const remaining = S6_PLAN_RECORDS_RETAINED_CAP - target["records"].length;
      if (remaining <= 0) {
        return;
      }
      const limit = continuationPlanLimit(target);
      const readerId = nextReaderId;
      nextReaderId += 1;
      state = s6WithPlanTarget(state, Object.freeze({ ...target, inFlightLimit: limit, readerId }));
      requestPlan(state["planTarget"], target["cursor"], limit, "continuation");
    },
    retryPlanContent() {
      if (!alive() || state === null || state["planTarget"] === null || planInFlight) {
        return;
      }
      planTransport.invalidate();
      planInFlight = false;
      const previous = state["planTarget"];
      const record = ledgerPlanRecord(previous["revision"]);
      if (record === null) {
        failPlanTarget("contract", "contract_row_gone");
        return;
      }
      const readerId = nextReaderId;
      nextReaderId += 1;
      const target = Object.freeze({
        ...s6NewPlanTarget(previous["revision"], record["sha256"], record["previousSha256"], record["originExecution"], nextEpoch, initialPlanLimit()),
        readerId,
      });
      nextEpoch += 1;
      state = s6WithPlanTarget(state, target);
      publish();
      requestPlan(target, 0, initialPlanLimit(), "initial");
    },
    reload() {
      if (!alive() || state === null || identityInFlight) {
        return;
      }
      identityTransport.invalidate();
      taskTransport.invalidate();
      planTransport.invalidate();
      identityInFlight = false;
      taskInFlight = false;
      planInFlight = false;
      state = createPlanTasksState(state["runId"], state["pin"]);
      publish();
      requestIdentity(state["runId"], 0, "initial", state["pin"]);
    },
    refreshBindings() {
      if (!alive()) {
        return;
      }
      publish();
    },
    reset() {
      identityTransport.invalidate();
      taskTransport.invalidate();
      planTransport.invalidate();
      identityInFlight = false;
      taskInFlight = false;
      planInFlight = false;
      state = null;
      publish();
    },
    currentState() {
      return state;
    },
    dispose() {
      disposed = true;
      identityTransport.invalidate();
      taskTransport.invalidate();
      planTransport.invalidate();
      state = null;
      publish();
    },
  });
}

/* Thin DOM adapter: createElement/textContent only.                   */
/* ------------------------------------------------------------------ */

function element(tag, text) {
  const node = document.createElement(tag);
  if (text !== undefined) {
    node.textContent = text;
  }
  return node;
}

function textInput(inputId, className) {
  const input = document.createElement("input");
  input.id = inputId;
  input.type = "text";
  input.className = className;
  input.autocomplete = "off";
  input.spellcheck = false;
  return input;
}

function labeledControl(labelText, control) {
  const label = document.createElement("label");
  label.htmlFor = control.id;
  label.textContent = labelText;
  label.appendChild(control);
  return label;
}

function describeCommandResult(model) {
  if (model.outcome === "failed") {
    return model.reason === null ? `outcome failed (exit ${model.exitCode})` : `outcome failed: ${model.reason}`;
  }
  return `outcome ${model.outcome}`;
}

function showResultFailure(result) {
  if (!result.ok) {
    showError(formatFailure(result.failure));
    return;
  }
  if (result.model.kind === "error") {
    showError(formatFailure(result.model));
    return;
  }
  showError(`contract: unexpected ${result.model.kind} document`);
}

function setMutationControlsDisabled(disabled) {
  for (const node of document.querySelectorAll(".mutation-control")) {
    node.disabled = disabled;
  }
}

function authorityFor(runId) {
  let entry = null;
  for (const run of runsState.runs) {
    if (run.runId === runId) {
      entry = run;
      break;
    }
  }
  return { projectsModel: projectsState, runEntry: entry };
}

function renderProjects(model) {
  projectsState = model;
  // the discovery publish is one exact invalidation point of an open
  // revise-task confirmation (normalized authority compared by content)
  dropConfirmationIfAuthorityChanged();
  const body = document.getElementById("projects-body");
  body.replaceChildren();
  for (const project of model.projects) {
    const row = document.createElement("tr");
    row.appendChild(element("td", project.id));
    row.appendChild(element("td", project.inputRootIds.join(", ")));
    body.appendChild(row);
  }
  const select = document.getElementById("new-run-project");
  const previous = select.value;
  select.replaceChildren();
  for (const project of model.projects) {
    const option = document.createElement("option");
    option.value = project.id;
    option.textContent = project.id;
    select.appendChild(option);
  }
  if (previous !== "" && model.projects.some((project) => project.id === previous)) {
    select.value = previous;
  }
  renderRunRail();
}

function renderRuns(state) {
  runsState = state;
  // the runs discovery publish is one exact invalidation point of an
  // open revise-task confirmation (normalized authority compared by
  // content, including the projectAvailable flip)
  dropConfirmationIfAuthorityChanged();
  const body = document.getElementById("runs-body");
  body.replaceChildren();
  for (const run of state.runs) {
    const row = document.createElement("tr");
    row.appendChild(element("td", run.runId));
    row.appendChild(element("td", run.projectId));
    row.appendChild(element("td", run.projectAvailable ? "yes" : "no"));
    const actions = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = "Inspect";
    button.addEventListener("click", () => {
      void inspectRun(run.runId);
    });
    actions.appendChild(button);
    row.appendChild(actions);
    body.appendChild(row);
  }
  document.getElementById("load-more").hidden = state.nextAfter === null;
}

function renderRunDetail(model) {
  document.getElementById("run-detail").hidden = false;
  document.getElementById("detail-status").textContent = model.status;
  document.getElementById("detail-revision").textContent = String(model.revision);
  document.getElementById("detail-boundary").textContent = JSON.stringify(model.boundary, null, 2);
  const outputsBody = document.getElementById("outputs-body");
  outputsBody.replaceChildren();
  for (const output of model.outputs) {
    const row = document.createElement("tr");
    row.appendChild(element("td", output.id));
    row.appendChild(element("td", output.type));
    row.appendChild(element("td", output.required ? "yes" : "no"));
    row.appendChild(element("td", output.present ? "yes" : "no"));
    row.appendChild(element("td", output.digest === null ? "" : output.digest));
    outputsBody.appendChild(row);
  }
  document.getElementById("outputs-table").hidden = false;
}

/* S1 REGION START -------------------------------------------------- */
/* Run rail (S1): one derived identity line and one capability chip,   */
/* both pure presentations over the already-applied models.            */
/* ------------------------------------------------------------------ */

/** The last applied authoritative inspect model the rail was rendered
 * with; the non-inspect rail call sites re-render from this snapshot. */
let lastRailInspectModel = null;

function railFallbackResult(events) {
  return Object.freeze({ kind: "fallback", label: `identity: from journal (${events} events)`, events });
}

function railResolvedResult(generation, iteration, state) {
  return Object.freeze({ kind: "resolved", generation, iteration, state });
}

function railRevisionOf(entry) {
  const revision = entry["revision"];
  return typeof revision === "number" && Number.isSafeInteger(revision) && revision >= 1 ? revision : null;
}

function railStageIndexOf(entry, key) {
  const value = entry[key];
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1 ? value : null;
}

function railStateIdOf(entry) {
  const value = entry["state_id"];
  return typeof value === "string" && isSafeId(value) ? value : null;
}

/** The exact top-level key set of the frozen timeline model. */
const RAIL_TIMELINE_KEYS = Object.freeze([
  "runId",
  "cursor",
  "snapshotRevision",
  "events",
  "nextAfter",
  "stopped",
  "error",
]);

function railPositiveSafeInteger(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1;
}

function railNonNegativeSafeInteger(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

/**
 * The one strict shape owner of the identity derivation. It validates
 * the EXACT frozen timeline model `createTimelineState`/`applyEventsPage`
 * produce — every deviation is a model-shape failure of the whole
 * derivation (the zero-event fallback), never skipped events and never
 * a partial result:
 * - exact own keys and types, the frozen model and its frozen events;
 * - the stopped/error coherence (`stopTimeline` sets both together);
 * - the cursor/snapshotRevision/nextAfter coherence of the merge chain
 *   (cursor <= snapshotRevision; nextAfter null iff cursor equals the
 *   snapshot revision; non-null nextAfter equals the cursor);
 * - the dense event prefix: `events.length === cursor` and every entry
 *   a frozen-shaped applied event — record, revision exactly index+1,
 *   a kind of the closed public vocabulary through the one
 *   `eventReferenceKeys` owner carrying exactly `{revision, kind}` plus
 *   its kind's reference keys, and well-typed identity references for
 *   the identity kinds.
 * A malformed entry is a shape failure wherever it sits in the model;
 * only semantic lifecycle contradictions stay bounded by the applied
 * inspect revision (the dense-prefix rule of §3.2.1).
 */
function railTimelineShapeValid(timeline) {
  if (isRecord(timeline) !== true || Object.isFrozen(timeline) !== true) {
    return false;
  }
  const own = Object.keys(timeline);
  if (own.length !== RAIL_TIMELINE_KEYS.length) {
    return false;
  }
  for (const key of RAIL_TIMELINE_KEYS) {
    if (Object.hasOwn(timeline, key) !== true) {
      return false;
    }
  }
  if (isSafeId(timeline["runId"]) !== true) {
    return false;
  }
  if (railNonNegativeSafeInteger(timeline["cursor"]) !== true) {
    return false;
  }
  if (railNonNegativeSafeInteger(timeline["snapshotRevision"]) !== true) {
    return false;
  }
  if (Array.isArray(timeline["events"]) !== true || Object.isFrozen(timeline["events"]) !== true) {
    return false;
  }
  const nextAfter = timeline["nextAfter"];
  if (nextAfter !== null && railPositiveSafeInteger(nextAfter) !== true) {
    return false;
  }
  if (typeof timeline["stopped"] !== "boolean") {
    return false;
  }
  if (timeline["error"] !== null && isRecord(timeline["error"]) !== true) {
    return false;
  }
  if (timeline["stopped"] === true && timeline["error"] === null) {
    return false;
  }
  if (timeline["stopped"] === false && timeline["error"] !== null) {
    return false;
  }
  if (timeline["cursor"] > timeline["snapshotRevision"]) {
    return false;
  }
  if (nextAfter === null) {
    if (timeline["cursor"] !== timeline["snapshotRevision"]) {
      return false;
    }
  } else if (timeline["cursor"] !== nextAfter || nextAfter > timeline["snapshotRevision"]) {
    return false;
  }
  const events = timeline["events"];
  if (events.length !== timeline["cursor"]) {
    return false;
  }
  for (let index = 0; index < events.length; index += 1) {
    const entry = events[index];
    if (isRecord(entry) !== true) {
      return false;
    }
    if (railRevisionOf(entry) !== index + 1) {
      return false;
    }
    const refs = eventReferenceKeys(entry["kind"]);
    if (refs === null) {
      return false;
    }
    if (Object.hasOwn(entry, "kind") !== true || Object.hasOwn(entry, "revision") !== true) {
      return false;
    }
    if (Object.keys(entry).length !== refs.length + 2) {
      return false;
    }
    for (const ref of refs) {
      if (Object.hasOwn(entry, ref) !== true) {
        return false;
      }
    }
    if (refs.includes("generation_index") && railStageIndexOf(entry, "generation_index") === null) {
      return false;
    }
    if (refs.includes("iteration_index") && railStageIndexOf(entry, "iteration_index") === null) {
      return false;
    }
    if (refs.includes("state_id") && railStateIdOf(entry) === null) {
      return false;
    }
  }
  return true;
}

/**
 * The one identity derivation of the run rail: a single forward scan
 * over the dense journal prefix bound to the applied inspect revision
 * (`events[revision] <= inspectRevision`), tracking the open stage
 * generation, its open iteration and the state id of the execution
 * started inside that iteration. The state is owned by the currently
 * open iteration and is never carried across iterations or
 * generations; execution starts outside an open iteration (planning,
 * control, pre-generation) are observed and ignored for identity. All
 * eligibility gate failures and journal contradictions render as the
 * one fixed fallback string; contradictions inside the prefix fail the
 * derivation closed, contradictions after the applied inspect revision
 * are ignored entirely. Identity-relevant entry count is the only
 * magnitude the fallback label may carry. Gate 1 is the one strict
 * timeline-shape owner: foreign shapes, missing/extra keys, malformed
 * entries and unknown kinds anywhere in the model fall back whole
 * (the ignored-entry leniency of the first revision is gone).
 */
export function deriveRunRailIdentity(timeline, inspectRevision) {
  if (railTimelineShapeValid(timeline) !== true) {
    return railFallbackResult(0);
  }
  if (timeline["stopped"] !== false) {
    return railFallbackResult(0);
  }
  const inspect =
    typeof inspectRevision === "number" && Number.isSafeInteger(inspectRevision) && inspectRevision >= 1
      ? inspectRevision
      : null;
  if (inspect === null || timeline["cursor"] < inspect) {
    return railFallbackResult(timeline["cursor"] < inspect ? timeline["cursor"] : 0);
  }
  const events = timeline["events"];
  const considered = inspect;
  let openGeneration = null;
  let openIteration = null;
  let currentState = null;
  let contradiction = false;
  for (let index = 0; index < inspect; index += 1) {
    const entry = events[index];
    if (contradiction) {
      continue;
    }
    if (entry.kind === "stage_generation_opened") {
      const generation = railStageIndexOf(entry, "generation_index");
      if (openGeneration !== null || generation === null) {
        contradiction = true;
      } else {
        openGeneration = generation;
        openIteration = null;
        currentState = null;
      }
    } else if (entry.kind === "stage_iteration_opened") {
      const generation = railStageIndexOf(entry, "generation_index");
      const iteration = railStageIndexOf(entry, "iteration_index");
      if (openGeneration === null || generation !== openGeneration || openIteration !== null || iteration === null) {
        contradiction = true;
      } else {
        openIteration = iteration;
        currentState = null;
      }
    } else if (entry.kind === "stage_iteration_closed") {
      const generation = railStageIndexOf(entry, "generation_index");
      const iteration = railStageIndexOf(entry, "iteration_index");
      if (openGeneration === null || openIteration === null || generation !== openGeneration || iteration !== openIteration) {
        contradiction = true;
      } else {
        openIteration = null;
        currentState = null;
      }
    } else if (entry.kind === "stage_generation_closed") {
      const generation = railStageIndexOf(entry, "generation_index");
      if (openGeneration === null || generation !== openGeneration) {
        contradiction = true;
      } else {
        openGeneration = null;
        openIteration = null;
        currentState = null;
      }
    } else if (entry.kind === "start_agent_execution" || entry.kind === "start_decision_execution") {
      if (openGeneration !== null && openIteration !== null) {
        const stateId = railStateIdOf(entry);
        if (stateId === null) {
          contradiction = true;
        } else {
          currentState = stateId;
        }
      }
    }
  }
  if (contradiction) {
    return railFallbackResult(considered);
  }
  if (openGeneration === null || openIteration === null || currentState === null) {
    return railFallbackResult(considered);
  }
  return railResolvedResult(openGeneration, openIteration, currentState);
}

/** The stopped/errored timeline and the unbound derivation are
 * handled inside the derivation itself (§3.2.3 gates 1-3); the
 * renderer only guards the model-timeline binding below.
 */
function railIdentityText(derivation) {
  if (derivation.kind === "resolved") {
    return `Generation ${derivation.generation} · Iteration ${derivation.iteration} · State ${derivation.state}`;
  }
  return derivation.label;
}

const RAIL_CHIP_NONE = Object.freeze({ state: "none", projectApply: null });

/** The exact parsed run-entry shape of the applied runs model
 * (`parseRunsDocument`): the three own keys with the run id and the
 * project id as safe ids and a typed `projectAvailable` boolean. An
 * entry without its authoritative run id — or any foreign shape — is
 * not an authoritative selected-run entry (§9.4 condition 1) and can
 * never carry the capability claim on its project fields alone. */
function railRunEntryValid(entry) {
  if (isRecord(entry) !== true) {
    return false;
  }
  const own = Object.keys(entry);
  if (own.length !== 3) {
    return false;
  }
  if (Object.hasOwn(entry, "runId") !== true || Object.hasOwn(entry, "projectId") !== true || Object.hasOwn(entry, "projectAvailable") !== true) {
    return false;
  }
  return isSafeId(entry["runId"]) === true && isSafeId(entry["projectId"]) === true && typeof entry["projectAvailable"] === "boolean";
}

function railChipResult(available, projectApply) {
  return Object.freeze({ state: available ? "available" : "unavailable", projectApply });
}

/**
 * The one capability derivation of the run rail chip: a claim exists
 * only when ALL FOUR authority conditions hold simultaneously — an
 * authoritative selected-run entry in the applied runs model, that
 * entry carrying `projectAvailable === true`, a matching discovery
 * project in the applied discovery model, and that project entry
 * declaring a typed `project_apply` boolean. Condition 1 is owned by
 * the one strict run-entry shape above: the exact three-key parsed
 * entry with a safe `runId` and a safe `projectId` — an entry missing
 * its run id (or any foreign shape) is never authoritative even when
 * its project fields alone would satisfy conditions 2-4. Any failing
 * condition yields no claim at all: absence is never rendered as a
 * false claim and never as an absence text; `projectAvailable` is part
 * of the authority so a stale matching discovery entry can never
 * resurrect a claim for a run whose binding reports the project
 * unavailable.
 */
export function deriveRunRailCapability(projectsModel, runEntry) {
  if (railRunEntryValid(runEntry) !== true || runEntry["projectAvailable"] !== true) {
    return RAIL_CHIP_NONE;
  }
  if (
    projectsModel === null ||
    isRecord(projectsModel) !== true ||
    projectsModel["kind"] !== "projects" ||
    Array.isArray(projectsModel["projects"]) !== true
  ) {
    return RAIL_CHIP_NONE;
  }
  let projectEntry = null;
  for (const project of projectsModel["projects"]) {
    if (
      isRecord(project) !== true ||
      isSafeId(project["id"]) !== true ||
      typeof project["projectApply"] !== "boolean"
    ) {
      return RAIL_CHIP_NONE;
    }
    if (project["id"] === runEntry["projectId"]) {
      projectEntry = project;
    }
  }
  if (projectEntry === null) {
    return RAIL_CHIP_NONE;
  }
  return railChipResult(projectEntry["projectApply"], projectEntry["projectApply"]);
}

const RAIL_CHIP_AVAILABLE_TEXT = "Project apply: available";
const RAIL_CHIP_UNAVAILABLE_TEXT = "Project apply: Unavailable in effective configuration";

/**
 * The dedicated rail renderer: the derived identity line and the
 * capability chip. Not owned by renderRunDetail; invoked at every
 * already-applied-model site (the applied inspect, every timeline
 * transition, the run selection change and the discovery/runs refresh
 * sites). Pure DOM presentation — no fetch, no timer, no mutation.
 */
function renderRunRail(inspectModel) {
  const givenModel = inspectModel === undefined;
  const model = givenModel ? lastRailInspectModel : inspectModel;
  if (givenModel !== true) {
    lastRailInspectModel = inspectModel;
  }
  const identity = document.getElementById("detail-identity");
  const chip = document.getElementById("detail-apply-capability");
  if (identity === null || chip === null) {
    return;
  }
  // The rail renders only for the timeline of the still-selected run;
  // a model of another run (or no applied inspect at all) zeros the
  // rail instead of presenting a stale derivation. The inspect anchor
  // itself stays owned by the application sites: after a timeline
  // reset (`lastInspectRevision === null`) the derivation reports the
  // honest zero-event fallback.
  const bound = model !== null && timeline !== null && timeline.runId === model.runId ? model : null;
  if (bound === null) {
    identity.textContent = "";
    chip.textContent = "";
    chip.hidden = true;
    return;
  }
  identity.textContent = railIdentityText(deriveRunRailIdentity(timeline, lastInspectRevision));
  const authority = authorityFor(model.runId);
  const capability = deriveRunRailCapability(authority.projectsModel, authority.runEntry);
  // no claim at all: the chip is hidden and cleared, never rendered as
  // a false or absence text
  if (capability.state === "none") {
    chip.textContent = "";
    chip.hidden = true;
    return;
  }
  chip.hidden = false;
  chip.textContent =
    capability.state === "available" ? RAIL_CHIP_AVAILABLE_TEXT : RAIL_CHIP_UNAVAILABLE_TEXT;
}

/* S1 REGION END ----------------------------------------------------- */

/* ------------------------------------------------------------------ */
/* Lifecycle timeline (v4): the thin DOM adapter of the events feed.   */
/* ------------------------------------------------------------------ */

/** The verbatim reference text of one event: only the safe public
 * references, in the canonical order, as `key=value` pairs. */
function eventReferenceText(entry) {
  const parts = [];
  for (const key of EVENT_REFERENCE_ORDER) {
    if (Object.hasOwn(entry, key)) {
      parts.push(`${key}=${String(entry[key])}`);
    }
  }
  return parts.join(", ");
}

function resetTimelineDom() {
  document.getElementById("events-body").replaceChildren();
  document.getElementById("events-status").textContent = "";
  document.getElementById("events-load-more").hidden = true;
  document.getElementById("events-retry").hidden = true;
}

/** The timeline of the selected run: created once per selection and
 * reset on a reload; a run change discards the previous timeline and
 * invalidates its in-flight events GET. Returns whether a fresh
 * timeline snapshot was created — the exact moment the initial
 * `after=0` load belongs to, and the only ordinary-inspect path that
 * may start one. An existing timeline is never reset here. */
function ensureTimelineFor(runId) {
  if (timeline !== null && timeline.runId === runId) {
    document.getElementById("events-section").hidden = false;
    return false;
  }
  eventsTransport.invalidate();
  timeline = createTimelineState(runId);
  lastInspectRevision = null;
  document.getElementById("events-section").hidden = false;
  resetTimelineDom();
  renderTimelineMeta();
  renderRunRail();
  return true;
}

function renderTimelineMeta() {
  if (timeline === null) {
    return;
  }
  const summary = timelineSummary(timeline, lastInspectRevision);
  const detail = summary.detailRevision === null ? "–" : String(summary.detailRevision);
  let text = `Detail revision: ${detail} · Events revision: ${summary.eventsRevision} · Cursor: ${summary.cursor} · `;
  if (summary.stopped) {
    text += "stopped";
  } else if (summary.caughtUp) {
    text += "caught up";
  } else if (summary.hasMore) {
    text += "more available";
  } else {
    text += "loading";
  }
  document.getElementById("events-meta").textContent = text;
  document.getElementById("events-load-more").hidden = !summary.hasMore;
  document.getElementById("events-retry").hidden = !(timeline.stopped && timelineRetryAvailable(timeline));
}

/** Append-only row rendering: only the not-yet-rendered events are
 * added, so focus and scroll position survive every page. */
function renderEventsDelta(previousCount) {
  if (timeline === null) {
    return;
  }
  const body = document.getElementById("events-body");
  for (let index = previousCount; index < timeline.events.length; index += 1) {
    const entry = timeline.events[index];
    const row = document.createElement("tr");
    row.appendChild(element("td", String(entry.revision)));
    row.appendChild(element("td", entry.kind));
    row.appendChild(element("td", eventReferenceText(entry)));
    body.appendChild(row);
  }
}

function announceTimeline(previous, next) {
  const message = timelineAnnouncement(previous, next);
  if (message !== null) {
    document.getElementById("events-status").textContent = message;
  }
}

/**
 * The one application path of a settled events GET: the page merges
 * only into the still-selected run's timeline, failures stop the
 * timeline fail-closed (isolated from the inspect detail and the
 * mutation controls) and `invalid_cursor` leaves no retry path.
 */
function applyEventsOutcome(runId, after, result) {
  if (selectedRunId !== runId || timeline === null || timeline.runId !== runId) {
    return;
  }
  const previous = timeline;
  if (result === undefined) {
    timeline = stopTimeline(timeline, { layer: "transport", reason: "network_error" });
    finishTimelineUpdate(previous);
    return;
  }
  if (result.ok === false) {
    timeline = stopTimeline(timeline, result.failure);
    finishTimelineUpdate(previous);
    return;
  }
  const model = result.model;
  if (model.kind === "error") {
    timeline = stopTimeline(timeline, { layer: model.layer, reason: model.reason });
    finishTimelineUpdate(previous);
    return;
  }
  if (model.kind === "command_result") {
    timeline = stopTimeline(
      timeline,
      model.runId === runId
        ? { layer: "orchestrator", reason: model.reason ?? "unknown_failure" }
        : { layer: "contract", reason: "unexpected_run" },
    );
    finishTimelineUpdate(previous);
    return;
  }
  try {
    timeline = applyEventsPage(timeline, model, after);
  } catch (error) {
    if (error instanceof UiContractError) {
      timeline = stopTimeline(previous, { layer: "contract", reason: error.reason });
      finishTimelineUpdate(previous);
      return;
    }
    throw error;
  }
  renderEventsDelta(previous.events.length);
  finishTimelineUpdate(previous);
}

function finishTimelineUpdate(previous) {
  announceTimeline(previous, timeline);
  renderTimelineMeta();
  renderRunRail();
}

function requestTimelinePage(runId, after) {
  if (!isSafeId(runId)) {
    return;
  }
  eventsTransport.request({
    runId,
    after,
    onApplied: (result) => {
      applyEventsOutcome(runId, after, result);
    },
  });
}

/**
 * The one automatic events fetch site: an applied authoritative inspect
 * may pull exactly one tail page when the timeline is caught up, not
 * stopped, and behind the inspect revision. The initial load (cursor 0)
 * is the same rule at selection time; a partial page (`nextAfter`
 * non-null) is continued only through the explicit Load more action.
 */
function maybeTailEvents(model) {
  if (selectedRunId !== model.runId || timeline === null || timeline.runId !== model.runId) {
    return;
  }
  if (timeline.stopped || timeline.nextAfter !== null) {
    return;
  }
  if (model.revision <= timeline.cursor) {
    return;
  }
  requestTimelinePage(model.runId, timeline.cursor);
}

function loadMoreEvents() {
  if (timeline === null || timeline.stopped || timeline.nextAfter === null) {
    return;
  }
  requestTimelinePage(timeline.runId, timeline.nextAfter);
}

/**
 * The explicit reload: the timeline resets to the fresh state and the
 * feed restarts from the beginning. An in-flight events GET of the
 * discarded timeline is invalidated first and awaited, so the reload
 * request always reaches the wire.
 */
async function reloadTimeline() {
  const runId = selectedRunId;
  if (runId === null || !isSafeId(runId)) {
    return;
  }
  const pending = eventsTransport.invalidate();
  timeline = createTimelineState(runId);
  lastInspectRevision = null;
  resetTimelineDom();
  renderTimelineMeta();
  renderRunRail();
  if (pending !== null) {
    await pending;
  }
  requestTimelinePage(runId, 0);
}

function retryTimeline() {
  if (timeline === null || !timeline.stopped || !timelineRetryAvailable(timeline)) {
    return;
  }
  requestTimelinePage(timeline.runId, timeline.cursor);
}

function showRunModel(model, options = {}) {
  const key = liveObservationDocumentKey(model);
  // A manual inspect reset the detail panel first, so its own result
  // must always render; a scheduled poll of an unchanged document
  // renders nothing (focus and selection survive).
  const changed = options.force === true || key !== lastDocumentKey;
  lastDocumentKey = key;
  selectedRunId = model.runId;
  lastRunModel = model;
  if (changed) {
    const modelAuthority = authorityFor(model.runId);
    // the applied authoritative inspect is one exact invalidation point
    // of the open confirmation: a changed document identity, a changed
    // wait or action set, a non-waiting boundary or a changed revise
    // authority destroy it (state destroy, never only a DOM removal)
    if (currentConfirmation !== null && !confirmationMatches(currentConfirmation, model, modelAuthority)) {
      destroyCurrentConfirmation();
    }
    renderRunDetail(model);
    renderRunControls(buildRunActionModel(model, modelAuthority));
    if (mutationGate.isInFlight()) {
      setMutationControlsDisabled(true);
    }
  }
  refreshSequence += 1;
  updateLiveStateLine();
  ensureTimelineFor(model.runId);
  ensureWorkerLogsFor(model.runId);
  lastInspectRevision = model.revision;
  // the applied authoritative inspect provides the pin and drives the
  // two initial lifecycle-history GETs (identity + execution bindings)
  ensureLifecycleHistoryFor(model.runId);
  lifecycleHistoryTerminal = model.status === "succeeded" || model.status === "failed";
  if (lifecycleHistoryViewer !== null) {
    lifecycleHistoryViewer.inspectApplied(model);
    renderLifecycleHistory(lifecycleHistoryViewer.currentState());
  }
  // the applied authoritative inspect provides the pin and drives the
  // plan-and-tasks pinned ledger GET (plus its stale-clear discipline)
  ensurePlanTasksFor(model.runId);
  if (planTasksViewer !== null) {
    planTasksViewer.inspectApplied(model);
    renderPlanTasks(planTasksViewer.currentState());
  }
  renderRunRail(model);
  renderTimelineMeta();
  maybeTailEvents(model);
  // the applied authoritative inspect drives the automatic metadata
  // refresh of the logs viewer (first inspect: exactly one list GET;
  // later inspects: one per strictly greater revision)
  workerLogViewer.inspectApplied(model);
  renderWorkerLogs(workerLogViewer === null ? null : workerLogViewer.currentState());
  // the applied authoritative inspect drives the automatic one-shot
  // project-changes load (exactly one initial GET per terminal revision)
  ensureProjectChangesFor(model.runId);
  if (projectChangesViewer !== null) {
    projectChangesViewer.inspectApplied(model);
    renderProjectChanges(projectChangesViewer.currentState());
  }
  // the apply panel follows the same selection lifecycle; the terminal
  // status of the last applied inspect feeds the eligibility only
  lastInspectedStatus = model.status;
  ensureProjectApplyFor(model.runId);
  return changed;
}

async function loadProjects() {
  const result = expectDiscoveryModel(await requestApiJson(fetchImplementation, "/v1/projects"), "projects");
  if (result.ok && result.model.kind === "projects") {
    renderProjects(result.model);
    // the effective capability may arrive after a run selection; the
    // apply panel re-evaluates against the loaded discovery
    const applyState = applyViewer.currentState();
    if (applyState !== null) {
      renderProjectApply(applyState);
    }
    setStatus("Projects loaded.");
    return;
  }
  showResultFailure(result);
}

function observationSnapshotOf(model) {
  return Object.freeze({ status: model.status, boundaryKind: model.boundary.kind, revision: model.revision });
}

function applySchedulerEffect(effect) {
  liveTimers.apply(effect);
  if (effect.effect === "start_inspect") {
    void scheduledInspect();
  }
  updateLiveStateLine();
}

function updateLiveStateLine() {
  const line = document.getElementById("live-state");
  if (line === null) {
    return;
  }
  const state = scheduler.presentationState();
  let text = `Live updates: ${liveStateLabel(state)}`;
  if (refreshSequence > 0 && state !== "off") {
    text += ` · refresh ${refreshSequence}`;
  }
  line.textContent = text;
}

/**
 * The shared inspect tail: only the response of a request the transport
 * still considers valid and of the still-selected run applies; the
 * transport calls it only for a never-superseded settlement, so a
 * superseded response is dropped before any rendering or machine
 * event. A scheduled poll stays silent on an unchanged document and
 * stops the observation on any failure.
 */
function applyInspectOutcome(runId, result, scheduled) {
  if (selectedRunId !== runId) {
    return;
  }
  const inspected = expectRunOrError(result);
  if (inspected.ok && inspected.model.kind === "run") {
    const model = inspected.model;
    const keyBefore = lastDocumentKey;
    showRunModel(model, { force: !scheduled });
    if (scheduled) {
      const announce = livePollAnnouncement(keyBefore, model);
      if (announce !== null) {
        setStatus(announce);
      }
    } else {
      setStatus(`Run ${runId} inspected.`);
    }
    applySchedulerEffect(scheduler.inspectSucceeded(runId, observationSnapshotOf(model)));
    return;
  }
  showError(formatFailure(inspected.ok ? inspected.model : inspected.failure));
  setStatus(
    scheduled
      ? "Live update stopped; use Refresh or Inspect to retry."
      : `Run ${runId} could not be inspected; use Refresh or Inspect to retry.`,
  );
  applySchedulerEffect(scheduler.inspectFailed(runId));
}

/** The scheduler-driven read-only poll of the selected run. */
function scheduledInspect() {
  const runId = selectedRunId;
  if (runId === null) {
    return;
  }
  inspectTransport.request({
    runId,
    onApplied: (result) => {
      applyInspectOutcome(runId, result, true);
    },
  });
}

/**
 * The manual inspect: a user action that resets the observation for the
 * selected run, cancels any pending timer and hands its desire to the
 * one inspect transport. An inspect already on the wire is superseded
 * (its response is dropped) and the manual inspect runs as the single
 * coalesced follow-up after the wire slot is released.
 */
function inspectRun(runId) {
  if (!isSafeId(runId)) {
    showError("contract: invalid_run_id");
    return;
  }
  setStatus(`Inspecting run ${runId}…`);
  applySchedulerEffect(scheduler.runSelected(runId));
  selectedRunId = runId;
  // selecting another run destroys any open confirmation before the
  // GET response arrives (the state destroy, never only a DOM removal)
  destroyCurrentConfirmation();
  applySchedulerEffect(scheduler.takeOverInspect(runId));
  document.getElementById("detail-run-id").textContent = `Run ${runId}`;
  document.getElementById("inspect-selected").hidden = false;
  document.getElementById("run-detail").hidden = true;
  document.getElementById("outputs-table").hidden = true;
  document.getElementById("run-controls").replaceChildren();
  // The initial `after=0` load belongs to the operation that creates a
  // new timeline snapshot: the first selection of a run (or its
  // re-selection after a full reset). An ordinary Inspect of an
  // already-selected run never resets the timeline and never restarts
  // the feed from the beginning — the applied authoritative inspect
  // tails the existing timeline through `maybeTailEvents` instead.
  if (ensureTimelineFor(runId)) {
    requestTimelinePage(runId, 0);
  }
  inspectTransport.request({
    runId,
    onApplied: (result) => {
      applyInspectOutcome(runId, result, false);
    },
  });
}

async function refreshRuns(announce) {
  const generation = runsGuard.next();
  const result = expectDiscoveryModel(await requestApiJson(fetchImplementation, runsPagePath(null)), "runs");
  // A page response started before a newer Refresh never overwrites it.
  if (!runsGuard.isCurrent(generation)) {
    return;
  }
  if (result.ok && result.model.kind === "runs") {
    runsState = applyRunsPage(createRunsState(), result.model);
    renderRuns(runsState);
    renderRunRail();
    if (announce) {
      setStatus("Runs refreshed.");
    }
    return;
  }
  showResultFailure(result);
}

async function loadMoreRuns() {
  if (runsState.nextAfter === null) {
    return;
  }
  const generation = runsGuard.current();
  const after = runsState.nextAfter;
  const result = expectDiscoveryModel(await requestApiJson(fetchImplementation, runsPagePath(after)), "runs");
  if (!runsGuard.isCurrent(generation)) {
    return;
  }
  if (result.ok && result.model.kind === "runs") {
    runsState = applyRunsPage(runsState, result.model);
    renderRuns(runsState);
    renderRunRail();
    setStatus("More runs loaded.");
    return;
  }
  showResultFailure(result);
}

function readInputRows() {
  const rows = [];
  for (const row of document.getElementById("input-rows").children) {
    const id = row.querySelector(".input-id").value;
    const rootId = row.querySelector(".input-root").value;
    const ref = row.querySelector(".input-ref").value;
    if (id === "" && rootId === "" && ref === "") {
      continue;
    }
    rows.push({ id, rootId, ref });
  }
  return rows;
}

/**
 * The one mutating POST owner: every mutating request of the UI (fresh
 * run, resume, resume-plan, continue-stage, revise-task) is sent here
 * exactly once and driven through the shared mutation flow. The
 * authoritative inspect of the flow also drives the live observation:
 * `running`/`in_flight` after a completed mutator may start polling,
 * any other boundary or failure never does.
 */
async function postMutation(request, flow) {
  await runMutationFlow({
    send: () =>
      requestApiJson(fetchImplementation, request.path, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(request.body),
      }),
    inspect: (runId) => notifyObservationOfInspect(runId),
    runId: flow.runId,
    handlers: flow.handlers,
  });
}

/**
 * The authoritative post-mutation inspect: one GET through the one
 * inspect transport that renders through the flow handlers and drives
 * the live-observation machine (never a mutation, never a retry of the
 * POST). The raw result reaches the flow whatever the supersede state
 * is; the machine application happens only for a never-superseded
 * settlement of the still-selected run, so a superseded poll response
 * can never release or take the machine slot.
 */
function notifyObservationOfInspect(runId) {
  return inspectTransport.request({
    runId,
    onApplied: (result) => {
      if (selectedRunId !== runId) {
        return;
      }
      const inspected = expectRunOrError(result);
      if (inspected.ok && inspected.model.kind === "run") {
        showRunModel(inspected.model);
        applySchedulerEffect(scheduler.inspectSucceeded(runId, observationSnapshotOf(inspected.model)));
        return;
      }
      applySchedulerEffect(scheduler.inspectFailed(runId));
    },
  });
}

/**
 * The one mutating submit path: the shared gate admits at most one
 * mutating POST, the request is built and fully validated before any
 * fetch, and the shared flow owns the outcome, the lost-response
 * recovery and the authoritative inspect.
 */
/**
 * The one mutating submit path AND the single mutation-admission owner:
 * admission is synchronous and before the first await — the token check
 * (confirmation press) or the direct submit, the one `mutationGate`
 * admission, the atomic consumption of the saved frozen request for a
 * confirmed press (after which the confirmation is destroyed and any
 * re-click, including after settlement, is stale), then the controls
 * disable, the existing flow, and the `finally` restore. A stale token
 * or a busy gate leaves the confirmation, the controls, the status and
 * the DOM byte-unchanged with zero POST.
 */
async function submitAction(admission, readForm, describe) {
  // pre-admission classification: the token check is byte-inert and
  // never reaches the gate
  const isConfirm = typeof admission === "number";
  let entry = null;
  if (isConfirm) {
    entry = currentConfirmation;
    if (entry === null || entry.token !== admission) {
      return;
    }
  }
  if (!mutationGate.begin()) {
    return;
  }
  // from a successful begin on, EVERY path of this submit — the
  // confirmation consumption, the direct build request refusal, every
  // asynchronous continuation — runs under ONE try/finally that
  // releases the gate again, so no post-begin path can ever leak the
  // global mutation gate
  let controlsDisabled = false;
  let mutationObserved = false;
  let request;
  let runId;
  try {
    if (isConfirm) {
      // admission is atomic: consume the confirmation and destroy the
      // panel inside the gate-owned region, immediately behind the
      // successful begin
      currentConfirmation = null;
      if (currentConfirmationPanel !== null) {
        currentConfirmationPanel.remove();
        currentConfirmationPanel = null;
      }
      request = entry.request;
      describe = confirmationDescribe(entry.sourceIdentity);
      runId = entry.sourceIdentity.runId;
    } else {
      const action = admission;
      runId = action.runId;
      try {
        request = buildMutationRequest(action, readForm());
      } catch (error) {
        if (error instanceof UiContractError) {
          showError(`contract: ${error.reason}`);
          return;
        }
        throw error;
      }
    }
    setMutationControlsDisabled(true);
    controlsDisabled = true;
    inspectTransport.invalidate();
    applySchedulerEffect(scheduler.mutationStarted());
    mutationObserved = true;
    setStatus(`Sending ${describe} for run ${runId}…`);
    await postMutation(request, {
      runId,
      handlers: {
        onCommandResult: async (model) => {
          setStatus(describeCommandResult(model));
          await refreshRuns(false);
        },
        onLost: () => {
          setStatus(`outcome unknown; the ${describe} request for run ${runId} may have been received; checking once…`);
        },
        onRecovered: (model) => {
          if (selectedRunId !== model.runId) {
            return;
          }
          showRunModel(model);
          setStatus(`Run ${model.runId} inspected after the ${describe} request.`);
        },
        onRecoveryUnavailable: (presented) => {
          showError(formatFailure(presented));
          setStatus(`outcome unknown or unchanged; use Refresh or Inspect.`);
        },
        onError: (presented) => {
          showError(formatFailure(presented));
          setStatus(`The ${describe} request was not applied; use Refresh or Inspect.`);
        },
      },
    });
  } finally {
    mutationGate.end();
    if (controlsDisabled) {
      setMutationControlsDisabled(false);
    }
    if (mutationObserved) {
      applySchedulerEffect(scheduler.mutationCompleted());
    }
    renderProjectApplyIfSelected();
  }
}

let controlCounter = 0;

function controlInput(labelText, className) {
  controlCounter += 1;
  return labeledControl(labelText, textInput(`control-${controlCounter}-${className}`, className));
}

function mutationButton(label, onClick) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "mutation-control";
  button.textContent = label;
  button.addEventListener("click", onClick);
  return button;
}

function waitIndexLine(control) {
  return element("p", `Wait index ${control.waitIndex} (from the inspect projection; not editable)`);
}

function resumePlanForm(action) {
  const form = document.createElement("form");
  const stageField = controlInput("Stage id", "form-stage-id");
  const budgetField = controlInput("Initial budget", "form-initial-budget");
  const submit = document.createElement("button");
  submit.type = "submit";
  submit.className = "mutation-control";
  submit.textContent = "Resume plan";
  form.appendChild(stageField);
  form.appendChild(budgetField);
  form.appendChild(submit);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    void submitAction(action, () => ({
      stageId: stageField.querySelector("input").value,
      initialBudget: budgetField.querySelector("input").value,
    }), "resume-plan");
  });
  return form;
}

function continueStageForm(control) {
  const form = document.createElement("form");
  const iterationsField = controlInput("Additional iterations", "form-additional-iterations");
  const submit = document.createElement("button");
  submit.type = "submit";
  submit.className = "mutation-control";
  submit.textContent = "Continue stage";
  form.appendChild(waitIndexLine(control));
  form.appendChild(iterationsField);
  form.appendChild(submit);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    openConfirmation(control, () => ({
      additionalIterations: iterationsField.querySelector("input").value,
    }));
  });
  return form;
}

function reviseTaskForm(control) {
  const form = document.createElement("form");
  const taskField = controlInput("Task id", "form-task-id");
  const rootSelect = document.createElement("select");
  controlCounter += 1;
  rootSelect.id = `control-${controlCounter}-form-root-id`;
  rootSelect.className = "form-root-id";
  rootSelect.autocomplete = "off";
  for (const rootId of control.inputRootIds) {
    const option = document.createElement("option");
    option.value = rootId;
    option.textContent = rootId;
    rootSelect.appendChild(option);
  }
  const rootField = labeledControl("Input root", rootSelect);
  const refField = controlInput("Input ref", "form-task-ref");
  const submit = document.createElement("button");
  submit.type = "submit";
  submit.className = "mutation-control";
  submit.textContent = "Revise task";
  form.appendChild(waitIndexLine(control));
  form.appendChild(taskField);
  form.appendChild(rootField);
  form.appendChild(refField);
  form.appendChild(submit);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    openConfirmation(control, () => ({
      taskId: taskField.querySelector("input").value,
      rootId: rootSelect.value,
      ref: refField.querySelector("input").value,
    }));
  });
  return form;
}

function blockedReviseControl(control) {
  const causes = {
    projects_not_loaded: "the project discovery has not been loaded",
    project_missing: "the run-to-project binding is not in the loaded discovery",
    project_unavailable: "the bound project is not available in the current registry",
    no_input_roots: "the bound project declares no input roots",
  };
  const container = document.createElement("div");
  container.appendChild(waitIndexLine(control));
  const button = document.createElement("button");
  button.type = "button";
  button.className = "mutation-control";
  button.textContent = "Revise task";
  button.disabled = true;
  container.appendChild(button);
  container.appendChild(element("p", `Revise task is disabled: ${causes[control.cause] ?? "the project authority is unavailable"}.`));
  return container;
}

function renderRunControls(action) {
  const container = document.getElementById("run-controls");
  container.replaceChildren();
  if (action.kind === "unusable_boundary") {
    container.appendChild(element("p", `contract: ${action.reason} — no manual control; use Refresh.`));
    return;
  }
  if (action.kind === "observe_only") {
    container.appendChild(element("p", "Execution in flight — use Refresh to re-inspect."));
    return;
  }
  if (action.kind === "terminal") {
    container.appendChild(
      element("p", action.status === "failed" ? "The run failed; no manual controls." : "The run succeeded; no manual controls."),
    );
    return;
  }
  if (action.kind === "resume") {
    container.appendChild(mutationButton("Resume run", () => {
      void submitAction(action, () => ({}), "resume");
    }));
    return;
  }
  if (action.kind === "resume_plan") {
    container.appendChild(resumePlanForm(action));
    return;
  }
  if (action.kind === "waiting") {
    for (const control of action.controls) {
      if (control.kind === "continue_stage") {
        container.appendChild(continueStageForm(control));
      } else if (control.kind === "revise_task") {
        container.appendChild(reviseTaskForm(control));
      } else if (control.kind === "respond") {
        container.appendChild(respondForm(control));
      } else {
        container.appendChild(blockedReviseControl(control));
      }
    }
    if (currentConfirmation !== null && currentConfirmationPanel !== null) {
      container.appendChild(currentConfirmationPanel);
    }
  }
}

/** The one confirmation-open path of every waiting action: the form
 * values are read exactly once and the request is built exactly once —
 * the frozen result is the single request owner of the confirmation
 * (the Confirm press never re-reads the form and never rebuilds). */
function openConfirmation(control, readForm) {
  // a new submit always replaces the open confirmation: the old panel
  // and its token die here, the new confirmation owns the page
  destroyCurrentConfirmation();
  let request;
  try {
    request = buildMutationRequest(control, readForm());
  } catch (error) {
    if (error instanceof UiContractError) {
      showError(`contract: ${error.reason}`);
      return;
    }
    throw error;
  }
  const entry = Object.freeze({
    token: confirmationCounter + 1,
    request,
    sourceIdentity: confirmationSourceOf(control, lastRunModel, authorityFor(control.runId)),
  });
  confirmationCounter = entry.token;
  currentConfirmation = entry;
  currentConfirmationPanel = renderConfirmationPanel(entry);
  document.getElementById("run-controls").appendChild(currentConfirmationPanel);
}

/** The confirmation panel: read-only textContent of the endpoint and
 * the exact canonical body of the saved request, plus Confirm (the
 * single admission path) and Cancel (zero POST by contract). */
function renderConfirmationPanel(entry) {
  const panel = document.createElement("div");
  panel.className = "confirmation-panel";
  const runId = entry.sourceIdentity.runId;
  panel.appendChild(element("p", `Confirm ${confirmationLabel(entry.sourceIdentity)} for run ${runId}?`));
  panel.appendChild(element("p", `POST ${entry.request.path}`));
  panel.appendChild(element("pre", JSON.stringify(entry.request.body)));
  const confirm = document.createElement("button");
  confirm.type = "button";
  confirm.className = "mutation-control";
  confirm.textContent = "Confirm";
  confirm.addEventListener("click", () => {
    void submitAction(entry.token);
  });
  panel.appendChild(confirm);
  const cancel = document.createElement("button");
  cancel.type = "button";
  cancel.className = "confirmation-cancel";
  cancel.textContent = "Cancel";
  cancel.addEventListener("click", () => {
    if (currentConfirmation !== null && currentConfirmation.token === entry.token) {
      destroyCurrentConfirmation();
    }
  });
  panel.appendChild(cancel);
  return panel;
}

/** The label of one open confirmation, derived exclusively from the
 * frozen source identity of the holder. */
function confirmationLabel(sourceIdentity) {
  if (sourceIdentity.actionKind === "continue_stage") {
    return "continue stage";
  }
  if (sourceIdentity.actionKind === "revise_task") {
    return "revise task";
  }
  return `respond (${sourceIdentity.actionId})`;
}

function respondForm(control) {
  const form = document.createElement("form");
  form.appendChild(waitIndexLine(control));
  form.appendChild(
    element(
      "p",
      `Respond with the declared action ${control.actionId} (the destination ${control.actionTo} is contract metadata only).`,
    ),
  );
  const submit = document.createElement("button");
  submit.type = "submit";
  submit.className = "mutation-control";
  submit.textContent = "Respond";
  form.appendChild(submit);
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    openConfirmation(control, () => ({}));
  });
  return form;
}

async function submitNewRun(event) {
  event.preventDefault();
  if (!mutationGate.begin()) {
    return;
  }
  clearMessages();
  const projectId = document.getElementById("new-run-project").value;
  const runId = document.getElementById("new-run-id").value;
  let body;
  try {
    body = buildFreshRunBody(projectId, runId, readInputRows());
  } catch (error) {
    if (error instanceof UiContractError) {
      showError(`contract: ${error.reason}`);
      return;
    }
    throw error;
  }
  const submit = document.getElementById("submit-run");
  setMutationControlsDisabled(true);
  let mutationObserved = false;
  try {
    inspectTransport.invalidate();
    applySchedulerEffect(scheduler.mutationStarted());
    mutationObserved = true;
    setStatus(`Creating run ${runId}…`);
    await postMutation({ path: freshRunPath(projectId), body }, {
      runId,
      handlers: {
        onCommandResult: async (model) => {
          setStatus(describeCommandResult(model));
          await refreshRuns(false);
        },
        onLost: () => {
          setStatus(`outcome unknown; the create request for run ${runId} may have been received; checking once…`);
        },
        onRecovered: (model) => {
          applySchedulerEffect(scheduler.runSelected(model.runId));
          showRunModel(model);
          applySchedulerEffect(scheduler.inspectSucceeded(model.runId, observationSnapshotOf(model)));
        },
        onRecoveryUnavailable: (presented) => {
          showError(formatFailure(presented));
          setStatus(`outcome unknown; run ${runId} not found yet — use Refresh or Inspect.`);
        },
        onError: (presented) => {
          showError(formatFailure(presented));
        },
      },
    });
  } finally {
    mutationGate.end();
    setMutationControlsDisabled(false);
    if (mutationObserved) {
      applySchedulerEffect(scheduler.mutationCompleted());
    }
  }
}

let runsState = createRunsState();
let projectsState = null;
let selectedRunId = null;
let lastDocumentKey = null;
let refreshSequence = 0;
let timeline = null;
let lastInspectRevision = null;
const mutationGate = createMutationGate();
const runsGuard = createGenerationGuard();
const fetchImplementation = typeof fetch === "function" ? fetch : null;
const scheduler = createLiveObservationScheduler();
const liveTimers = createSchedulerTimerAdapter(
  { setTimeout, clearTimeout },
  () => {
    applySchedulerEffect(scheduler.timerFired());
  },
);
const inspectTransport = createInspectTransport({
  fetchInspect: (runId, signal) => requestApiJson(fetchImplementation, `/v1/runs/${runId}`, { signal }),
  onInspectStarted: (runId) => {
    applySchedulerEffect(scheduler.inspectStarted(runId));
  },
  onInspectDropped: (runId) => {
    applySchedulerEffect(scheduler.inspectCancelled(runId));
  },
});
const eventsTransport = createEventsTransport({
  fetchEvents: (runId, after, signal) => requestEventsJson(fetchImplementation, eventsRequestPath(runId, after), { signal }, runId),
  onStarted: () => {
    for (const id of ["events-load-more", "events-reload", "events-retry"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["events-load-more", "events-reload", "events-retry"]) {
      document.getElementById(id).disabled = false;
    }
  },
});


/* ------------------------------------------------------------------ */
/* Worker logs (v5): the thin DOM adapter of the logs viewer.          */
/* ------------------------------------------------------------------ */

let workerLogViewer = null;

const logsTransport = createLogsTransport({
  fetchLogs: (runId, path, signal) => requestWorkerLogsJson(fetchImplementation, path, { signal }, runId),
  onStarted: () => {
    for (const id of ["logs-load", "logs-next-chunk", "logs-retry"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["logs-load", "logs-next-chunk", "logs-retry"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

/** The section of the selected run: created once per selection; a run
 * change discards the previous viewer and invalidates its in-flight
 * logs GET. The metadata list follows the applied authoritative
 * inspects automatically (the viewer's `inspectApplied`); payload bytes
 * load only on the explicit `Load log` press. */
function ensureWorkerLogsFor(runId) {
  const current = workerLogViewer === null ? null : workerLogViewer.currentState();
  if (current !== null && current.runId === runId) {
    document.getElementById("worker-logs-section").hidden = false;
    return;
  }
  if (workerLogViewer === null) {
    workerLogViewer = createWorkerLogViewer({
      transport: logsTransport,
      onState: (state) => renderWorkerLogs(state),
    });
  }
  workerLogViewer.transitionToLogsView(runId);
  document.getElementById("worker-logs-section").hidden = false;
  resetWorkerLogsDom();
}

/* ------------------------------------------------------------------ */
/* Lifecycle history (v9): the thin DOM adapter of the merged streams.  */
/* ------------------------------------------------------------------ */

let lifecycleHistoryViewer = null;
let lifecycleHistoryRunId = null;
let lifecycleHistoryTerminal = false;

const lhIdentityTransport = createIdentityTransport({
  fetchIdentity: (path, runId, signal) => requestIdentityJson(fetchImplementation, path, { signal }, runId),
  onStarted: () => {
    for (const id of ["lh-identity-load-more", "lh-bindings-load-more", "lh-reload"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["lh-identity-load-more", "lh-bindings-load-more", "lh-reload"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

const lhBindingsTransport = createExecutionBindingsTransport({
  fetchBindings: (path, runId, signal) => requestExecutionBindingsJson(fetchImplementation, path, { signal }, runId),
  onStarted: () => {
    for (const id of ["lh-identity-load-more", "lh-bindings-load-more", "lh-reload"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["lh-identity-load-more", "lh-bindings-load-more", "lh-reload"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

function resetLifecycleHistoryDom() {
  document.getElementById("lh-identity-meta").textContent = "";
  document.getElementById("lh-bindings-meta").textContent = "";
  document.getElementById("lh-iterations").replaceChildren();
  document.getElementById("lh-compare").replaceChildren();
  document.getElementById("lh-historical").replaceChildren();
  document.getElementById("lh-status").textContent = "";
  document.getElementById("lh-identity-load-more").hidden = true;
  document.getElementById("lh-bindings-load-more").hidden = true;
  document.getElementById("lh-reload").hidden = true;
}

/** The section of the selected run: the viewer lives for the whole page
 * and resets its streams itself whenever the selected run or the
 * applied inspect revision changes; a run change additionally resets
 * the DOM. `inspectApplied` (called from the applied inspect flow)
 * issues the initial pinned GET pair; nothing else triggers a fetch. */
function ensureLifecycleHistoryFor(runId) {
  const runChanged = lifecycleHistoryRunId !== runId;
  if (lifecycleHistoryViewer === null) {
    lifecycleHistoryViewer = createLifecycleHistoryViewer({
      identityTransport: lhIdentityTransport,
      bindingsTransport: lhBindingsTransport,
      onState: (state) => renderLifecycleHistory(state),
    });
  }
  if (runChanged) {
    lifecycleHistoryViewer.reset();
    lifecycleHistoryRunId = runId;
    resetLifecycleHistoryDom();
  }
  document.getElementById("lifecycle-history-section").hidden = false;
}

/* Plan and tasks (S6): the thin DOM adapter of the pinned ledger
 * stream and the two content surfaces. Created once per selection; a
 * run or revision change resets the viewer (the transports invalidate
 * their in-flight GETs). */

let planTasksViewer = null;
let planTasksRunId = null;

const ptIdentityTransport = createIdentityTransport({
  fetchIdentity: (path, runId, signal) => requestIdentityJson(fetchImplementation, path, { signal }, runId),
  onStarted: () => {
    for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

const ptTaskTransport = createTaskContentTransport({
  fetchTaskContent: (path, runId, signal, binding) => requestTaskContentJson(fetchImplementation, path, { signal }, binding === undefined ? null : binding),
  onStarted: () => {
    for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

const ptPlanTransport = createPlanContentTransport({
  fetchPlanContent: (path, runId, signal, binding) => requestPlanContentJson(fetchImplementation, path, { signal }, binding === undefined ? null : binding),
  onStarted: () => {
    for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["pt-identity-load-more", "pt-reload", "pt-task-load-more", "pt-task-retry", "pt-plan-load-more", "pt-plan-retry"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

function s6Paragraph(parent, className, text) {
  const node = document.createElement("p");
  node.className = className;
  node.textContent = text;
  parent.appendChild(node);
  return node;
}

function s6RenderLedgerRows(body, rows, buildButtonId) {
  body.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const cell of row.cells) {
      const td = document.createElement("td");
      td.textContent = cell;
      tr.appendChild(td);
    }
    const actionCell = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.id = buildButtonId(row);
    button.textContent = "Open";
    actionCell.appendChild(button);
    tr.appendChild(actionCell);
    body.appendChild(tr);
  }
}

function s6TaskStatusText(view) {
  if (view === null) {
    return "No task body selected.";
  }
  if (view.failure !== null) {
    return `Failed (${view.failure.layer}: ${view.failure.reason}). Use Retry body.`;
  }
  if (view.status === "unrequested") {
    return "Selected; not loaded yet.";
  }
  if (view.status === "requesting") {
    return "Requesting body chunks…";
  }
  if (view.status === "streaming") {
    return `Streaming body (${view.retainedBytes} bytes retained${view.byteCount === null ? "" : ` of ${view.byteCount}`})…`;
  }
  if (view.status === "verifying") {
    return `Verifying body digest (${view.retainedBytes} bytes retained)…`;
  }
  if (view.status === "truncated") {
    return `Truncated at the ${(view.retainedBytes / 1024).toFixed(0)} KiB preview cap; the full body is ${view.byteCount} bytes. Preview published unverified.`;
  }
  if (view.status === "ready" && view.integrity === "verified") {
    const durable = view.durable === "verified" ? "ledger-verified" : view.durable === "absent" ? "no accepted execution binding in the ledger" : view.durable;
    return `Body verified (sha256 ${digestShorthand(view.verifiedDigest ?? "")}…, ${view.byteCount} bytes); ${durable}.`;
  }
  return view.status;
}

function s6PlanStatusLabel(status) {
  if (status === "unrequested") return "Selected; not loaded yet.";
  if (status === "requesting") return "Requesting plan pages…";
  if (status === "streaming") return "Streaming plan records…";
  if (status === "ready") return "Loaded completely.";
  if (status === "truncated") return `Truncated at the ${S6_PLAN_RECORDS_RETAINED_CAP} record cap.`;
  return status;
}

function s6RenderTaskPanel(derived) {
  const panel = document.getElementById("pt-task-panel");
  const view = derived.taskTarget;
  if (view === null) {
    panel.hidden = true;
    return;
  }
  panel.hidden = false;
  document.getElementById("pt-task-title").textContent = `task ${view.taskId} revision ${view.revision}`;
  document.getElementById("pt-task-status").textContent = s6TaskStatusText(view);
  const metaParts = [];
  if (view.manifestSha256 !== null) {
    metaParts.push(`manifest sha256 ${digestShorthand(view.manifestSha256)}…`);
  }
  if (view.bodySha256 !== null) {
    metaParts.push(`body sha256 ${digestShorthand(view.bodySha256)}…`);
  }
  if (view.durable !== null) {
    metaParts.push(`durable evidence: ${view.durable}`);
  }
  document.getElementById("pt-task-meta").textContent = metaParts.join(" · ");
  const text = document.getElementById("pt-task-body");
  if (view.text === null) {
    text.hidden = true;
    text.textContent = "";
  } else {
    text.hidden = false;
    text.textContent = view.text;
  }
  document.getElementById("pt-task-load-more").hidden = view.status !== "streaming";
  document.getElementById("pt-task-retry").hidden = view.failure === null;
}

function s6RenderPlanPanel(derived) {
  const panel = document.getElementById("pt-plan-panel");
  const view = derived.planTarget;
  if (view === null) {
    panel.hidden = true;
    return;
  }
  panel.hidden = false;
  document.getElementById("pt-plan-title").textContent = `plan revision ${view.revision}`;
  const statusLines = [s6PlanStatusLabel(view.status)];
  if (view.failure !== null) {
    statusLines.push(`Failed (${view.failure.layer}: ${view.failure.reason}). Use Retry plan.`);
  } else if (view.pointerStatus === "verified") {
    statusLines.push("Every task pointer matches the pinned ledger.");
  } else if (view.pointerStatus === "failed") {
    statusLines.push("contract: pointer verification failed — a selected task pointer does not match the pinned ledger.");
  }
  document.getElementById("pt-plan-status").textContent = statusLines.join(" ");
  const metaParts = [];
  if (view.manifestSha256 !== null) {
    metaParts.push(`manifest sha256 ${digestShorthand(view.manifestSha256)}…`);
  }
  if (view.originExecution !== null) {
    metaParts.push(`origin execution ${view.originExecution}`);
  }
  document.getElementById("pt-plan-meta").textContent = metaParts.join(" · ");
  const body = document.getElementById("pt-plan-content");
  body.replaceChildren();
  for (const record of view.records) {
    const tr = document.createElement("tr");
    const kindCell = document.createElement("td");
    const positionCell = document.createElement("td");
    const refCell = document.createElement("td");
    if (record.record === "stage") {
      kindCell.textContent = "stage";
      positionCell.textContent = String(record.stagePosition);
      refCell.textContent = `${record.id} (template ${record.template})`;
    } else if (record.record === "task") {
      kindCell.textContent = "task";
      positionCell.textContent = `${record.stagePosition}.${record.taskPosition}`;
      refCell.textContent = `${record.id} r${record.revision}`;
    } else {
      kindCell.textContent = "task_dependency";
      positionCell.textContent = `${record.stagePosition}.${record.taskPosition}.${record.dependencyPosition}`;
      refCell.textContent = record.taskId;
    }
    tr.appendChild(kindCell);
    tr.appendChild(positionCell);
    tr.appendChild(refCell);
    body.appendChild(tr);
  }
  document.getElementById("pt-plan-load-more").hidden = view.status !== "streaming";
  document.getElementById("pt-plan-retry").hidden = view.failure === null;
}

function renderPlanTasks(state) {
  const section = document.getElementById("plan-tasks-section");
  if (section === null || section.hidden) {
    return;
  }
  const meta = document.getElementById("pt-identity-meta");
  const status = document.getElementById("pt-status");
  const loadMore = document.getElementById("pt-identity-load-more");
  document.getElementById("pt-reload").hidden = false;
  if (state === null) {
    return;
  }
  if (state.staleCleared || state.error !== null) {
    meta.textContent = `Ledger stream: ${state.staleCleared ? "cleared after a stale snapshot or a failed load" : "failed"} · revision ${String(state.pin)}`;
    status.textContent = state.error === null
      ? "The ledger data was cleared; it reloads with the next inspect refresh, or press Reload."
      : `The ledger could not be loaded (${state.error.layer}: ${state.error.reason}). Reload or wait for the next inspect refresh.`;
    loadMore.hidden = true;
    document.getElementById("pt-task-ledger").replaceChildren();
    document.getElementById("pt-plan-ledger").replaceChildren();
    document.getElementById("pt-task-panel").hidden = true;
    document.getElementById("pt-plan-panel").hidden = true;
    return;
  }
  const identity = state.identity;
  meta.textContent = `Ledger stream: ${identity.loaded ? `loaded to ${identity.cursor}${identity.nextAfter === null ? " (complete)" : " (more available)"}` : "not loaded"} · revision ${identity.loaded ? String(state.pin) : "–"}`;
  status.textContent = identity.loaded ? "" : "The pinned ledger is loading…";
  loadMore.hidden = identity.nextAfter === null || !identity.loaded;
  const derived = derivePlanTasksView(state);
  const taskBody = document.getElementById("pt-task-ledger");
  const planBody = document.getElementById("pt-plan-ledger");
  if (!derived.complete) {
    taskBody.replaceChildren();
    planBody.replaceChildren();
    const taskNote = document.createElement("p");
    taskNote.className = "lh-partial";
    taskNote.textContent = "The pinned ledger is not complete yet; selections open once it is.";
    taskBody.appendChild(taskNote);
    s6RenderTaskPanel(derived);
    s6RenderPlanPanel(derived);
    return;
  }
  const taskRows = derived.taskLedger.map((record) => ({
    taskId: record.taskId,
    revision: record.revision,
    cells: [record.taskId, `r${record.revision}`, digestShorthand(record.sha256)],
    buttonId: `pt-task-open-${record.taskId}-${record.revision}`,
  }));
  s6RenderLedgerRows(taskBody, taskRows, (row) => row.buttonId);
  for (const row of taskRows) {
    document.getElementById(row.buttonId).addEventListener("click", () => {
      try {
        planTasksViewer.selectTask(row.taskId, row.revision);
      } catch (error) {
        if (error instanceof UiContractError) {
          status.textContent = `contract: ${error.reason}`;
          return;
        }
        throw error;
      }
      renderPlanTasks(planTasksViewer.currentState());
    });
  }
  const planRows = derived.planLedger.map((record) => ({
    revision: record.revision,
    cells: [`r${record.revision}`, digestShorthand(record.sha256)],
    buttonId: `pt-plan-open-${record.revision}`,
  }));
  s6RenderLedgerRows(planBody, planRows, (row) => row.buttonId);
  for (const row of planRows) {
    document.getElementById(row.buttonId).addEventListener("click", () => {
      try {
        planTasksViewer.selectPlan(row.revision);
      } catch (error) {
        if (error instanceof UiContractError) {
          status.textContent = `contract: ${error.reason}`;
          return;
        }
        throw error;
      }
      renderPlanTasks(planTasksViewer.currentState());
    });
  }
  s6RenderTaskPanel(derived);
  s6RenderPlanPanel(derived);
}

function resetPlanTasksDom() {
  document.getElementById("pt-identity-meta").textContent = "";
  document.getElementById("pt-status").textContent = "";
  document.getElementById("pt-task-ledger").replaceChildren();
  document.getElementById("pt-plan-ledger").replaceChildren();
  document.getElementById("pt-task-panel").hidden = true;
  document.getElementById("pt-plan-panel").hidden = true;
  document.getElementById("pt-task-body").hidden = true;
  document.getElementById("pt-task-body").textContent = "";
  document.getElementById("pt-identity-load-more").hidden = true;
  document.getElementById("pt-reload").hidden = true;
  document.getElementById("pt-task-load-more").hidden = true;
  document.getElementById("pt-task-retry").hidden = true;
  document.getElementById("pt-plan-load-more").hidden = true;
  document.getElementById("pt-plan-retry").hidden = true;
}

/** The section of the selected run: the viewer lives for the whole page
 * and resets its streams on every run or applied-revision change;
 * `inspectApplied` (called from the applied inspect flow) issues the
 * initial pinned GET; task/plan bodies open only on the explicit
 * ledger selection. */
function ensurePlanTasksFor(runId) {
  const runChanged = planTasksRunId !== runId;
  if (planTasksViewer === null) {
    planTasksViewer = createPlanTasksViewer({
      identityTransport: ptIdentityTransport,
      taskTransport: ptTaskTransport,
      planTransport: ptPlanTransport,
      onState: (state) => renderPlanTasks(state),
    });
  }
  if (runChanged) {
    planTasksViewer.reset();
    planTasksRunId = runId;
    resetPlanTasksDom();
  }
  document.getElementById("plan-tasks-section").hidden = false;
}

function digestShorthand(digest) {
  return digest.slice(0, 16);
}

function kvLine(parent, label, value) {
  const line = document.createElement("div");
  line.className = "lh-kv";
  const name = document.createElement("span");
  name.className = "lh-kv-name";
  name.textContent = label;
  const valueNode = document.createElement("span");
  valueNode.textContent = value;
  line.appendChild(name);
  line.appendChild(valueNode);
  parent.appendChild(line);
}

function executionLines(row, view) {
  const execution = view.record;
  const bindingNote =
    view.binding === null
      ? "not loaded yet"
      : view.binding.generationIndex === null
        ? "planning/control (no generation)"
        : `generation ${view.binding.generationIndex}, iteration ${view.binding.iterationIndex === null ? "–" : view.binding.iterationIndex}`;
  kvLine(row, "execution", `${execution.index} · ${execution.type} ${execution.stateId}`);
  kvLine(row, "role", `${execution.executionRole} · phase ${execution.phase} · iteration ${execution.iterationIndex === null ? "–" : execution.iterationIndex}`);
  kvLine(row, "binding", bindingNote);
  if (execution.stageTask !== undefined && execution.stageTask !== null) {
    kvLine(
      row,
      "stage task",
      `${execution.stageTask.taskId} r${execution.stageTask.taskRevision} · manifest ${digestShorthand(execution.stageTask.taskManifestSha256)} · body ${digestShorthand(execution.stageTask.bodySha256)}`,
    );
  }
  if (execution.result !== undefined && execution.result !== null) {
    const result = execution.result;
    if (result.status === "selected") {
      kvLine(row, "decision", `selected ${result.outcome} (decision ${result.decision}, rule ${result.ruleId})`);
    } else if (result.status === "invalid_facts") {
      kvLine(
        row,
        "decision",
        `invalid facts ${result.reason}${result.factId !== null ? ` · fact ${result.factId}` : ""}${result.actualType !== null ? ` (${result.actualType})` : ""}`,
      );
    } else {
      kvLine(row, "decision", result.status);
    }
  }
  for (const output of view.outputs) {
    kvLine(row, "output", `${output.id} · ${digestShorthand(output.digest)}`);
  }
  if (view.activeConstraints.length > 0) {
    kvLine(row, "active constraints", view.activeConstraints.map((entry) => entry.id).join(", "));
  }
  if (view.violatedRelations.length > 0) {
    kvLine(row, "violated relations", view.violatedRelations.map((entry) => entry.id).join(", "));
  }
}

const LH_CLOSURE_REASONS = ["grant", "replanned", "normal_close", "exhausted", "next_stage", "final_stage"];

function closureText(closed, waitText) {
  if (closed === null) {
    return "open";
  }
  return `${closed.by}${closed.waitIndex != null ? ` (wait ${closed.waitIndex})` : ""} at ${closed.closedTransitionCount}${waitText}`;
}

function renderGenerationCard(card, parent) {
  const generation = card.generation;
  const node = document.createElement("div");
  node.className = "lh-card";
  const heading = document.createElement("h5");
  heading.textContent = `Generation ${generation.index} — stage ${generation.stageId} (position ${generation.stagePosition}, template ${generation.templateId})`;
  node.appendChild(heading);
  kvLine(node, "plan", digestShorthand(generation.planSha256));
  kvLine(node, "initial budget", String(generation.initialBudget));
  kvLine(node, "opened at", String(generation.openedTransitionCount));
  kvLine(node, "iterations", String(generation.iterationCount));
  kvLine(
    node,
    "open iteration",
    generation.openIteration === null
      ? "–"
      : `${generation.openIteration.index} (opened at ${generation.openIteration.openedTransitionCount})`,
  );
  let closedText = closureText(generation.closed, "");
  let ran = false;
  if (generation.closed !== null && !LH_CLOSURE_REASONS.includes(generation.closed.by)) {
    closedText = `unexpected closure reason ${generation.closed.by}`;
    const span = document.createElement("span");
    span.className = "lh-contract-error";
    span.textContent = closedText;
    node.appendChild(span);
    ran = true;
  }
  if (!ran) {
    kvLine(node, "closed", closedText);
  }
  for (const grant of card.grants) {
    kvLine(node, "grant", `#${grant.index} · ${grant.additionalIterations} additional iteration${grant.additionalIterations === 1 ? "" : "s"} · wait ${grant.waitIndex} · intent ${digestShorthand(grant.intentSha256)}`);
  }
  for (const iteration of card.iterations) {
    const row = document.createElement("div");
    row.className = "lh-iteration";
    kvLine(row, `iteration ${iteration.index}`, `opened at ${iteration.openedTransitionCount} · ${closureText(iteration.closed, "")}`);
    for (const view of card.executions) {
      if (view.binding !== null && view.binding.iterationIndex === iteration.index) {
        executionLines(row, view);
      }
    }
    node.appendChild(row);
  }
  for (const view of card.executions) {
    if (view.binding === null || view.binding.iterationIndex === null) {
      const row = document.createElement("div");
      row.className = "lh-execution-unplaced";
      kvLine(row, "execution in this generation", String(view.record.index));
      executionLines(row, view);
      node.appendChild(row);
    }
  }
  parent.appendChild(node);
}

function renderCompare(container, cardA, cardB) {
  container.replaceChildren();
  if (cardA === null && cardB === null) {
    const note = document.createElement("p");
    note.textContent = "Select two iterations of the same stage template and press Apply.";
    container.appendChild(note);
    return;
  }
  if (cardA === null || cardB === null) {
    const note = document.createElement("p");
    note.textContent = "Both sides must be selected.";
    container.appendChild(note);
    return;
  }
  const generationA = cardA.generation;
  const generationB = cardB.generation;
  if (generationA.templateId !== generationB.templateId) {
    const note = document.createElement("p");
    note.textContent = `contract: template_mismatch (${generationA.templateId} vs ${generationB.templateId})`;
    container.appendChild(note);
    return;
  }
  const rows = document.createElement("div");
  const side = (label, card) => {
    const box = document.createElement("div");
    box.className = "lh-compare-side";
    const heading = document.createElement("h5");
    heading.textContent = `${label}: Generation ${card.generation.index} · iteration ${card.iterations.length === 0 ? "–" : String(card.iterations[card.iterations.length - 1].index)}`;
    box.appendChild(heading);
    kvLine(box, "plan", digestShorthand(card.generation.planSha256));
    kvLine(box, "initial budget", String(card.generation.initialBudget));
    kvLine(box, "opened at", String(card.generation.openedTransitionCount));
    kvLine(box, "closed", closureText(card.generation.closed, ""));
    for (const execution of card.executions) {
      if (execution.record.stageTask !== undefined && execution.record.stageTask !== null) {
        kvLine(
          box,
          "task",
          `${execution.record.stageTask.taskId} r${execution.record.stageTask.taskRevision} body ${digestShorthand(execution.record.stageTask.bodySha256)}`,
        );
      }
    }
    for (const grant of card.grants) {
      kvLine(box, "grant", `+${grant.additionalIterations} (wait ${grant.waitIndex})`);
    }
    container.appendChild(box);
  };
  side("Side A", cardA);
  side("Side B", cardB);
  const compareNote = document.createElement("p");
  compareNote.textContent =
    generationA.planSha256 === generationB.planSha256
      ? "Both generations were planned on the same plan revision."
      : "The generations were planned on different plan revisions.";
  rows.appendChild(compareNote);
  container.appendChild(rows);
}

function renderHistorical(container, derived) {
  container.replaceChildren();
  const historical = derived.historical;
  if (historical === null) {
    const note = document.createElement("p");
    note.textContent = "Historical interventions appear on terminal runs.";
    container.appendChild(note);
    return;
  }
  if (historical.waits.length === 0) {
    const note = document.createElement("p");
    note.textContent = "No user intervention happened in this run.";
    container.appendChild(note);
    return;
  }
  for (const wait of historical.waits) {
    const node = document.createElement("div");
    node.className = "lh-card";
    const heading = document.createElement("h5");
    heading.textContent = `Wait ${wait.index} at transition ${wait.transitionCount} — ${wait.reason} (state ${wait.stateId})`;
    node.appendChild(heading);
    kvLine(node, "request", digestShorthand(wait.requestSha256));
    if (wait.intentSha256 !== null) {
      kvLine(node, "intent", digestShorthand(wait.intentSha256));
    }
    const declared = historical.actions.filter((entry) => entry.waitIndex === wait.index);
    if (declared.length > 0 && wait.response === null) {
      const span = document.createElement("span");
      span.className = "lh-contract-error";
      span.textContent = `An answered wait carries no recorded response (wait ${wait.index}).`;
      node.appendChild(span);
    }
    if (declared.length === 0 && wait.response !== null) {
      const span = document.createElement("span");
      span.className = "lh-contract-error";
      span.textContent = `A wait response with no declared action (wait ${wait.index}).`;
      node.appendChild(span);
    }
    for (const entry of declared) {
      const line = document.createElement("div");
      line.className = "lh-kv";
      const name = document.createElement("span");
      name.className = "lh-kv-name";
      name.textContent = "action";
      const value = document.createElement("span");
      value.textContent = `${entry.action.id} → ${entry.action.to}`;
      line.appendChild(name);
      line.appendChild(value);
      node.appendChild(line);
    }
    kvLine(
      node,
      "response",
      wait.response === null ? "not answered" : `${wait.response.actionId} · ${digestShorthand(wait.response.responseSha256)}`,
    );
    container.appendChild(node);
  }
}

function renderLifecycleHistory(state) {
  const section = document.getElementById("lifecycle-history-section");
  if (section === null || section.hidden) {
    return;
  }
  if (state === null) {
    return;
  }
  const identity = state.identity;
  const bindings = state.bindings;
  const metaA = `Identity stream: ${identity.loaded ? `loaded to ${identity.cursor}${identity.nextAfter === null ? " (complete)" : " (more available)"}` : "not loaded"} · revision ${identity.loaded ? String(state.pin) : "–"}`;
  const metaB = `Bindings stream: ${bindings.loaded ? `loaded to ${bindings.cursor}${bindings.nextAfter === null ? " (complete)" : " (more available)"}` : "not loaded"} · revision ${bindings.loaded ? String(state.pin) : "–"}`;
  document.getElementById("lh-identity-meta").textContent = metaA;
  document.getElementById("lh-bindings-meta").textContent = metaB;
  const derived = deriveLifecycleHistory(state, lifecycleHistoryTerminal);
  const status = document.getElementById("lh-status");
  status.textContent = "";
  if (state.error !== null) {
    status.textContent = `${state.error.which} stream: ${state.error.layer}: ${state.error.reason}`;
    document.getElementById("lh-reload").hidden = false;
    document.getElementById("lh-identity-load-more").hidden = true;
    document.getElementById("lh-bindings-load-more").hidden = true;
    return;
  }
  document.getElementById("lh-identity-load-more").hidden = identity.nextAfter === null;
  document.getElementById("lh-bindings-load-more").hidden = bindings.nextAfter === null;
  document.getElementById("lh-reload").hidden = false;
  const iterations = document.getElementById("lh-iterations");
  iterations.replaceChildren();
  if (!state.identity.loaded || !state.bindings.loaded) {
    const note = document.createElement("p");
    note.className = "lh-partial";
    note.textContent = "The lifecycle history is not loaded yet (either stream is pending).";
    iterations.appendChild(note);
    return;
  }
  if (derived.error !== null) {
    const note = document.createElement("p");
    note.className = "lh-contract-error";
    note.textContent = `contract: ${derived.error.reason} — the placement between the streams failed; reload both streams.`;
    iterations.appendChild(note);
    document.getElementById("lh-reload").hidden = false;
    return;
  }
  if (derived.generations.length === 0 && derived.unplacedExecutions.length === 0) {
    const note = document.createElement("p");
    note.textContent = "No stage generations are recorded in this run.";
    iterations.appendChild(note);
  }
  for (const card of derived.generations) {
    renderGenerationCard(card, iterations);
  }
  if (derived.unplacedExecutions.length > 0) {
    const unplacedBox = document.createElement("div");
    unplacedBox.className = "lh-card";
    const heading = document.createElement("h5");
    heading.textContent = "Planning and control executions (no stage generation)";
    unplacedBox.appendChild(heading);
    for (const view of derived.unplacedExecutions) {
      executionLines(unplacedBox, view);
    }
    iterations.appendChild(unplacedBox);
  }
  renderCompareCards(derived);
  renderHistorical(document.getElementById("lh-historical"), derived);
  status.textContent =
    derived.complete && derived.error === null ? "Streams complete." : "Waiting for more stream pages.";
}

let lhCompareSelection = { a: null, b: null };

function compareChoiceLabel(card) {
  const lastIteration = card.iterations.length === 0 ? null : card.iterations[card.iterations.length - 1];
  return `Generation ${card.generation.index} · template ${card.generation.templateId}${lastIteration === null ? "" : ` · iteration ${lastIteration.index}`}`;
}

function renderCompareCards(derived) {
  const container = document.getElementById("lh-compare");
  const state = lifecycleHistoryViewer.currentState();
  if (state === null) {
    return;
  }
  const selectA = document.getElementById("lh-compare-a");
  const selectB = document.getElementById("lh-compare-b");
  selectA.replaceChildren();
  selectB.replaceChildren();
  const addPlaceholder = (select) => {
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "— select —";
    select.appendChild(placeholder);
  };
  addPlaceholder(selectA);
  addPlaceholder(selectB);
  const choices = [];
  derived.generations.forEach((card) => {
    const option = document.createElement("option");
    const key = String(card.generation.index);
    option.value = key;
    option.textContent = compareChoiceLabel(card);
    choices.push(key);
    selectA.appendChild(option);
  });
  derived.generations.forEach((card) => {
    const option = document.createElement("option");
    option.value = String(card.generation.index);
    option.textContent = compareChoiceLabel(card);
    selectB.appendChild(option);
  });
  if (lhCompareSelection.a !== null && !choices.includes(lhCompareSelection.a)) {
    lhCompareSelection = { a: null, b: null };
  }
  if (lhCompareSelection.b !== null && !choices.includes(lhCompareSelection.b)) {
    lhCompareSelection = { a: null, b: null };
  }
  selectA.value = lhCompareSelection.a ?? "";
  selectB.value = lhCompareSelection.b ?? "";
  const applyButton = document.getElementById("lh-compare-apply");
  const generationByIndex = new Map();
  for (const card of derived.generations) {
    generationByIndex.set(String(card.generation.index), card);
  }
  applyButton.textContent = "Apply compare";
  if (lhCompareSelection.a === null && lhCompareSelection.b === null) {
    renderCompare(container, null, null);
    return;
  }
  renderCompare(
    container,
    lhCompareSelection.a === null ? null : generationByIndex.get(lhCompareSelection.a) ?? null,
    lhCompareSelection.b === null ? null : generationByIndex.get(lhCompareSelection.b) ?? null,
  );
}

/* ------------------------------------------------------------------ */
/* Project changes (v6): the thin DOM adapter of the changes viewer.   */
/* ------------------------------------------------------------------ */

let projectChangesViewer = null;

const changesTransport = createProjectChangesTransport({
  fetchChanges: (runId, path, signal) => requestProjectChangesJson(fetchImplementation, path, { signal }, runId),
  onStarted: () => {
    for (const id of ["changes-reload", "changes-load-more"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["changes-reload", "changes-load-more"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

/** The content review transport and viewer (v7): the single-flight
 * wire of the bounded read-only content preview. No GET exists before
 * an explicit Review; the viewer owns no timers and never mutates. */
const contentTransport = createProjectChangeContentTransport({
  fetchContent: (runId, path, signal, expected) => requestProjectChangeContentJson(fetchImplementation, path, { signal }, expected),
  onStarted: () => {
    for (const id of ["content-review-load-baseline", "content-review-load-result", "content-review-retry", "content-review-reload-changes", "content-review-close"]) {
      document.getElementById(id).disabled = true;
    }
  },
  onSettled: () => {
    for (const id of ["content-review-load-baseline", "content-review-load-result", "content-review-retry", "content-review-reload-changes", "content-review-close"]) {
      document.getElementById(id).disabled = false;
    }
  },
});

const contentViewer = createProjectChangeContentViewer({
  transport: contentTransport,
  onState: (state) => renderContentReview(state),
});

/* S3: the single placement/visibility owner of the content review
 * panel. The panel is the one shipped element: it either sits anchored
 * behind its change row inside the changes table (a valid
 * tr > td[colspan=6] structure) or rests hidden in its shipped static
 * slot at the end of the project changes section. Every move goes
 * through insertBefore(node, reference) — the reference is the
 * map-carried next data row or null, never a position or a scan. */

const REVIEW_TABLE_COLUMN_COUNT = 6;
let reviewRowReferences = new Map();
let reviewWrapperRow = null;

function parkContentReviewPanel() {
  const panel = document.getElementById("content-review-panel");
  document.getElementById("project-changes-section").insertBefore(panel, null);
  if (reviewWrapperRow !== null) {
    reviewWrapperRow.remove();
    reviewWrapperRow = null;
  }
  panel.hidden = true;
}

function placeContentReviewPanel() {
  parkContentReviewPanel();
  const contentState = contentViewer.currentState();
  const changesState = projectChangesViewer === null ? null : projectChangesViewer.currentState();
  if (
    contentState === null ||
    contentState.changeIndex === null ||
    changesState === null ||
    changesState.runId !== contentState.runId ||
    changesState.observation === null ||
    changesState.observation !== contentState.observation
  ) {
    return;
  }
  const anchor = reviewRowReferences.get(contentState.changeIndex);
  if (anchor === undefined) {
    return;
  }
  const body = document.getElementById("changes-body");
  const panel = document.getElementById("content-review-panel");
  const wrapperRow = document.createElement("tr");
  const wrapperCell = document.createElement("td");
  wrapperCell.colSpan = REVIEW_TABLE_COLUMN_COUNT;
  wrapperRow.appendChild(wrapperCell);
  wrapperCell.insertBefore(panel, null);
  body.insertBefore(wrapperRow, anchor.insertBefore);
  reviewWrapperRow = wrapperRow;
  panel.hidden = false;
}

/** The side a content Retry click re-requests; set by the render
 * adapter from the viewer state, never persisted anywhere. */
let contentRetrySide = null;

/* ------------------------------------------------------------------ */
/* Project apply (v8): the wiring of the explicit durable operation.   */
/* ------------------------------------------------------------------ */

/** The terminal status of the last applied authoritative inspect of
 * the selected run; the apply eligibility never derives it from
 * anything else. */
let lastInspectedStatus = null;

/** The production recovery store binds the one closed localStorage
 * namespace; an unavailable browser storage refuses every write and
 * therefore every apply. */
const applyRecoveryStore = createApplyRecoveryStore(
  typeof localStorage === "undefined" || localStorage === null ? null : localStorage,
);

const applyTransport = createProjectApplyTransport({
  fetchOperation: (runId, applyId, observation, signal) =>
    requestProjectApplyJson(fetchImplementation, projectApplyRequestPath(runId, applyId), { signal }, {
      runId,
      applyId,
      observation,
    }),
  onStarted: () => {
    document.getElementById("project-apply-check").disabled = true;
  },
  onSettled: () => {
    document.getElementById("project-apply-check").disabled = false;
  },
});

/**
 * The one shared mutation gate admits the apply POST exactly like a
 * run mutation: the same begin/end pair, the same control disabling
 * and the same live-observation pause — an apply can never run
 * parallel to a run mutation and vice versa.
 */
async function submitProjectApplyViaGate(identity) {
  if (!mutationGate.begin()) {
    return { ok: false, lost: false, failure: { layer: "transport", reason: "mutation_busy" } };
  }
  setMutationControlsDisabled(true);
  let mutationObserved = false;
  try {
    inspectTransport.invalidate();
    applySchedulerEffect(scheduler.mutationStarted());
    mutationObserved = true;
    return await requestProjectApplyJson(
      fetchImplementation,
      projectApplyRequestPath(identity.runId, identity.applyId),
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(projectApplyPostBody(identity.observation)),
      },
      { runId: identity.runId, applyId: identity.applyId, observation: identity.observation },
    );
  } finally {
    mutationGate.end();
    setMutationControlsDisabled(false);
    if (mutationObserved) {
      applySchedulerEffect(scheduler.mutationCompleted());
    }
    renderProjectApplyIfSelected();
  }
}

const applyViewer = createProjectApplyViewer({
  recoveryStore: applyRecoveryStore,
  transport: applyTransport,
  submit: submitProjectApplyViaGate,
  onState: (state) => renderProjectApply(state),
  onApplied: (identity) => {
    // The one explicit post-apply Project Changes reload (after=0,
    // no observation): the same reload the Reload button runs — no
    // second changes model, no content auto-fetch, no inspect POST.
    if (selectedRunId === identity.runId && projectChangesViewer !== null) {
      projectChangesViewer.reload();
    }
  },
});

/** The effective capability of the selected run's bound project, read
 * only from the discovery projection: never a project path, apply store
 * root or registry internal. */
function applyCapabilityFor(runId) {
  const authority = authorityFor(runId);
  if (projectsState === null || authority.runEntry === null) {
    return false;
  }
  const project = projectsState.projects.find((entry) => entry.id === authority.runEntry.projectId);
  return project !== undefined && project.projectApply === true;
}

/** The section of the selected run: a run change resets the panel and
 * invalidates the in-flight operation GET while the recovery record
 * stays on disk (the authoritative state is the GET API). */
function ensureProjectApplyFor(runId) {
  const current = applyViewer.currentState();
  if (current !== null && current.runId === runId) {
    document.getElementById("project-apply-section").hidden = false;
    return;
  }
  applyViewer.transitionToRun(runId);
  document.getElementById("project-apply-section").hidden = false;
}

/** The section of the selected run: created once per selection; a run
 * change discards the previous viewer and invalidates its in-flight
 * changes GET. The automatic one-shot initial load follows the applied
 * authoritative terminal inspects (the viewer's `inspectApplied`);
 * Reload and Load more stay explicit user actions. */
function ensureProjectChangesFor(runId) {
  const current = projectChangesViewer === null ? null : projectChangesViewer.currentState();
  if (current !== null && current.runId === runId) {
    document.getElementById("project-changes-section").hidden = false;
    return;
  }
  if (projectChangesViewer === null) {
    projectChangesViewer = createProjectChangesViewer({
      transport: changesTransport,
      onState: (state) => renderProjectChanges(state),
    });
  }
  projectChangesViewer.transitionToRun(runId);
  contentViewer.transitionToRun(runId);
  document.getElementById("project-changes-section").hidden = false;
  resetProjectChangesDom();
}

function resetProjectChangesDom() {
  parkContentReviewPanel();
  reviewRowReferences = new Map();
  document.getElementById("changes-body").replaceChildren();
  document.getElementById("changes-table").hidden = true;
  document.getElementById("changes-empty").textContent = "";
  document.getElementById("changes-status").textContent = "";
  document.getElementById("changes-meta").textContent = "";
  document.getElementById("changes-summary").textContent = "";
  document.getElementById("changes-reload").hidden = true;
  document.getElementById("changes-load-more").hidden = true;
}

/** The read-only metadata text of one baseline/result node; every
 * value renders through `textContent` and the symlink target stays
 * verbatim plain text. */
function changesNodeText(node) {
  if (node === null) {
    return "";
  }
  if (node.kind === "directory") {
    return `directory · mode ${octalMode(node.mode)}`;
  }
  if (node.kind === "symlink") {
    return `symlink → ${node.target}`;
  }
  const group = node.hardlink_group === undefined ? "" : ` · hardlink group ${node.hardlink_group}`;
  return `file · mode ${octalMode(node.mode)} · ${node.byte_count} B · sha256 ${node.sha256}${group}`;
}

function octalMode(mode) {
  return `0${mode.toString(8).padStart(3, "0")}`;
}

const PROJECT_PRESENTATION_LABELS = {
  unavailable: "unavailable",
  no_changes: "no changes",
  pending: "pending",
  already_present: "already present",
  mixed: "mixed",
  diverged: "diverged",
};

function renderProjectChanges(state) {
  if (state === null) {
    return;
  }
  // the content review is bound to the loaded metadata: a changed run
  // or observation discards its loaded bytes and withdraws the wire
  // (the viewer itself never re-fetches anything)
  if (state.observation !== null) {
    contentViewer.metadataApplied(state);
  }
  const meta = document.getElementById("changes-meta");
  const summary = document.getElementById("changes-summary");
  const status = document.getElementById("changes-status");
  const empty = document.getElementById("changes-empty");
  const reload = document.getElementById("changes-reload");
  const loadMore = document.getElementById("changes-load-more");
  const table = document.getElementById("changes-table");
  const body = document.getElementById("changes-body");
  // the review panel is preserved through the rebuild: it is parked
  // (connected, hidden) before the body is emptied and re-placed by
  // the one placement owner after the rows and their insertion
  // references exist again
  parkContentReviewPanel();
  body.replaceChildren();
  const builtRows = [];
  for (const entry of state.changes) {
    const row = document.createElement("tr");
    row.appendChild(element("td", entry.path));
    row.appendChild(element("td", entry.change));
    row.appendChild(element("td", entry.source_relation));
    row.appendChild(element("td", changesNodeText(entry.baseline)));
    row.appendChild(element("td", changesNodeText(entry.result)));
    const actions = document.createElement("td");
    const reviewButton = document.createElement("button");
    reviewButton.type = "button";
    reviewButton.textContent = "Review content";
    reviewButton.addEventListener("click", () => {
      clearMessages();
      contentViewer.review(entry);
    });
    actions.appendChild(reviewButton);
    row.appendChild(actions);
    body.appendChild(row);
    builtRows.push({ index: entry.index, row });
  }
  // the insertion references are resolved only after every data row
  // exists: each entry carries its row and the next data row (null for
  // the last), so the placement owner never scans the body
  reviewRowReferences = new Map();
  for (let position = 0; position < builtRows.length; position++) {
    const current = builtRows[position];
    const next = position + 1 < builtRows.length ? builtRows[position + 1].row : null;
    reviewRowReferences.set(current.index, { row: current.row, insertBefore: next });
  }
  placeContentReviewPanel();
  table.hidden = state.changes.length === 0;
  const presentation = PROJECT_PRESENTATION_LABELS[state.presentationState] ?? state.presentationState;
  const loadedTotal = state.summary === null ? "" : ` · loaded ${state.changes.length} of ${state.summary.total}`;
  meta.textContent = state.inspectRevision === null ? "" : `snapshot revision ${state.inspectRevision} · ${presentation}${loadedTotal}`;
  summary.textContent = state.summary === null
    ? ""
    : `total ${state.summary.total} · added ${state.summary.added} · deleted ${state.summary.deleted} · content ${state.summary.content_modified} · mode ${state.summary.mode_modified} · content+mode ${state.summary.content_and_mode_modified} · symlink target ${state.summary.symlink_target_modified} · type ${state.summary.type_changed} · baseline ${state.summary.matches_baseline} · already present ${state.summary.matches_result} · diverged ${state.summary.diverged}`;
  let statusText = "";
  if (state.requestState === "loading_initial" || state.requestState === "loading_more" || state.requestState === "reloading") {
    statusText = "Loading…";
  } else if (state.requestState === "stale") {
    statusText = "Stale. The run workspace or registered project changed after this view was created. Reload the changes.";
  } else if (state.requestState === "error" && state.error !== null) {
    statusText = `failed (${state.error.layer}: ${state.error.reason})`;
  }
  status.textContent = statusText;
  empty.textContent = state.requestState === "ready" && state.summary !== null && state.summary.total === 0
    ? "The run workspace carries no changes relative to its durable baseline."
    : "";
  // the explicit actions: Reload stays available except after a
  // contract-invalid document (fail closed, no automatic retry);
  // Load more exists only for a ready snapshot with a continuation
  const contractFailed = state.requestState === "error" && state.error !== null && state.error.layer === "contract";
  reload.hidden = contractFailed;
  loadMore.hidden = !(state.requestState === "ready" && state.nextAfter !== null);
  // the apply panel consumes every fully loaded, ready snapshot of the
  // selected run: it drives the one recovery GET per load and the
  // record removal after a confirmed applied operation
  if (state.requestState === "ready" && state.summary !== null && state.nextAfter === null && state.changes.length === state.summary.total) {
    applyViewer.changesSnapshotReady(state);
  }
  renderProjectApplyIfSelected();
}

/** The apply panel re-evaluates whenever its inputs change without an
 * apply state change (a finished mutation releases the gate; a
 * reloaded snapshot changes the eligibility inputs). */
function renderProjectApplyIfSelected() {
  const applyState = applyViewer.currentState();
  if (applyState !== null) {
    renderProjectApply(applyState);
  }
}

const CONTENT_SIDE_PHASE_TEXT = {
  unrequested: "not requested",
  loading: "Loading…",
  implicit_empty: "empty side — no content existed on this side",
  metadata_only: "metadata only — this side has no file content",
  unavailable: "baseline bytes can no longer be proven (the registered project no longer matches the baseline)",
};

/** The read-only status line and bounded view of one side. */
function contentSideStatus(sideState) {
  if (sideState.phase === "available") {
    const total = sideState.byteCount === null ? "?" : String(sideState.byteCount);
    const preview = sideState.capped || sideState.nextOffset !== null ? "truncated preview" : "full preview";
    return `available · ${sideState.loadedBytes} of ${total} B retained · sha256 ${sideState.sha256.slice(0, 12)}… · ${preview}`;
  }
  if (sideState.phase === "error") {
    return `failed (${sideState.error.layer}: ${sideState.error.reason})`;
  }
  return CONTENT_SIDE_PHASE_TEXT[sideState.phase] ?? sideState.phase;
}

/** The bounded presentation of one retained side: decoded text through
 * `textContent`, or the bounded hex view. */
function renderContentSide(statusId, viewId, sideState) {
  const status = document.getElementById(statusId);
  const view = document.getElementById(viewId);
  status.textContent = contentSideStatus(sideState);
  if (sideState.phase !== "available" || sideState.data === null) {
    view.hidden = true;
    view.textContent = "";
    return;
  }
  const decoded = decodePreviewText(sideState.data, { incompleteTail: sideState.nextOffset !== null });
  if (decoded.kind === "text") {
    view.hidden = false;
    view.textContent = decoded.text;
    return;
  }
  view.hidden = false;
  const rows = hexPreviewRows(sideState.data, CONTENT_HEX_DISPLAY_BYTES);
  const lines = rows.map((row) => `${row.offset}  ${row.hex}  |${row.ascii}|`);
  if (sideState.data.length > CONTENT_HEX_DISPLAY_BYTES) {
    lines.push(`(hex preview limited to the first ${CONTENT_HEX_DISPLAY_BYTES} bytes of ${sideState.data.length} retained)`);
  }
  view.textContent = lines.join("\n");
}

const DIFF_REFUSAL_TEXT = {
  not_text: "diff unavailable: at least one side is not UTF-8 text (binary content)",
  too_many_lines: "diff unavailable: the change exceeds the bounded line limit",
  work_budget: "diff unavailable: the change exceeds the bounded diff work budget",
};

/** The thin DOM adapter of the content review viewer: every value
 * renders through `createElement`/`textContent`, the diff rows are
 * plain text, and the panel owns no fetch and no mutation. */
function renderContentReview(state) {
  if (state === null) {
    return;
  }
  const panel = document.getElementById("content-review-panel");
  const meta = document.getElementById("content-review-meta");
  const status = document.getElementById("content-review-status");
  const diffStatus = document.getElementById("content-diff-status");
  const diffView = document.getElementById("content-diff-view");
  const loadBaseline = document.getElementById("content-review-load-baseline");
  const loadResult = document.getElementById("content-review-load-result");
  const retry = document.getElementById("content-review-retry");
  const reloadChanges = document.getElementById("content-review-reload-changes");
  // the placement owner derives the panel's position and visibility
  // from the published viewer states; this render only fills content
  placeContentReviewPanel();
  meta.textContent = state.changeIndex === null
    ? ""
    : `change ${state.changeIndex} · ${state.changePath} · ${state.changeKind} · observation ${state.observation.slice(0, 12)}…`;
  let statusText = "";
  if (state.requestState === "loading") {
    statusText = "Loading…";
  } else if (state.requestState === "stale") {
    statusText = "Stale. The run workspace or registered project changed after this review was created. Reload the Project Changes.";
  }
  status.textContent = statusText;
  renderContentSide("content-baseline-status", "content-baseline-view", state.baseline);
  renderContentSide("content-result-status", "content-result-view", state.result);
  // the bounded diff: only two fully loaded, uncapped UTF-8 sides
  const diffable = (side) => side.phase === "available" && side.nextOffset === null && !side.capped && side.data !== null;
  const emptyable = (side) => side.phase === "implicit_empty";
  const leftOk = diffable(state.baseline) || emptyable(state.baseline);
  const rightOk = diffable(state.result) || emptyable(state.result);
  let diffText = "";
  let diffRows = null;
  if (state.changeIndex === null || state.requestState === "stale") {
    diffStatus.textContent = "";
  } else if (!leftOk || !rightOk) {
    diffStatus.textContent = "diff unavailable: both sides must be loaded text or an explicit empty side";
  } else {
    const left = state.baseline.phase === "implicit_empty" ? new Uint8Array(0) : state.baseline.data;
    const right = state.result.phase === "implicit_empty" ? new Uint8Array(0) : state.result.data;
    const leftDecoded = decodePreviewText(left, { incompleteTail: false });
    const rightDecoded = decodePreviewText(right, { incompleteTail: false });
    if (leftDecoded.kind !== "text" || rightDecoded.kind !== "text") {
      diffStatus.textContent = DIFF_REFUSAL_TEXT.not_text;
    } else {
      const diff = computeBoundedLineDiff(left, right);
      if (diff.kind === "refusal") {
        diffStatus.textContent = DIFF_REFUSAL_TEXT[diff.reason] ?? "diff unavailable";
      } else if (diff.identical) {
        diffStatus.textContent = state.changeKind === "mode_modified"
          ? "Content identical; metadata changed."
          : "No textual differences.";
      } else {
        diffStatus.textContent = "";
        diffRows = diff.rows;
      }
    }
  }
  diffView.hidden = diffRows === null;
  if (diffRows !== null) {
    const lines = diffRows.map((row) => {
      const prefix = row.type === "add" ? "+ " : row.type === "delete" ? "- " : "  ";
      const line = prefix + row.text;
      return row.noEol ? `${line}\n\\ No newline at end of file` : line;
    });
    diffView.textContent = lines.join("\n");
  } else {
    diffView.textContent = "";
  }
  const busy = state.requestState === "loading";
  loadBaseline.hidden = !(state.baseline.phase === "available" && state.baseline.nextOffset !== null && !state.baseline.capped);
  loadResult.hidden = !(state.result.phase === "available" && state.result.nextOffset !== null && !state.result.capped);
  const retryableSide = state.baseline.phase === "error" && state.baseline.error !== null && state.baseline.error.layer === "transport"
    ? "baseline"
    : state.result.phase === "error" && state.result.error !== null && state.result.error.layer === "transport"
      ? "result"
      : null;
  retry.hidden = retryableSide === null;
  contentRetrySide = retryableSide;
  reloadChanges.hidden = !state.stale;
}

/** The labels of the apply panel's closed UI states; the durable
 * operation statuses render under their exact production names. */
const PROJECT_APPLY_STATE_LABELS = {
  idle: "",
  confirming: "Confirmation required.",
  submitting: "Applying…",
  network_unknown: "Outcome unknown. The apply request may have been received; check the status.",
  preparing: "Operation preparing.",
  prepared: "Operation prepared but not started.",
  applying: "Operation applying.",
  applied: "Operation applied.",
  conflict: "Operation stopped on a project conflict.",
  recovery_required: "Operation needs recovery; continue explicitly.",
  failed: "Operation failed.",
  unavailable: "",
};

/** The thin DOM adapter of the apply panel: every value renders
 * through `createElement`/`textContent`, the confirmation panel is an
 * inline block (never `confirm()`), and the Review press never POSTs. */
function renderProjectApply(state) {
  if (state === null) {
    return;
  }
  const section = document.getElementById("project-apply-section");
  const statusLine = document.getElementById("project-apply-status");
  const meta = document.getElementById("project-apply-meta");
  const recordLine = document.getElementById("project-apply-record");
  const confirmation = document.getElementById("project-apply-confirmation");
  const confirmationDetails = document.getElementById("project-apply-confirmation-details");
  const review = document.getElementById("project-apply-review");
  const confirmButton = document.getElementById("project-apply-confirm");
  const cancelButton = document.getElementById("project-apply-cancel");
  const retry = document.getElementById("project-apply-retry");
  const check = document.getElementById("project-apply-check");
  const continueButton = document.getElementById("project-apply-continue");
  section.hidden = false;
  const changesState = projectChangesViewer === null ? null : projectChangesViewer.currentState();
  const eligibility = projectApplyEligibility({
    terminalStatus: lastInspectedStatus,
    capability: applyCapabilityFor(state.runId),
    changesState,
    applyState: state.uiState,
    mutationInFlight: mutationGate.isInFlight(),
  });
  const label = PROJECT_APPLY_STATE_LABELS[state.uiState] ?? state.uiState;
  statusLine.textContent = state.error !== null
    ? `${label} failed (${state.error.layer}: ${state.error.reason})`.trim()
    : label;
  meta.textContent = state.applyId === null
    ? ""
    : `operation ${state.applyId.slice(0, 12)}… · revision ${state.revision === null ? "?" : state.revision}` +
      (state.totalSteps === null ? "" : ` · steps ${state.completedSteps}/${state.totalSteps}`);
  recordLine.textContent = state.recordError !== null
    ? `recovery record problem (${state.recordError}); the apply is blocked until it is resolved.`
    : "";
  // the confirmation panel: rendered from the current snapshot the
  // operator reviewed; the Confirm press re-validates everything
  confirmation.hidden = state.uiState !== "confirming";
  if (state.uiState === "confirming" && changesState !== null && changesState.summary !== null) {
    const summary = changesState.summary;
    const rows = [
      `Run: ${state.runId}`,
      `Snapshot revision: ${changesState.inspectRevision}`,
      `Observation: ${changesState.observation}`,
      `Changes: ${summary.total} total · ${summary.matches_baseline} pending · ${summary.matches_result} already present`,
      "Warning: the registered project will be modified.",
      "Warning: the operation is per-path crash-recoverable but not whole-tree atomic.",
      "Warning: the project is locked cooperatively; external IDE or git writers are not serialized.",
    ];
    confirmationDetails.replaceChildren();
    for (const text of rows) {
      confirmationDetails.appendChild(element("p", text));
    }
  } else {
    confirmationDetails.replaceChildren();
  }
  review.hidden = !eligibility.eligible;
  confirmButton.hidden = state.uiState !== "confirming";
  cancelButton.hidden = state.uiState !== "confirming";
  retry.hidden = !(state.applyId !== null && (state.uiState === "network_unknown" || state.uiState === "unavailable"));
  check.hidden = !(state.applyId !== null && state.uiState !== "submitting" && state.uiState !== "confirming");
  continueButton.hidden = !(
    state.applyId !== null &&
    (state.uiState === "prepared" || state.uiState === "applying" || state.uiState === "recovery_required")
  );
}

function resetWorkerLogsDom() {
  document.getElementById("logs-body").replaceChildren();
  document.getElementById("logs-content").textContent = "";
  document.getElementById("logs-content").hidden = true;
  document.getElementById("logs-status").textContent = "";
  document.getElementById("logs-retry").hidden = true;
  document.getElementById("logs-next-chunk").hidden = true;
  document.getElementById("logs-empty").textContent = "The metadata list is not loaded yet; use Load logs.";
  document.getElementById("logs-table").hidden = true;
}

/** The closed S2 row-action table, derived from the published viewer
 * state only (no DOM-local pending flag, no module flag, no timer):
 * an unselected row stays the selection-only View affordance; a
 * non-loadable selected row never receives a load label; a selected
 * loadable row shows the in-place Load log affordance, disabled
 * `Loading…` while exactly its own content desire owns the published
 * descriptor (a metadata desire requested during the content GET never
 * takes it — the row keeps the pending label through the overlap) and
 * disabled `Loaded` once the artifact is fully read (a press of either
 * disabled form can create no GET, and the viewer refuses both). */
function workerLogRowAction(entry, state) {
  if (state.selectedExecutionIndex !== entry.executionIndex) {
    return { label: "View", disabled: false };
  }
  if (entry.status !== "captured" && entry.status !== "truncated") {
    return { label: "View", disabled: false };
  }
  const content = state.content;
  if (content !== null && content.executionIndex === entry.executionIndex && content.nextOffset === null) {
    return { label: "Loaded", disabled: true };
  }
  if (state.requestKind === "content" && state.requestExecutionIndex === entry.executionIndex) {
    return { label: "Loading…", disabled: true };
  }
  return { label: "Load log", disabled: false };
}

/** One metadata row: the execution identity, the status and, for a
 * captured/truncated execution, the confirmed byte count; the closed
 * row-action affordance reads the published viewer state. */
function workerLogRow(entry, state) {
  const row = document.createElement("tr");
  row.appendChild(element("td", String(entry.executionIndex)));
  row.appendChild(element("td", entry.stateId));
  row.appendChild(element("td", entry.status));
  row.appendChild(element("td", entry.byteCount === undefined ? "" : String(entry.byteCount)));
  row.appendChild(element("td", entry.truncatedDaemon === true || entry.truncatedLocal === true ? "yes" : ""));
  const action = workerLogRowAction(entry, state);
  const actions = document.createElement("td");
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = action.label;
  button.disabled = action.disabled;
  button.addEventListener("click", () => {
    clearMessages();
    if (workerLogViewer === null) {
      return;
    }
    if (state.selectedExecutionIndex === entry.executionIndex) {
      // the in-place Load-log affordance (and the disabled Loaded form:
      // the viewer refuses the fully-read artifact without a GET)
      workerLogViewer.fetchChunk();
    } else {
      workerLogViewer.selectExecution(entry.executionIndex);
    }
  });
  actions.appendChild(button);
  row.appendChild(actions);
  return row;
}

/** The read-only status explanation of one selected execution. */
function workerLogStatusText(status) {
  if (status === "captured") {
    return "Captured artifact; upstream completeness is not guaranteed";
  }
  if (status === "truncated") {
    return "Captured tail; earlier bytes were discarded";
  }
  if (status === "unavailable") {
    return "content was not durably captured";
  }
  return "execution has no terminal durable log binding yet";
}

function renderWorkerLogs(state) {
  if (state === null) {
    return;
  }
  const body = document.getElementById("logs-body");
  body.replaceChildren();
  for (const entry of state.logs) {
    body.appendChild(workerLogRow(entry, state));
  }
  document.getElementById("logs-table").hidden = state.logs.length === 0;
  const empty = document.getElementById("logs-empty");
  const showHint = state.requestState === "idle" || (state.requestState === "ready" && state.logs.length === 0);
  empty.hidden = !showHint;
  empty.textContent = showHint
    ? state.requestState === "ready"
      ? "This run has no agent executions with worker logs."
      : "The metadata list is not loaded yet; use Load logs."
    : "";
  const status = document.getElementById("logs-status");
  let text = "";
  if (state.requestState === "loading") {
    text = "Loading…";
  } else if (state.requestState === "failed" && state.error !== null) {
    text = `failed (${state.error.layer}: ${state.error.reason})`;
  } else if (state.snapshotRevision !== null) {
    text = `snapshot revision ${state.snapshotRevision}`;
  }
  status.textContent = text;
  // the read-only status explanation of the selected execution
  const selected = state.selectedExecutionIndex === null
    ? null
    : state.logs.find((item) => item.executionIndex === state.selectedExecutionIndex) ?? null;
  document.getElementById("logs-hint").textContent = selected === null ? "" : workerLogStatusText(selected.status);
  const retry = document.getElementById("logs-retry");
  retry.hidden = !(state.requestState === "failed" && state.error !== null && state.error.layer !== "viewer");
  // the content frame: only a validated captured/truncated chunk renders
  const content = state.content;
  const pre = document.getElementById("logs-content");
  if (content === null) {
    pre.hidden = true;
    pre.textContent = "";
  } else {
    pre.hidden = false;
    // safe render: the incrementally decoded plain text via textContent,
    // never markup; the scroll position survives every refresh
    const previousScroll = typeof pre.scrollTop === "number" ? pre.scrollTop : 0;
    pre.textContent = content.text;
    if (previousScroll > 0) {
      pre.scrollTop = previousScroll;
    }
    const cursor = content.nextOffset === null
      ? `bytes ${content.offset}–${content.offset + (content.byteCount - content.offset)} (end)`
      : `bytes ${content.offset}–${content.nextOffset} of ${content.byteCount}`;
    document.getElementById("logs-cursor").textContent =
      `execution ${state.selectedExecutionIndex} · ${content.status} · sha256 ${content.sha256.slice(0, 12)}… · ${cursor}` +
      (content.truncatedDaemon || content.truncatedLocal ? " · truncated" : "");
  }
  // the explicit Load log action: any boundary may read a durable log
  const next = document.getElementById("logs-next-chunk");
  const loadable = selected !== null
    && (selected.status === "captured" || selected.status === "truncated")
    && (content === null || content.nextOffset !== null);
  next.hidden = !loadable;
}

function setStatus(text) {
  document.getElementById("status").textContent = text;
}

function showError(text) {
  document.getElementById("error").textContent = text;
}

function clearMessages() {
  setStatus("");
  showError("");
}

let inputRowCounter = 0;

function addInputRow() {
  inputRowCounter += 1;
  const row = document.createElement("div");
  row.className = "input-row";
  row.appendChild(labeledControl("Input id", textInput(`input-row-${inputRowCounter}-id`, "input-id")));
  row.appendChild(labeledControl("Input root", textInput(`input-row-${inputRowCounter}-root`, "input-root")));
  row.appendChild(labeledControl("Input ref", textInput(`input-row-${inputRowCounter}-ref`, "input-ref")));
  const remove = document.createElement("button");
  remove.type = "button";
  remove.textContent = "Remove";
  remove.addEventListener("click", () => {
    row.remove();
  });
  row.appendChild(remove);
  return row;
}

function boot() {
  document.getElementById("refresh-projects").addEventListener("click", () => {
    clearMessages();
    setStatus("Loading projects…");
    void loadProjects();
  });
  document.getElementById("refresh-runs").addEventListener("click", () => {
    clearMessages();
    void refreshRuns(true);
  });
  document.getElementById("load-more").addEventListener("click", () => {
    clearMessages();
    void loadMoreRuns();
  });
  document.getElementById("inspect-selected").addEventListener("click", () => {
    if (document.getElementById("detail-run-id").textContent.startsWith("Run ")) {
      clearMessages();
      void inspectRun(document.getElementById("detail-run-id").textContent.slice(4));
    }
  });
  const liveToggle = document.getElementById("live-updates");
  liveToggle.addEventListener("change", () => {
    applySchedulerEffect(scheduler.liveToggleChanged(liveToggle.checked));
    setStatus(liveToggle.checked ? "Live updates enabled." : "Live updates disabled.");
  });
  document.addEventListener("visibilitychange", () => {
    applySchedulerEffect(scheduler.visibilityChanged(document.visibilityState !== "hidden"));
  });
  document.getElementById("generate-id").addEventListener("click", () => {
    if (typeof crypto.randomUUID === "function") {
      document.getElementById("new-run-id").value = crypto.randomUUID();
      return;
    }
    showError("contract: randomuuid_unavailable");
  });
  document.getElementById("add-input").addEventListener("click", () => {
    document.getElementById("input-rows").appendChild(addInputRow());
  });
  document.getElementById("new-run-form").addEventListener("submit", (event) => {
    void submitNewRun(event);
  });
  document.getElementById("input-rows").appendChild(addInputRow());
  document.getElementById("events-load-more").addEventListener("click", () => {
    loadMoreEvents();
  });
  document.getElementById("events-reload").addEventListener("click", () => {
    void reloadTimeline();
  });
  document.getElementById("events-retry").addEventListener("click", () => {
    retryTimeline();
  });
  document.getElementById("lh-identity-load-more").addEventListener("click", () => {
    clearMessages();
    if (lifecycleHistoryViewer !== null) {
      lifecycleHistoryViewer.loadMoreIdentity();
    }
  });
  document.getElementById("lh-bindings-load-more").addEventListener("click", () => {
    clearMessages();
    if (lifecycleHistoryViewer !== null) {
      lifecycleHistoryViewer.loadMoreBindings();
    }
  });
  document.getElementById("lh-reload").addEventListener("click", () => {
    clearMessages();
    if (lifecycleHistoryViewer !== null) {
      lifecycleHistoryViewer.reload();
    }
  });
  document.getElementById("pt-identity-load-more").addEventListener("click", () => {
    clearMessages();
    if (planTasksViewer !== null) {
      planTasksViewer.loadMoreIdentity();
    }
  });
  document.getElementById("pt-reload").addEventListener("click", () => {
    clearMessages();
    if (planTasksViewer !== null) {
      planTasksViewer.reload();
    }
    renderPlanTasks(planTasksViewer === null ? null : planTasksViewer.currentState());
  });
  document.getElementById("pt-task-load-more").addEventListener("click", () => {
    clearMessages();
    if (planTasksViewer !== null) {
      planTasksViewer.loadMoreTaskBody();
    }
    renderPlanTasks(planTasksViewer === null ? null : planTasksViewer.currentState());
  });
  document.getElementById("pt-task-retry").addEventListener("click", () => {
    clearMessages();
    if (planTasksViewer !== null) {
      planTasksViewer.retryTaskBody();
    }
    renderPlanTasks(planTasksViewer === null ? null : planTasksViewer.currentState());
  });
  document.getElementById("pt-plan-load-more").addEventListener("click", () => {
    clearMessages();
    if (planTasksViewer !== null) {
      planTasksViewer.loadMorePlanPage();
    }
    renderPlanTasks(planTasksViewer === null ? null : planTasksViewer.currentState());
  });
  document.getElementById("pt-plan-retry").addEventListener("click", () => {
    clearMessages();
    if (planTasksViewer !== null) {
      planTasksViewer.retryPlanContent();
    }
    renderPlanTasks(planTasksViewer === null ? null : planTasksViewer.currentState());
  });
  document.getElementById("lh-compare-apply").addEventListener("click", () => {
    const valueA = document.getElementById("lh-compare-a").value;
    const valueB = document.getElementById("lh-compare-b").value;
    lhCompareSelection = {
      a: valueA === "" ? null : valueA,
      b: valueB === "" ? null : valueB,
    };
    if (lifecycleHistoryViewer !== null) {
      renderLifecycleHistory(lifecycleHistoryViewer.currentState());
    }
  });
  document.getElementById("logs-load").addEventListener("click", () => {
    clearMessages();
    if (workerLogViewer !== null) {
      workerLogViewer.fetchLogsList();
    }
  });
  document.getElementById("logs-next-chunk").addEventListener("click", () => {
    clearMessages();
    if (workerLogViewer !== null) {
      workerLogViewer.fetchChunk();
    }
  });
  document.getElementById("logs-retry").addEventListener("click", () => {
    clearMessages();
    if (workerLogViewer !== null) {
      workerLogViewer.retryLastChunkRequest();
    }
  });
  document.getElementById("changes-reload").addEventListener("click", () => {
    clearMessages();
    if (projectChangesViewer !== null) {
      projectChangesViewer.reload();
    }
  });
  document.getElementById("changes-load-more").addEventListener("click", () => {
    clearMessages();
    if (projectChangesViewer !== null) {
      projectChangesViewer.loadMore();
    }
  });
  document.getElementById("content-review-close").addEventListener("click", () => {
    clearMessages();
    contentViewer.closeReview();
  });
  document.getElementById("content-review-load-baseline").addEventListener("click", () => {
    clearMessages();
    contentViewer.loadMoreSide("baseline");
  });
  document.getElementById("content-review-load-result").addEventListener("click", () => {
    clearMessages();
    contentViewer.loadMoreSide("result");
  });
  document.getElementById("content-review-retry").addEventListener("click", () => {
    clearMessages();
    if (contentRetrySide !== null) {
      contentViewer.retrySide(contentRetrySide);
    }
  });
  document.getElementById("content-review-reload-changes").addEventListener("click", () => {
    clearMessages();
    if (projectChangesViewer !== null) {
      projectChangesViewer.reload();
    }
  });
  document.getElementById("project-apply-review").addEventListener("click", () => {
    clearMessages();
    applyViewer.review();
  });
  document.getElementById("project-apply-cancel").addEventListener("click", () => {
    clearMessages();
    applyViewer.cancel();
  });
  document.getElementById("project-apply-confirm").addEventListener("click", () => {
    clearMessages();
    const changesState = projectChangesViewer === null ? null : projectChangesViewer.currentState();
    const observation = changesState === null ? null : changesState.observation;
    if (typeof observation !== "string") {
      showError("contract: invalid_request");
      return;
    }
    // the confirm press re-validates every changes-derived eligibility
    // condition against the snapshot being applied (the operation
    // state is still the pre-operation confirming panel)
    const eligibility = projectApplyEligibility({
      terminalStatus: lastInspectedStatus,
      capability: applyCapabilityFor(applyViewer.currentState()?.runId ?? null),
      changesState,
      applyState: "idle",
      mutationInFlight: mutationGate.isInFlight(),
    });
    if (!eligibility.eligible) {
      showError(`contract: ${eligibility.reason}`);
      return;
    }
    applyViewer.confirm(observation);
  });
  document.getElementById("project-apply-retry").addEventListener("click", () => {
    clearMessages();
    applyViewer.retry();
  });
  document.getElementById("project-apply-check").addEventListener("click", () => {
    clearMessages();
    applyViewer.checkStatus();
  });
  document.getElementById("project-apply-continue").addEventListener("click", () => {
    clearMessages();
    applyViewer.continueRecovery();
  });
  clearMessages();
}

if (typeof document !== "undefined") {
  boot();
}
