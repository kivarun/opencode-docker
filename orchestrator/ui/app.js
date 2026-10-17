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
 * origins, no browser storage, no workers, no websockets); failures are
 * shown as `layer: reason` strings only. The fresh-run POST is never
 * retried automatically: a lost response is recovered exclusively
 * through the safe GET inspect of the caller-owned run id, which is
 * created and displayed before the request is sent.
 */

const SCHEMA_VERSION = 1;
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
    expectShape(entry, ["id", "input_roots"]);
    const inputRootIds = expectArray(entry, "input_roots").map((root) => {
      if (!isRecord(root)) {
        throw new UiContractError("invalid_shape");
      }
      expectShape(root, ["id"]);
      return expectString(root, "id");
    });
    return Object.freeze({ id: expectString(entry, "id"), inputRootIds: Object.freeze(inputRootIds) });
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
  if (document["schema_version"] !== SCHEMA_VERSION) {
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
  for (const action of actions) {
    if (!isRecord(action) || typeof action["id"] !== "string" || action["id"] === "" || typeof action["to"] !== "string" || action["to"] === "") {
      return null;
    }
    if (!RESERVED_WAIT_ACTIONS.includes(action["id"])) {
      return unusableBoundary(runId, "unknown_action");
    }
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
      return Object.freeze({ kind: "continue_stage", runId, waitIndex });
    }
    return Object.freeze(reviseTaskControl(runId, waitIndex, projectsModel, runEntry));
  });
  return Object.freeze({ kind: "waiting", runId, controls: Object.freeze(controls) });
}

function reviseTaskControl(runId, waitIndex, projectsModel, runEntry) {
  const blocked = (cause) => ({ kind: "revise_task_blocked", runId, waitIndex, cause });
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
  return { kind: "revise_task", runId, waitIndex, projectId, inputRootIds: project["inputRootIds"] };
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
    request(desire) {
      const runId = isRecord(desire) && isSafeId(desire["runId"]) ? desire["runId"] : null;
      const path = isRecord(desire) && typeof desire["path"] === "string" ? desire["path"] : null;
      const onApplied = isRecord(desire) && typeof desire["onApplied"] === "function" ? desire["onApplied"] : null;
      let resolveResult = null;
      const settled = new Promise((resolve) => {
        resolveResult = resolve;
      });
      if (disposed || runId === null || path === null || onApplied === null) {
        resolveResult(undefined);
        return settled;
      }
      const request = { runId, path, onApplied, superseded: false, controller: null, resolve: resolveResult };
      if (active === null) {
        startRequest(request);
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

/** The empty logs state of one run selection. */
export function createWorkerLogsState(runId) {
  return Object.freeze({
    runId,
    snapshotRevision: null,
    logs: [],
    selectedExecutionIndex: null,
    content: null,
    requestState: "idle",
    error: null,
  });
}

/**
 * The closed viewer state machine of the Worker logs section. It owns
 * no fetch and no URL: `fetchLogsList`/`fetchChunk` hand one prepared
 * desire to the single-flight logs transport, and every transition
 * accepts only an already validated logs document of the same run. The
 * list loads only on an explicit button press; a chunk loads only on an
 * explicit `Load next chunk` press, and only for a captured/truncated
 * execution while the authoritative inspect boundary shows the run as
 * running/plan-ready/waiting and no mutating POST is in flight — a
 * settled run's captured payloads are never rendered here.
 */
export function createWorkerLogViewer(io) {
  if (
    !isRecord(io) ||
    typeof io["transport"] !== "object" ||
    typeof io["onState"] !== "function" ||
    typeof io["snapshot"] !== "function" ||
    typeof io["mutatingInFlight"] !== "function"
  ) {
    throw new UiContractError("invalid_viewer_io");
  }
  const transport = io["transport"];
  const onState = io["onState"];
  const snapshot = io["snapshot"];
  const mutatingInFlight = io["mutatingInFlight"];
  let state = null;
  let lastChunkDesire = null;

  const publish = () => {
    onState(state);
  };

  const enter = (runId) => {
    transport.invalidate();
    state = createWorkerLogsState(runId);
    lastChunkDesire = null;
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

  const applyFailure = (result) => {
    if (result === undefined) {
      // a superseded or disposed request: no state change
      return;
    }
    if (result.ok) {
      return;
    }
    state = Object.freeze({
      ...state,
      requestState: "failed",
      error: Object.freeze({ layer: result.failure.layer, reason: result.failure.reason }),
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
    },
    /**
     * The explicit metadata load: one bounded GET of the list endpoint.
     * Never automatic.
     */
    fetchLogsList() {
      if (state === null || state.requestState === "loading") {
        return;
      }
      state = Object.freeze({ ...state, requestState: "loading", error: null });
      publish();
      const requestedRunId = state.runId;
      transport.request({
        runId: state.runId,
        path: workerLogsRequestPath(state.runId, null, 0, 0),
        onApplied: (result) => {
          if (state === null || state.runId !== requestedRunId) {
            return;
          }
          if (result === undefined) {
            return;
          }
          if (!result.ok) {
            applyFailure(result);
            return;
          }
          const model = result.model;
          if (model.kind !== "worker_logs") {
            applyFailure(endpointFailure(model));
            return;
          }
          if (model.runId !== state.runId) {
            applyFailure({ ok: false, failure: { layer: "contract", reason: "unexpected_kind" } });
            return;
          }
          state = Object.freeze({
            ...state,
            snapshotRevision: model.revision,
            logs: model.logs,
            selectedExecutionIndex: null,
            content: null,
            requestState: "ready",
            error: null,
          });
          publish();
        },
      });
    },
    /** One execution selection: content stays until a chunk is loaded. */
    selectExecution(index) {
      if (state === null || !Number.isSafeInteger(index) || index < 1) {
        return;
      }
      const entry = state.logs.find((item) => item.executionIndex === index) ?? null;
      if (entry === null) {
        return;
      }
      state = Object.freeze({ ...state, selectedExecutionIndex: index, content: null, error: null });
      publish();
    },
    /**
     * The explicit next-chunk load of the selected execution. The
     * capture gate: only while the authoritative inspect boundary shows
     * the run as running/plan-ready/waiting and no mutating POST is in
     * flight; only a captured/truncated execution carries content.
     */
    fetchChunk() {
      if (state === null || state.selectedExecutionIndex === null || state.requestState === "loading") {
        return;
      }
      const entry = state.logs.find((item) => item.executionIndex === state.selectedExecutionIndex) ?? null;
      if (entry === null || (entry.status !== "captured" && entry.status !== "truncated")) {
        return;
      }
      const boundaryKind = snapshot().boundaryKind;
      if (boundaryKind !== "in_flight" && boundaryKind !== "plan_ready" && boundaryKind !== "waiting") {
        state = Object.freeze({
          ...state,
          requestState: "failed",
          error: Object.freeze({ layer: "viewer", reason: "capture_gate_refused" }),
        });
        publish();
        return;
      }
      if (mutatingInFlight()) {
        return;
      }
      const offset = state.content === null ? 0 : state.content.nextOffset === null ? null : state.content.nextOffset;
      if (offset === null) {
        return;
      }
      const executionIndex = state.selectedExecutionIndex;
      state = Object.freeze({ ...state, requestState: "loading", error: null });
      publish();
      const desire = { executionIndex, offset };
      const requestedRunId = state.runId;
      lastChunkDesire = desire;
      transport.request({
        runId: state.runId,
        path: workerLogsRequestPath(state.runId, executionIndex, offset, 65536),
        onApplied: (result) => {
          if (state === null || state.runId !== requestedRunId) {
            return;
          }
          if (result === undefined) {
            return;
          }
          if (!result.ok) {
            applyFailure(result);
            return;
          }
          const model = result.model;
          if (model.kind !== "worker_log" || model.runId !== state.runId || model.executionIndex !== desire.executionIndex) {
            applyFailure(model.kind === "worker_log" || model.kind === "error" || model.kind === "command_result"
              ? { ok: false, failure: { layer: "contract", reason: "unexpected_kind" } }
              : endpointFailure(model));
            return;
          }
          if (model.status !== "captured" && model.status !== "truncated") {
            applyFailure({ ok: false, failure: { layer: "contract", reason: "unexpected_kind" } });
            return;
          }
          lastChunkDesire = null;
          state = Object.freeze({
            ...state,
            snapshotRevision: model.revision,
            content: Object.freeze({
              status: model.status,
              sha256: model.sha256,
              byteCount: model.byteCount,
              truncatedDaemon: model.truncatedDaemon,
              truncatedLocal: model.truncatedLocal,
              offset: model.offset,
              dataBase64: model.dataBase64,
              nextOffset: model.nextOffset,
            }),
            requestState: "ready",
            error: null,
          });
          publish();
        },
      });
    },
    /** One retry click: exactly one re-request of the last failed chunk desire. */
    retryLastChunkRequest() {
      if (state === null || state.requestState !== "failed" || lastChunkDesire === null) {
        return;
      }
      const retryOffset = lastChunkDesire.offset;
      const retryExecutionIndex = lastChunkDesire.executionIndex;
      state = Object.freeze({ ...state, requestState: "loading", error: null });
      publish();
      const requestedRunId = state.runId;
      transport.request({
        runId: state.runId,
        path: workerLogsRequestPath(state.runId, lastChunkDesire.executionIndex, retryOffset, 65536),
        onApplied: (result) => {
          if (state === null || state.runId !== requestedRunId) {
            return;
          }
          if (result === undefined) {
            return;
          }
          if (!result.ok) {
            applyFailure(result);
            return;
          }
          const model = result.model;
          if (model.kind !== "worker_log" || model.runId !== state.runId || model.executionIndex !== retryExecutionIndex || model.offset !== retryOffset) {
            applyFailure(model.kind === "worker_log" || model.kind === "error" || model.kind === "command_result"
              ? { ok: false, failure: { layer: "contract", reason: "unexpected_kind" } }
              : endpointFailure(model));
            return;
          }
          lastChunkDesire = null;
          state = Object.freeze({
            ...state,
            snapshotRevision: model.revision,
            content: Object.freeze({
              status: model.status,
              sha256: model.sha256,
              byteCount: model.byteCount,
              truncatedDaemon: model.truncatedDaemon,
              truncatedLocal: model.truncatedLocal,
              offset: model.offset,
              dataBase64: model.dataBase64,
              nextOffset: model.nextOffset,
            }),
            requestState: "ready",
            error: null,
          });
          publish();
        },
      });
    },
    /** The read-only projection for the DOM adapter. */
    currentState() {
      return state;
    },
  });
}

/**
 * The safe render contract of one bounded chunk: the literal base64 is
 * decoded and rendered as plain text through `textContent` only. NUL
 * bytes, ANSI escapes and invalid UTF-8 sequences can never become
 * markup, and the payload is never logged, announced or embedded in a
 * diagnostic. The rendered text of a chunk that is not a validated
 * `worker_log` document is a contract error, never a fallback.
 */
export function decodeLogChunkText(dataBase64) {
  const bytes = Uint8Array.from(atob(dataBase64), (char) => char.charCodeAt(0));
  return new TextDecoder("utf-8", { fatal: false }).decode(bytes);
}

export function logsCaptureGate(state, boundaryKind, mutatingInFlight) {
  if (state === null || state.selectedExecutionIndex === null) {
    return false;
  }
  const entry = state.logs.find((item) => item.executionIndex === state.selectedExecutionIndex) ?? null;
  if (entry === null || (entry.status !== "captured" && entry.status !== "truncated")) {
    return false;
  }
  if (state.content !== null && state.content.nextOffset === null) {
    return false;
  }
  const gate = boundaryKind === "in_flight" || boundaryKind === "plan_ready" || boundaryKind === "waiting";
  return gate && !mutatingInFlight();
}

/* ------------------------------------------------------------------ */
/* Live observation (v3): bounded read-only polling of one selected    */
/* run while the authoritative inspect shows `running` + `in_flight`.  */
/* ------------------------------------------------------------------ */

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
}

function renderRuns(state) {
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
 * invalidates its in-flight events GET. */
function ensureTimelineFor(runId) {
  if (timeline !== null && timeline.runId === runId) {
    document.getElementById("events-section").hidden = false;
    return;
  }
  eventsTransport.invalidate();
  timeline = createTimelineState(runId);
  lastInspectRevision = null;
  document.getElementById("events-section").hidden = false;
  resetTimelineDom();
  renderTimelineMeta();
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
  if (changed) {
    renderRunDetail(model);
    renderRunControls(buildRunActionModel(model, authorityFor(model.runId)));
    if (mutationGate.isInFlight()) {
      setMutationControlsDisabled(true);
    }
  }
  refreshSequence += 1;
  updateLiveStateLine();
  ensureTimelineFor(model.runId);
  ensureWorkerLogsFor(model.runId);
  lastInspectRevision = model.revision;
  lastBoundaryKind = model.boundary.kind;
  renderTimelineMeta();
  maybeTailEvents(model);
  renderWorkerLogs(workerLogViewer === null ? null : workerLogViewer.currentState());
  return changed;
}

async function loadProjects() {
  const result = expectDiscoveryModel(await requestApiJson(fetchImplementation, "/v1/projects"), "projects");
  if (result.ok && result.model.kind === "projects") {
    renderProjects(result.model);
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
  applySchedulerEffect(scheduler.takeOverInspect(runId));
  document.getElementById("detail-run-id").textContent = `Run ${runId}`;
  document.getElementById("inspect-selected").hidden = false;
  document.getElementById("run-detail").hidden = true;
  document.getElementById("outputs-table").hidden = true;
  document.getElementById("run-controls").replaceChildren();
  ensureTimelineFor(runId);
  requestTimelinePage(runId, 0);
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
async function submitAction(action, readForm, describe) {
  if (!mutationGate.begin()) {
    return;
  }
  setMutationControlsDisabled(true);
  let mutationObserved = false;
  try {
    let request;
    try {
      request = buildMutationRequest(action, readForm());
    } catch (error) {
      if (error instanceof UiContractError) {
        showError(`contract: ${error.reason}`);
        return;
      }
      throw error;
    }
    inspectTransport.invalidate();
    applySchedulerEffect(scheduler.mutationStarted());
    mutationObserved = true;
    setStatus(`Sending ${describe} for run ${action.runId}…`);
    await postMutation(request, {
      runId: action.runId,
      handlers: {
        onCommandResult: async (model) => {
          setStatus(describeCommandResult(model));
          await refreshRuns(false);
        },
        onLost: () => {
          setStatus(`outcome unknown; the ${describe} request for run ${action.runId} may have been received; checking once…`);
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
    setMutationControlsDisabled(false);
    if (mutationObserved) {
      applySchedulerEffect(scheduler.mutationCompleted());
    }
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
    void submitAction(control, () => ({
      additionalIterations: iterationsField.querySelector("input").value,
    }), "continue-stage");
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
    void submitAction(control, () => ({
      taskId: taskField.querySelector("input").value,
      rootId: rootSelect.value,
      ref: refField.querySelector("input").value,
    }), "revise-task");
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
      } else {
        container.appendChild(blockedReviseControl(control));
      }
    }
  }
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
let lastBoundaryKind = null;
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
 * logs GET. The list is never loaded automatically. */
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
      snapshot: () => ({ boundaryKind: lastBoundaryKind }),
      mutatingInFlight: () => mutationGate.isInFlight(),
    });
  }
  workerLogViewer.transitionToLogsView(runId);
  document.getElementById("worker-logs-section").hidden = false;
  resetWorkerLogsDom();
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

/** One metadata row: the execution identity, the status and, for a
 * captured/truncated execution, the confirmed byte count. */
function workerLogRow(entry) {
  const row = document.createElement("tr");
  row.appendChild(element("td", String(entry.executionIndex)));
  row.appendChild(element("td", entry.stateId));
  row.appendChild(element("td", entry.status));
  row.appendChild(element("td", entry.byteCount === undefined ? "" : String(entry.byteCount)));
  row.appendChild(element("td", entry.truncatedDaemon === true || entry.truncatedLocal === true ? "yes" : ""));
  const actions = document.createElement("td");
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = "View";
  button.addEventListener("click", () => {
    clearMessages();
    if (workerLogViewer !== null) {
      workerLogViewer.selectExecution(entry.executionIndex);
    }
  });
  actions.appendChild(button);
  row.appendChild(actions);
  return row;
}

function renderWorkerLogs(state) {
  if (state === null) {
    return;
  }
  const body = document.getElementById("logs-body");
  body.replaceChildren();
  for (const entry of state.logs) {
    body.appendChild(workerLogRow(entry));
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
    // safe render: plain text via textContent, never markup
    pre.textContent = decodeLogChunkText(content.dataBase64);
    const cursor = content.nextOffset === null
      ? `bytes ${content.offset}–${content.offset + (content.byteCount - content.offset)} (end)`
      : `bytes ${content.offset}–${content.nextOffset} of ${content.byteCount}`;
    document.getElementById("logs-cursor").textContent =
      `execution ${state.selectedExecutionIndex} · ${content.status} · sha256 ${content.sha256.slice(0, 12)}… · ${cursor}` +
      (content.truncatedDaemon || content.truncatedLocal ? " · truncated" : "");
  }
  const next = document.getElementById("logs-next-chunk");
  const gate = logsCaptureGate(
    state,
    lastBoundaryKind,
    () => mutationGate.isInFlight(),
  );
  next.hidden = !gate;
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
  clearMessages();
}

if (typeof document !== "undefined") {
  boot();
}
