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
 * The one browser API client owner. It checks the HTTP content type,
 * reads the body bounded, requires schema_version 1, classifies the
 * document only by `kind`, and returns the built UI model — the raw
 * response is never retained. A mutating request whose delivery is
 * unknown (network failure after send) is reported as `lost`, which the
 * caller must treat as outcome-unknown and recover through inspect only.
 */
export async function requestApiJson(fetchImpl, path, init) {
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
    return Object.freeze({ ok: true, lost: false, model: parseApiDocument(document) });
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
  clearMessages();
}

if (typeof document !== "undefined") {
  boot();
}
