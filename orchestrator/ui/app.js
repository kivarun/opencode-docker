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
/* Thin DOM adapter: createElement/textContent only.                   */
/* ------------------------------------------------------------------ */

function element(tag, text) {
  const node = document.createElement(tag);
  if (text !== undefined) {
    node.textContent = text;
  }
  return node;
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

function renderProjects(model) {
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
      void inspectRun(run.runId, "manual");
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

async function inspectRun(runId, mode) {
  if (mode !== "after_command") {
    setStatus(mode === "manual" ? `Inspecting run ${runId}…` : `Checking run ${runId}…`);
  }
  document.getElementById("detail-run-id").textContent = `Run ${runId}`;
  document.getElementById("inspect-selected").hidden = false;
  document.getElementById("run-detail").hidden = true;
  document.getElementById("outputs-table").hidden = true;
  const result = await requestApiJson(fetchImplementation, `/v1/runs/${runId}`);
  if (result.ok && result.model.kind === "run") {
    renderRunDetail(result.model);
    if (mode !== "after_command") {
      setStatus(mode === "after_lost" ? `Run ${runId} found after the lost create request.` : `Run ${runId} inspected.`);
    }
    return;
  }
  if (result.ok && result.model.kind === "command_result") {
    setStatus(`${describeCommandResult(result.model)}; use Refresh or Inspect to retry.`);
    return;
  }
  if (result.ok && result.model.kind === "error") {
    showError(formatFailure(result.model));
  } else if (result.ok) {
    showError(`contract: unexpected ${result.model.kind} document`);
  } else {
    showError(formatFailure(result.failure));
  }
  if (mode !== "after_command") {
    setStatus(
      mode === "after_lost"
        ? `outcome unknown; run ${runId} not found yet — use Refresh or Inspect.`
        : `Run ${runId} could not be inspected; use Refresh or Inspect to retry.`,
    );
  }
}

async function refreshRuns(announce) {
  const result = await requestApiJson(fetchImplementation, runsPagePath(null));
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
  const after = runsState.nextAfter;
  const result = await requestApiJson(fetchImplementation, runsPagePath(after));
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

async function recoverFromLostResponse(runId) {
  const recovery = lostResponseRecovery(runId);
  setStatus(`outcome unknown; the create request for run ${recovery.runId} may have been received; checking once…`);
  await inspectRun(recovery.runId, "after_lost");
}

async function submitNewRun(event) {
  event.preventDefault();
  if (submitInFlight) {
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
  submitInFlight = true;
  submit.disabled = true;
  try {
    setStatus(`Creating run ${runId}…`);
    const result = await requestApiJson(fetchImplementation, freshRunPath(projectId), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (result.ok && result.model.kind === "command_result") {
      setStatus(describeCommandResult(result.model));
      await refreshRuns(false);
      await inspectRun(result.model.runId, "after_command");
      return;
    }
    if (result.lost) {
      await recoverFromLostResponse(runId);
      return;
    }
    showResultFailure(result);
  } finally {
    submitInFlight = false;
    submit.disabled = false;
  }
}

let runsState = createRunsState();
let submitInFlight = false;
const fetchImplementation = typeof fetch === "function" ? fetch : null;

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

function labeledInput(inputId, labelText, className) {
  const label = document.createElement("label");
  label.htmlFor = inputId;
  label.textContent = labelText;
  const input = document.createElement("input");
  input.id = inputId;
  input.type = "text";
  input.className = className;
  input.autocomplete = "off";
  input.spellcheck = false;
  label.appendChild(input);
  return label;
}

let inputRowCounter = 0;

function addInputRow() {
  inputRowCounter += 1;
  const row = document.createElement("div");
  row.className = "input-row";
  row.appendChild(labeledInput(`input-row-${inputRowCounter}-id`, "Input id", "input-id"));
  row.appendChild(labeledInput(`input-row-${inputRowCounter}-root`, "Input root", "input-root"));
  row.appendChild(labeledInput(`input-row-${inputRowCounter}-ref`, "Input ref", "input-ref"));
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
    void (async () => {
      const result = await requestApiJson(fetchImplementation, "/v1/projects");
      if (result.ok && result.model.kind === "projects") {
        renderProjects(result.model);
        setStatus("Projects loaded.");
        return;
      }
      showResultFailure(result);
    })();
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
      void inspectRun(document.getElementById("detail-run-id").textContent.slice(4), "manual");
    }
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
