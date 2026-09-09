import { isAbsolute, join } from "node:path";
import { describeError, MAX_RUN_TIMEOUT_SECONDS } from "./docker_helper.ts";
import { matchesStandardAgentResultSchema } from "./agent_result.ts";
import { validateProfileName } from "./profile.ts";
import {
  readBundleFile,
  requireBundleFileInsideRoot,
  requireCanonicalDirectoryRoot,
  validateBundleRelativePath,
} from "./bundle_file.ts";

export const PIPELINE_SCHEMA_VERSION = 1;

/**
 * Schema version 2 (declarative data ports) is loadable through the pure
 * `pipeline_v2.ts` APIs but is not executable by the production path yet.
 */
export const PIPELINE_SCHEMA_VERSION_V2 = 2;

export class PipelineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineError";
  }
}

/**
 * Shared exact-field spec validators used by both pipeline schema versions.
 * They throw PipelineError and are exported for the v2 compiler; their
 * behavior is identical for v1 documents.
 */

export function expectObject(value: unknown, what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineError(`${what} is not a YAML mapping`);
  }
  return value as Record<string, unknown>;
}

export function expectArray(value: unknown, what: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new PipelineError(`${what} must be a list, not a mapping or scalar`);
  }
  return value;
}

export function expectNonEmptyString(value: unknown, what: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new PipelineError(`${what} must be a non-empty string`);
  }
  return value;
}

export function expectExactKeys(
  obj: Record<string, unknown>,
  keys: readonly string[],
  what: string,
): void {
  const expected = new Set(keys);
  for (const key of Object.keys(obj)) {
    if (!expected.has(key)) {
      throw new PipelineError(`${what} has unknown field ${JSON.stringify(key)}`);
    }
  }
  for (const key of keys) {
    if (!(key in obj)) {
      throw new PipelineError(`${what} is missing required field ${JSON.stringify(key)}`);
    }
  }
}

export function expectPositiveSafeInteger(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
    throw new PipelineError(
      `${what} must be a positive safe integer, got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

const SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

export function validateSafeId(value: unknown, what: string): string {
  const id = expectNonEmptyString(value, what);
  if (!SAFE_ID_PATTERN.test(id) || id.includes("..")) {
    throw new PipelineError(`${what} ${JSON.stringify(id)} is not a safe identifier`);
  }
  return id;
}

export function validateProfileReference(name: string): string {
  try {
    return validateProfileName(name);
  } catch (cause) {
    throw new PipelineError(cause instanceof Error ? cause.message : String(cause));
  }
}

export interface PipelineInputSpec {
  id: string;
  path: string;
  protected: boolean;
}

export interface PipelineTransitionSpec {
  outcome: string;
  to: string;
}

export interface AgentStateSpec {
  id: string;
  type: "agent";
  profile: string;
  prompt: string;
  inputs: string[];
  result_schema: string;
  timeout_seconds: number;
  max_attempts: number;
  transitions: PipelineTransitionSpec[];
}

export interface TerminalStateSpec {
  id: string;
  type: "terminal";
  result: "success" | "failed";
}

export type PipelineStateSpec = AgentStateSpec | TerminalStateSpec;

export interface PipelineSpec {
  schema_version: number;
  entry_state: string;
  max_transitions: number;
  inputs: PipelineInputSpec[];
  states: PipelineStateSpec[];
}

export interface ResolvedAgentState {
  id: string;
  type: "agent";
  profile: string;
  promptPath: string;
  promptContent: string;
  inputs: string[];
  resultSchemaPath: string;
  resultSchema: Record<string, unknown>;
  timeout_seconds: number;
  max_attempts: number;
  transitions: PipelineTransitionSpec[];
}

export interface ResolvedTerminalState {
  id: string;
  type: "terminal";
  result: "success" | "failed";
}

export type ResolvedState = ResolvedAgentState | ResolvedTerminalState;

export interface ResolvedPipeline {
  schema_version: number;
  bundleRoot: string;
  entry_state: string;
  max_transitions: number;
  inputs: PipelineInputSpec[];
  states: ResolvedState[];
}

/**
 * Workspace-relative input paths must be clean: no absolute paths, no home
 * expansion, no empty segments, no `.` or `..` traversal segments.
 */
function validateWorkspaceRelativePath(value: string, what: string): string {
  if (isAbsolute(value) || value.startsWith("~")) {
    throw new PipelineError(`${what} must be a workspace-relative path, got ${JSON.stringify(value)}`);
  }
  for (const segment of value.split("/")) {
    if (segment === "" || segment === "." || segment === ".." || segment === "~") {
      throw new PipelineError(
        `${what} must be a clean workspace-relative path without empty, ".", ".." or "~" segments, got ${JSON.stringify(value)}`,
      );
    }
  }
  return value;
}

export function parseTransition(raw: unknown, stateId: string, index: number): PipelineTransitionSpec {
  const obj = expectObject(raw, `transition ${index} of state ${JSON.stringify(stateId)}`);
  expectExactKeys(obj, ["outcome", "to"], `transition ${index} of state ${JSON.stringify(stateId)}`);
  return {
    outcome: expectNonEmptyString(obj.outcome, `transition ${index} of state ${JSON.stringify(stateId)} outcome`),
    to: validateSafeId(obj.to, `transition ${index} of state ${JSON.stringify(stateId)} target state id`),
  };
}

function parseAgentState(obj: Record<string, unknown>): AgentStateSpec {
  const id = validateSafeId(obj.id, "agent state id");
  const what = `agent state ${JSON.stringify(id)}`;
  expectExactKeys(
    obj,
    [
      "id",
      "type",
      "profile",
      "prompt",
      "inputs",
      "result_schema",
      "timeout_seconds",
      "max_attempts",
      "transitions",
    ],
    what,
  );
  const stateWhat = what;
  const inputsRaw = expectArray(obj.inputs, `${stateWhat} inputs`);
  const inputs: string[] = [];
  for (const rawInput of inputsRaw) {
    const inputId = validateSafeId(rawInput, `${stateWhat} input id`);
    if (inputs.includes(inputId)) {
      throw new PipelineError(`${stateWhat} lists input ${JSON.stringify(inputId)} more than once`);
    }
    inputs.push(inputId);
  }
  const transitionsRaw = expectArray(obj.transitions, `${stateWhat} transitions`);
  const transitions: PipelineTransitionSpec[] = [];
  for (let index = 0; index < transitionsRaw.length; index++) {
    transitions.push(parseTransition(transitionsRaw[index], id, index));
  }
  return {
    id,
    type: "agent",
    profile: validateProfileReference(
      expectNonEmptyString(obj.profile, `${stateWhat} profile`),
    ),
    prompt: validateBundleRelativePath(
      expectNonEmptyString(obj.prompt, `${stateWhat} prompt`),
      `${stateWhat} prompt`,
      PipelineError,
    ),
    inputs,
    result_schema: validateBundleRelativePath(
      expectNonEmptyString(obj.result_schema, `${stateWhat} result_schema`),
      `${stateWhat} result_schema`,
      PipelineError,
    ),
    timeout_seconds: expectPositiveSafeInteger(obj.timeout_seconds, `${stateWhat} timeout_seconds`),
    max_attempts: expectPositiveSafeInteger(obj.max_attempts, `${stateWhat} max_attempts`),
    transitions,
  };
}

export function parseTerminalState(obj: Record<string, unknown>): TerminalStateSpec {
  const id = validateSafeId(obj.id, "terminal state id");
  const what = `terminal state ${JSON.stringify(id)}`;
  expectExactKeys(obj, ["id", "type", "result"], what);
  if (obj.result !== "success" && obj.result !== "failed") {
    throw new PipelineError(
      `${what} result must be "success" or "failed", got ${JSON.stringify(obj.result)}`,
    );
  }
  return { id, type: "terminal", result: obj.result };
}

function parseState(raw: unknown, index: number): PipelineStateSpec {
  const obj = expectObject(raw, `state ${index}`);
  const type = obj.type;
  if (type !== "agent" && type !== "terminal") {
    throw new PipelineError(
      `state ${index} has unsupported type ${JSON.stringify(type)}, expected "agent" or "terminal"`,
    );
  }
  return type === "agent" ? parseAgentState(obj) : parseTerminalState(obj);
}

/**
 * Shared graph-shape validation for both pipeline schema versions: terminal
 * existence, declared entry state, unique per-state outcomes, declared
 * transition targets, and agent reachability from the entry state. Port and
 * input reference checks are version-specific and live with their parsers.
 */
export interface GraphShapeState {
  id: string;
  type: "agent" | "terminal";
  transitions: readonly { outcome: string; to: string }[];
}

export function checkGraphShape(entryState: string, states: readonly GraphShapeState[]): void {
  const terminalIds = states
    .filter((state) => state.type === "terminal")
    .map((state) => state.id);
  if (terminalIds.length === 0) {
    throw new PipelineError("pipeline must declare at least one terminal state");
  }
  const stateIds = new Set(states.map((state) => state.id));
  if (!stateIds.has(entryState)) {
    throw new PipelineError(
      `entry_state ${JSON.stringify(entryState)} does not name a declared state`,
    );
  }
  const declaredOutcomeTargets = new Map<string, string[]>();
  for (const state of states) {
    if (state.type !== "agent") {
      continue;
    }
    const outcomes = new Set<string>();
    const targets: string[] = [];
    for (const transition of state.transitions) {
      if (outcomes.has(transition.outcome)) {
        throw new PipelineError(
          `agent state ${JSON.stringify(state.id)} declares outcome ${JSON.stringify(transition.outcome)} more than once`,
        );
      }
      outcomes.add(transition.outcome);
      if (!stateIds.has(transition.to)) {
        throw new PipelineError(
          `agent state ${JSON.stringify(state.id)} transition outcome ${JSON.stringify(transition.outcome)} targets unknown state ${JSON.stringify(transition.to)}`,
        );
      }
      targets.push(transition.to);
    }
    declaredOutcomeTargets.set(state.id, targets);
  }

  // reachability from the entry state. Agent states must be reachable from
  // the entry; terminal states may sit beyond the reachable part (a cycle
  // can never leave itself, and the transition budget bounds the whole run).
  const reachable = new Set<string>([entryState]);
  const queue: string[] = [entryState];
  while (queue.length > 0) {
    const current = queue.pop() ?? "";
    for (const next of declaredOutcomeTargets.get(current) ?? []) {
      if (!reachable.has(next)) {
        reachable.add(next);
        queue.push(next);
      }
    }
  }
  for (const state of states) {
    if (state.type === "agent" && !reachable.has(state.id)) {
      throw new PipelineError(`agent state ${JSON.stringify(state.id)} is not reachable from entry_state`);
    }
  }
}

function checkGraph(spec: PipelineSpec): void {
  checkGraphShape(
    spec.entry_state,
    spec.states.map((state): GraphShapeState => ({
      id: state.id,
      type: state.type,
      transitions: state.type === "agent" ? state.transitions : [],
    })),
  );
  const inputIds = new Set(spec.inputs.map((input) => input.id));
  for (const state of spec.states) {
    if (state.type !== "agent") {
      continue;
    }
    for (const inputId of state.inputs) {
      if (!inputIds.has(inputId)) {
        throw new PipelineError(
          `agent state ${JSON.stringify(state.id)} references undeclared input ${JSON.stringify(inputId)}`,
        );
      }
    }
  }
}

export function parsePipelineSpec(raw: string): PipelineSpec {
  let parsed: unknown;
  try {
    parsed = Bun.YAML.parse(raw);
  } catch (cause) {
    throw new PipelineError(`pipeline is not valid YAML: ${describeError(cause)}`);
  }
  const obj = expectObject(parsed, "pipeline");
  // Any document with schema_version 2 is a v2 pipeline; the production path
  // cannot execute v2 yet, so it is rejected explicitly and uniformly before
  // anything else happens. No shape sniffing decides "genuine v2".
  if (obj.schema_version === PIPELINE_SCHEMA_VERSION_V2) {
    throw new PipelineError("pipeline schema version 2 is not executable yet");
  }
  expectExactKeys(
    obj,
    ["schema_version", "entry_state", "max_transitions", "inputs", "states"],
    "pipeline",
  );
  if (obj.schema_version !== PIPELINE_SCHEMA_VERSION) {
    throw new PipelineError(
      `pipeline has schema_version ${JSON.stringify(obj.schema_version)}, expected ${PIPELINE_SCHEMA_VERSION}`,
    );
  }
  const entryState = validateSafeId(obj.entry_state, "pipeline entry_state");
  const maxTransitions = expectPositiveSafeInteger(obj.max_transitions, "pipeline max_transitions");

  const inputsRaw = expectArray(obj.inputs, "pipeline inputs");
  const inputs: PipelineInputSpec[] = [];
  const inputIds = new Set<string>();
  const inputPaths = new Set<string>();
  for (let index = 0; index < inputsRaw.length; index++) {
    const inputObj = expectObject(inputsRaw[index], `pipeline input ${index}`);
    expectExactKeys(inputObj, ["id", "path", "protected"], `pipeline input ${index}`);
    const id = validateSafeId(inputObj.id, `pipeline input ${index} id`);
    if (inputIds.has(id)) {
      throw new PipelineError(`pipeline declares input ${JSON.stringify(id)} more than once`);
    }
    inputIds.add(id);
    if (typeof inputObj.protected !== "boolean") {
      throw new PipelineError(
        `pipeline input ${JSON.stringify(id)} protected must be a boolean`,
      );
    }
    const path = validateWorkspaceRelativePath(
      expectNonEmptyString(inputObj.path, `pipeline input ${JSON.stringify(id)} path`),
      `pipeline input ${JSON.stringify(id)} path`,
    );
    if (inputPaths.has(path)) {
      throw new PipelineError(
        `pipeline declares workspace input path ${JSON.stringify(path)} more than once (each workspace path may map to only one input declaration)`,
      );
    }
    inputPaths.add(path);
    inputs.push({ id, path, protected: inputObj.protected });
  }

  const statesRaw = expectArray(obj.states, "pipeline states");
  if (statesRaw.length === 0) {
    throw new PipelineError("pipeline must declare at least one state");
  }
  const states: PipelineStateSpec[] = [];
  const stateIds = new Set<string>();
  for (let index = 0; index < statesRaw.length; index++) {
    const state = parseState(statesRaw[index], index);
    if (stateIds.has(state.id)) {
      throw new PipelineError(`pipeline declares state ${JSON.stringify(state.id)} more than once`);
    }
    stateIds.add(state.id);
    states.push(state);
  }

  const spec: PipelineSpec = {
    schema_version: PIPELINE_SCHEMA_VERSION,
    entry_state: entryState,
    max_transitions: maxTransitions,
    inputs,
    states,
  };
  checkGraph(spec);
  return spec;
}

async function resolveAgentBundleFiles(
  state: AgentStateSpec,
  rootCanonical: string,
): Promise<{ promptPath: string; promptContent: string; resultSchemaPath: string; resultSchema: Record<string, unknown> }> {
  const promptPath = await requireBundleFileInsideRoot(
    join(rootCanonical, state.prompt),
    rootCanonical,
    `agent state ${JSON.stringify(state.id)} prompt`,
    PipelineError,
    "pipeline bundle",
  );
  const promptContent = await readBundleFile(
    promptPath,
    `agent state ${JSON.stringify(state.id)} prompt`,
    PipelineError,
  );
  if (promptContent.trim() === "") {
    throw new PipelineError(
      `agent state ${JSON.stringify(state.id)} prompt ${promptPath} is empty`,
    );
  }
  const resultSchemaPath = await requireBundleFileInsideRoot(
    join(rootCanonical, state.result_schema),
    rootCanonical,
    `agent state ${JSON.stringify(state.id)} result_schema`,
    PipelineError,
    "pipeline bundle",
  );
  const resultSchemaRaw = await readBundleFile(
    resultSchemaPath,
    `agent state ${JSON.stringify(state.id)} result_schema`,
    PipelineError,
  );
  let resultSchema: unknown;
  try {
    resultSchema = JSON.parse(resultSchemaRaw);
  } catch (cause) {
    throw new PipelineError(
      `agent state ${JSON.stringify(state.id)} result schema ${resultSchemaPath} is not valid JSON: ${describeError(cause)}`,
    );
  }
  if (typeof resultSchema !== "object" || resultSchema === null || Array.isArray(resultSchema)) {
    throw new PipelineError(
      `agent state ${JSON.stringify(state.id)} result schema ${resultSchemaPath} is not a JSON object`,
    );
  }
  return {
    promptPath,
    promptContent,
    resultSchemaPath,
    resultSchema: resultSchema as Record<string, unknown>,
  };
}

export async function loadPipeline(bundleRoot: string): Promise<ResolvedPipeline> {
  const rootCanonical = await requireCanonicalDirectoryRoot(
    bundleRoot,
    "pipeline bundle root",
    PipelineError,
  );
  // pipeline.yaml obeys the same fail-closed containment contract as the other
  // bundle files: a symlink to a file inside the bundle is allowed, a symlink
  // escape outside the bundle is rejected. The file is read from the verified
  // canonical path only.
  const pipelinePath = await requireBundleFileInsideRoot(
    join(rootCanonical, "pipeline.yaml"),
    rootCanonical,
    "pipeline file",
    PipelineError,
    "pipeline bundle",
  );
  const spec = parsePipelineSpec(await readBundleFile(pipelinePath, "pipeline file", PipelineError));

  const states: ResolvedState[] = [];
  for (const state of spec.states) {
    if (state.type === "agent") {
      const files = await resolveAgentBundleFiles(state, rootCanonical);
      states.push({
        id: state.id,
        type: "agent",
        profile: state.profile,
        promptPath: files.promptPath,
        promptContent: files.promptContent,
        inputs: state.inputs,
        resultSchemaPath: files.resultSchemaPath,
        resultSchema: files.resultSchema,
        timeout_seconds: state.timeout_seconds,
        max_attempts: state.max_attempts,
        transitions: state.transitions,
      });
    } else {
      states.push({ id: state.id, type: "terminal", result: state.result });
    }
  }

  return {
    schema_version: spec.schema_version,
    bundleRoot: rootCanonical,
    entry_state: spec.entry_state,
    max_transitions: spec.max_transitions,
    inputs: spec.inputs,
    states,
  };
}

export const MULTI_STATE_ATTEMPT = 1;
export const MULTI_STATE_OUTCOME = "completed";

export interface MultiStatePlan {
  pipeline: ResolvedPipeline;
  /** Unique profile names referenced by agent states, in declaration order. */
  profileNames: string[];
  /** The protected subset of the declared inputs. */
  protectedInputs: PipelineInputSpec[];
}

/**
 * Builds the multi-state execution plan this increment supports. Any other
 * structurally valid pipeline is rejected with a clear message before any
 * Launcher authentication or child Session creation. The supported form: any
 * number of agent and terminal states (the entry may be a terminal), cycles
 * bounded by `max_transitions`, every agent state with `max_attempts: 1` and
 * exactly one transition with outcome "completed", and a result schema that
 * equals the standard multi-state agent result contract. Other outcomes,
 * several transitions per agent state, retries, and custom decision payloads
 * are rejected here — the engine then owns the only supported mapping
 * (outcome "completed" -> the declared transition).
 */
export function planMultiStateExecution(pipeline: ResolvedPipeline): MultiStatePlan {
  const unsupported = (reason: string): PipelineError =>
    new PipelineError(
      `pipeline is not supported by the current multi-state execution: ${reason}`,
    );

  for (const state of pipeline.states) {
    if (state.type !== "agent") {
      continue;
    }
    if (state.max_attempts !== MULTI_STATE_ATTEMPT) {
      throw unsupported(
        `agent state ${JSON.stringify(state.id)} max_attempts must be ${MULTI_STATE_ATTEMPT}, got ${state.max_attempts}`,
      );
    }
    if (state.transitions.length !== 1) {
      throw unsupported(
        `agent state ${JSON.stringify(state.id)} must have exactly one transition, got ${state.transitions.length}`,
      );
    }
    const transition = state.transitions[0];
    if (transition === undefined) {
      throw unsupported(`agent state ${JSON.stringify(state.id)} has no transition`);
    }
    if (transition.outcome !== MULTI_STATE_OUTCOME) {
      throw unsupported(
        `the single transition outcome of agent state ${JSON.stringify(state.id)} must be ${JSON.stringify(MULTI_STATE_OUTCOME)}, got ${JSON.stringify(transition.outcome)}`,
      );
    }
    if (state.timeout_seconds > MAX_RUN_TIMEOUT_SECONDS) {
      throw unsupported(
        `agent state ${JSON.stringify(state.id)} timeout_seconds ${state.timeout_seconds} exceeds the maximum representable single-timer bound ${MAX_RUN_TIMEOUT_SECONDS}`,
      );
    }
    if (!matchesStandardAgentResultSchema(state.resultSchema)) {
      throw unsupported(
        `the result schema of agent state ${JSON.stringify(state.id)} does not match the supported standard agent result contract`,
      );
    }
  }

  const profileNames: string[] = [];
  const seenProfiles = new Set<string>();
  for (const state of pipeline.states) {
    if (state.type !== "agent" || seenProfiles.has(state.profile)) {
      continue;
    }
    seenProfiles.add(state.profile);
    profileNames.push(state.profile);
  }
  const protectedInputs = pipeline.inputs.filter((input) => input.protected);
  return {
    pipeline,
    profileNames,
    protectedInputs,
  };
}
