import { realpath, stat } from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { describeError } from "./docker_helper.ts";
import { validateProfileName } from "./profile.ts";

export const PIPELINE_SCHEMA_VERSION = 1;

export class PipelineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PipelineError";
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
  bundleRoot: string;
  entry_state: string;
  max_transitions: number;
  inputs: PipelineInputSpec[];
  states: ResolvedState[];
}

const SAFE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;

function expectObject(value: unknown, what: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PipelineError(`${what} is not a YAML mapping`);
  }
  return value as Record<string, unknown>;
}

function expectArray(value: unknown, what: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new PipelineError(`${what} must be a list, not a mapping or scalar`);
  }
  return value;
}

function expectNonEmptyString(value: unknown, what: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new PipelineError(`${what} must be a non-empty string`);
  }
  return value;
}

function expectExactKeys(
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

function expectPositiveSafeInteger(value: unknown, what: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
    throw new PipelineError(
      `${what} must be a positive safe integer, got ${JSON.stringify(value)}`,
    );
  }
  return value;
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

/**
 * Bundle file references (prompt, result_schema) must be clean relative paths;
 * containment inside the bundle is verified by realpath at load time.
 */
function validateBundleRelativePath(value: string, what: string): string {
  if (isAbsolute(value) || value.startsWith("~")) {
    throw new PipelineError(`${what} must be a bundle-relative path, got ${JSON.stringify(value)}`);
  }
  for (const segment of value.split("/")) {
    if (segment === "" || segment === "." || segment === ".." || segment === "~") {
      throw new PipelineError(
        `${what} must be a clean bundle-relative path without empty, ".", ".." or "~" segments, got ${JSON.stringify(value)}`,
      );
    }
  }
  return value;
}

function validateSafeId(value: unknown, what: string): string {
  const id = expectNonEmptyString(value, what);
  if (!SAFE_ID_PATTERN.test(id) || id.includes("..")) {
    throw new PipelineError(`${what} ${JSON.stringify(id)} is not a safe identifier`);
  }
  return id;
}

function validateProfileReference(name: string): string {
  try {
    return validateProfileName(name);
  } catch (cause) {
    throw new PipelineError(cause instanceof Error ? cause.message : String(cause));
  }
}

function parseTransition(raw: unknown, stateId: string, index: number): PipelineTransitionSpec {
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
    ),
    inputs,
    result_schema: validateBundleRelativePath(
      expectNonEmptyString(obj.result_schema, `${stateWhat} result_schema`),
      `${stateWhat} result_schema`,
    ),
    timeout_seconds: expectPositiveSafeInteger(obj.timeout_seconds, `${stateWhat} timeout_seconds`),
    max_attempts: expectPositiveSafeInteger(obj.max_attempts, `${stateWhat} max_attempts`),
    transitions,
  };
}

function parseTerminalState(obj: Record<string, unknown>): TerminalStateSpec {
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

function checkGraph(spec: PipelineSpec): void {
  const terminalIds = spec.states
    .filter((state) => state.type === "terminal")
    .map((state) => state.id);
  if (terminalIds.length === 0) {
    throw new PipelineError("pipeline must declare at least one terminal state");
  }
  const stateIds = new Set(spec.states.map((state) => state.id));
  if (!stateIds.has(spec.entry_state)) {
    throw new PipelineError(
      `entry_state ${JSON.stringify(spec.entry_state)} does not name a declared state`,
    );
  }
  const inputIds = new Set(spec.inputs.map((input) => input.id));
  const declaredOutcomeTargets = new Map<string, string[]>();
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

  // reachability from entry_state (cycles are fine; the whole run is bounded
  // by max_transitions)
  const reachable = new Set<string>([spec.entry_state]);
  const queue: string[] = [spec.entry_state];
  while (queue.length > 0) {
    const current = queue.pop() ?? "";
    for (const next of declaredOutcomeTargets.get(current) ?? []) {
      if (!reachable.has(next)) {
        reachable.add(next);
        queue.push(next);
      }
    }
  }
  for (const state of spec.states) {
    if (!reachable.has(state.id)) {
      throw new PipelineError(`state ${JSON.stringify(state.id)} is not reachable from entry_state`);
    }
  }

  // every agent state must have a path to at least one terminal state
  const canReachTerminal = new Set<string>(terminalIds);
  const reverseQueue: string[] = [...terminalIds];
  while (reverseQueue.length > 0) {
    const current = reverseQueue.pop() ?? "";
    for (const state of spec.states) {
      if (state.type !== "agent" || canReachTerminal.has(state.id)) {
        continue;
      }
      if ((declaredOutcomeTargets.get(state.id) ?? []).includes(current)) {
        canReachTerminal.add(state.id);
        reverseQueue.push(state.id);
      }
    }
  }
  for (const state of spec.states) {
    if (state.type === "agent" && !canReachTerminal.has(state.id)) {
      throw new PipelineError(
        `agent state ${JSON.stringify(state.id)} has no path to a terminal state`,
      );
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
    inputs.push({
      id,
      path: validateWorkspaceRelativePath(
        expectNonEmptyString(inputObj.path, `pipeline input ${JSON.stringify(id)} path`),
        `pipeline input ${JSON.stringify(id)} path`,
      ),
      protected: inputObj.protected,
    });
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

async function requireBundleFileInsideRoot(
  path: string,
  rootCanonical: string,
  what: string,
): Promise<string> {
  let info;
  try {
    info = await stat(path);
  } catch (cause) {
    throw new PipelineError(`${what} ${path} is not accessible: ${describeError(cause)}`);
  }
  if (!info.isFile()) {
    throw new PipelineError(`${what} ${path} is not a regular file`);
  }
  let canonical: string;
  try {
    canonical = await realpath(path);
  } catch (cause) {
    throw new PipelineError(`${what} ${path} cannot be canonicalized: ${describeError(cause)}`);
  }
  if (canonical !== rootCanonical && !canonical.startsWith(`${rootCanonical}/`)) {
    throw new PipelineError(`${what} ${path} resolves outside the pipeline bundle`);
  }
  return canonical;
}

async function readBundleFile(path: string, what: string): Promise<string> {
  try {
    return await Bun.file(path).text();
  } catch (cause) {
    throw new PipelineError(`${what} ${path} is not readable: ${describeError(cause)}`);
  }
}

async function resolveAgentBundleFiles(
  state: AgentStateSpec,
  rootCanonical: string,
): Promise<{ promptPath: string; promptContent: string; resultSchemaPath: string; resultSchema: Record<string, unknown> }> {
  const promptPath = await requireBundleFileInsideRoot(
    join(rootCanonical, state.prompt),
    rootCanonical,
    `agent state ${JSON.stringify(state.id)} prompt`,
  );
  const promptContent = await readBundleFile(
    promptPath,
    `agent state ${JSON.stringify(state.id)} prompt`,
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
  );
  const resultSchemaRaw = await readBundleFile(
    resultSchemaPath,
    `agent state ${JSON.stringify(state.id)} result_schema`,
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
  if (!isAbsolute(bundleRoot)) {
    throw new PipelineError(
      `pipeline bundle root must be an absolute path, got ${JSON.stringify(bundleRoot)}`,
    );
  }
  let rootCanonical: string;
  try {
    rootCanonical = await realpath(bundleRoot);
  } catch (cause) {
    throw new PipelineError(
      `pipeline bundle root ${bundleRoot} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  const pipelineFile = join(rootCanonical, "pipeline.yaml");
  let pipelineInfo;
  try {
    pipelineInfo = await stat(pipelineFile);
  } catch (cause) {
    throw new PipelineError(
      `pipeline bundle ${bundleRoot} has no readable pipeline.yaml (only pipeline.yaml is supported): ${describeError(cause)}`,
    );
  }
  if (!pipelineInfo.isFile()) {
    throw new PipelineError(
      `pipeline bundle ${bundleRoot}: pipeline.yaml is not a regular file`,
    );
  }
  const spec = parsePipelineSpec(await readBundleFile(pipelineFile, "pipeline file"));

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
    bundleRoot: rootCanonical,
    entry_state: spec.entry_state,
    max_transitions: spec.max_transitions,
    inputs: spec.inputs,
    states,
  };
}
