import { join } from "node:path";
import {
  PipelineError,
  expectArray,
  expectExactKeys,
  expectNonEmptyString,
  expectObject,
  expectPositiveSafeInteger,
  validateProfileReference,
  validateSafeId,
  checkGraphShape,
  parseTerminalState,
  parseTransition,
  type GraphShapeState,
  type PipelineTransitionSpec,
  type TerminalStateSpec,
} from "./pipeline.ts";
import {
  readBundleFile,
  requireBundleFileInsideRoot,
  requireCanonicalDirectoryRoot,
  validateBundleRelativePath,
} from "./bundle_file.ts";
import { describeError } from "./docker_helper.ts";

/**
 * Pipeline schema version 2: declarative data ports.
 *
 * A v2 pipeline describes data flow as named ports instead of workspace
 * paths. Run-level `inputs` are the logical inputs of a run (actual host
 * paths are bound later by the CLI/API and are not part of the document);
 * run-level `outputs` are the data handed back to the user after
 * completion; every agent state declares local input and output ports. A
 * port source has exactly one form: `{pipeline_input: ID}` or
 * `{state_output: {state: ID, output: ID}}`.
 *
 * Runtime semantics fixed here for later increments (not implemented yet):
 * a `state_output` value is the last successfully accepted output of the
 * named state from an earlier activation; when no such value exists the run
 * fails closed — there is no fallback value. Run-level outputs are taken by
 * the same rule when a terminal state is reached; a missing
 * `required: true` output fails the run, an optional output may be absent.
 * Self-references and cycles between state outputs compile fine: value
 * availability is a runtime question.
 *
 * The activation completion envelope for v2 is orchestrator-owned; work
 * products are defined only by the declared outputs — there is no
 * free-form artifact path list.
 *
 * This module is pure substrate: it is not wired into the production
 * runner (`pipeline.ts` load path, `agent-smoke`, profiles, durable state,
 * helper transport). The production loader rejects v2 documents with
 * "pipeline schema version 2 is not executable yet" before Launcher auth
 * and before any Session is created.
 */

export const PIPELINE_SCHEMA_VERSION_V2 = 2;

export type PortType = "file" | "directory" | "json";

/**
 * Port source: exactly one form. `state_output` resolves at runtime to the
 * last successfully accepted output of the named state from an earlier
 * activation; when no such value exists the run fails closed without
 * fallback. Value availability is never a compile-time question, so
 * self-references and cycles between state outputs compile.
 */
export interface PipelinePortSourcePipelineInput {
  readonly pipeline_input: string;
}

export interface PipelinePortSourceStateOutput {
  readonly state_output: { readonly state: string; readonly output: string };
}

export type PipelinePortSource =
  | PipelinePortSourcePipelineInput
  | PipelinePortSourceStateOutput;

export interface PipelineV2InputSpec {
  id: string;
  type: PortType;
  protected: boolean;
  /** Bundle-relative JSON schema file; present exactly when type is json. */
  schema?: string;
}

export interface PipelineV2OutputSpec {
  id: string;
  type: PortType;
  required: boolean;
  source: PipelinePortSource;
  /** Bundle-relative JSON schema file; present exactly when type is json. */
  schema?: string;
}

/** Input port as declared: the type is derived from the source at compile time. */
export interface PipelineV2AgentInputSpecDraft {
  id: string;
  source: PipelinePortSource;
}

export interface PipelineV2AgentInputSpec {
  id: string;
  source: PipelinePortSource;
  /** Derived from the source at compile time; never declared by the user. */
  type: PortType;
}

/** Agent state as parsed, before input types are derived from sources. */
export interface PipelineV2AgentStateDraft {
  id: string;
  type: "agent";
  profile: string;
  prompt: string;
  inputs: PipelineV2AgentInputSpecDraft[];
  outputs: PipelineV2AgentOutputSpec[];
  timeout_seconds: number;
  max_attempts: number;
  transitions: PipelineTransitionSpec[];
}

export interface PipelineV2AgentOutputSpec {
  id: string;
  type: PortType;
  /** Bundle-relative JSON schema file; present exactly when type is json. */
  schema?: string;
}

export interface PipelineV2AgentStateSpec {
  id: string;
  type: "agent";
  profile: string;
  prompt: string;
  inputs: PipelineV2AgentInputSpec[];
  outputs: PipelineV2AgentOutputSpec[];
  timeout_seconds: number;
  max_attempts: number;
  transitions: PipelineTransitionSpec[];
}

export type PipelineV2StateSpec = PipelineV2AgentStateSpec | TerminalStateSpec;

export interface PipelineV2Spec {
  schema_version: 2;
  entry_state: string;
  max_transitions: number;
  inputs: PipelineV2InputSpec[];
  outputs: PipelineV2OutputSpec[];
  states: PipelineV2StateSpec[];
}

export interface ResolvedV2RunInput {
  readonly id: string;
  readonly type: PortType;
  readonly protected: boolean;
}

export interface ResolvedV2RunOutput {
  readonly id: string;
  readonly type: PortType;
  readonly required: boolean;
  readonly source: PipelinePortSource;
  readonly schemaPath?: string;
  readonly schema?: Readonly<Record<string, unknown>>;
}

export interface ResolvedV2AgentInputPort {
  readonly id: string;
  readonly source: PipelinePortSource;
  readonly type: PortType;
}

export interface ResolvedV2AgentOutputPort {
  readonly id: string;
  readonly type: PortType;
  readonly schemaPath?: string;
  readonly schema?: Readonly<Record<string, unknown>>;
}

export interface ResolvedV2AgentState {
  readonly id: string;
  readonly type: "agent";
  readonly profile: string;
  readonly promptPath: string;
  readonly promptContent: string;
  readonly inputs: readonly ResolvedV2AgentInputPort[];
  readonly outputs: readonly ResolvedV2AgentOutputPort[];
  readonly timeout_seconds: number;
  readonly max_attempts: number;
  readonly transitions: readonly PipelineTransitionSpec[];
}

export type ResolvedV2State = ResolvedV2AgentState | TerminalStateSpec;

export interface ResolvedPipelineV2 {
  readonly schema_version: 2;
  readonly bundleRoot: string;
  readonly entry_state: string;
  readonly max_transitions: number;
  readonly inputs: readonly ResolvedV2RunInput[];
  readonly outputs: readonly ResolvedV2RunOutput[];
  readonly states: readonly ResolvedV2State[];
}

function deepFreeze<T>(value: T): T {
  if (Array.isArray(value)) {
    for (const item of value) deepFreeze(item);
    return Object.freeze(value) as T;
  }
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      deepFreeze(record[key]);
    }
    return Object.freeze(record) as unknown as T;
  }
  return value;
}

function parsePortType(value: unknown, what: string): PortType {
  if (value !== "file" && value !== "directory" && value !== "json") {
    throw new PipelineError(
      `${what} must be one of "file", "directory" or "json", got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function parseSchemaField(
  entry: Record<string, unknown>,
  keys: readonly string[],
  type: PortType,
  id: string,
  what: string,
): { schema?: string } {
  const hasSchema = keys.includes("schema");
  if (type === "json") {
    if (!hasSchema) {
      throw new PipelineError(
        `${what} ${JSON.stringify(id)} with type "json" must declare a schema`,
      );
    }
    return {
      schema: validateBundleRelativePath(
        expectNonEmptyString(entry.schema, `${what} ${JSON.stringify(id)} schema`),
        `${what} ${JSON.stringify(id)} schema`,
        PipelineError,
      ),
    };
  }
  if (hasSchema) {
    throw new PipelineError(
      `${what} ${JSON.stringify(id)} with type ${JSON.stringify(type)} must not declare a schema`,
    );
  }
  return {};
}

function parsePortSource(raw: unknown, what: string): PipelinePortSource {
  const source = expectObject(raw, `${what} source`);
  const keys = Object.keys(source);
  const allowed = new Set(["pipeline_input", "state_output"]);
  for (const key of keys) {
    if (!allowed.has(key)) {
      throw new PipelineError(`${what} source has unknown field ${JSON.stringify(key)}`);
    }
  }
  const hasPipelineInput = keys.includes("pipeline_input");
  const hasStateOutput = keys.includes("state_output");
  if (hasPipelineInput === hasStateOutput) {
    throw new PipelineError(
      `${what} source must declare exactly one of "pipeline_input" or "state_output"`,
    );
  }
  if (hasPipelineInput) {
    return Object.freeze({
      pipeline_input: validateSafeId(
        source.pipeline_input,
        `${what} source pipeline_input id`,
      ),
    }) as PipelinePortSourcePipelineInput;
  }
  const stateOutput = expectObject(source.state_output, `${what} source state_output`);
  expectExactKeys(stateOutput, ["state", "output"], `${what} source state_output`);
  return Object.freeze({
    state_output: Object.freeze({
      state: validateSafeId(stateOutput.state, `${what} source state_output state id`),
      output: validateSafeId(stateOutput.output, `${what} source state_output output id`),
    }),
  }) as PipelinePortSourceStateOutput;
}

function parseV2Input(raw: unknown, index: number): PipelineV2InputSpec {
  const what = `pipeline input ${index}`;
  const entry = expectObject(raw, what);
  const hasSchema = "schema" in entry;
  expectExactKeys(
    entry,
    hasSchema ? ["id", "type", "protected", "schema"] : ["id", "type", "protected"],
    what,
  );
  const id = validateSafeId(entry.id, `${what} id`);
  const type = parsePortType(entry.type, `pipeline input ${JSON.stringify(id)} type`);
  if (typeof entry.protected !== "boolean") {
    throw new PipelineError(`pipeline input ${JSON.stringify(id)} protected must be a boolean`);
  }
  return {
    id,
    type,
    protected: entry.protected,
    ...parseSchemaField(entry, Object.keys(entry), type, id, "pipeline input"),
  };
}

function parseV2Output(raw: unknown, index: number): PipelineV2OutputSpec {
  const what = `pipeline output ${index}`;
  const entry = expectObject(raw, what);
  const hasSchema = "schema" in entry;
  expectExactKeys(
    entry,
    hasSchema
      ? ["id", "type", "required", "source", "schema"]
      : ["id", "type", "required", "source"],
    what,
  );
  const id = validateSafeId(entry.id, `${what} id`);
  const type = parsePortType(entry.type, `pipeline output ${JSON.stringify(id)} type`);
  if (typeof entry.required !== "boolean") {
    throw new PipelineError(`pipeline output ${JSON.stringify(id)} required must be a boolean`);
  }
  return {
    id,
    type,
    required: entry.required,
    source: parsePortSource(entry.source, `pipeline output ${JSON.stringify(id)}`),
    ...parseSchemaField(entry, Object.keys(entry), type, id, "pipeline output"),
  };
}

function parseV2AgentState(raw: Record<string, unknown>): PipelineV2AgentStateDraft {
  const id = validateSafeId(raw.id, "agent state id");
  const what = `agent state ${JSON.stringify(id)}`;
  expectExactKeys(
    raw,
    ["id", "type", "profile", "prompt", "inputs", "outputs", "timeout_seconds", "max_attempts", "transitions"],
    what,
  );

  const inputsRaw = expectArray(raw.inputs, `${what} inputs`);
  const inputs: PipelineV2AgentInputSpecDraft[] = [];
  const inputIds = new Set<string>();
  for (let index = 0; index < inputsRaw.length; index++) {
    const portWhat = `${what} input port ${index}`;
    const entry = expectObject(inputsRaw[index], portWhat);
    expectExactKeys(entry, ["id", "source"], portWhat);
    const portId = validateSafeId(entry.id, `${portWhat} id`);
    if (inputIds.has(portId)) {
      throw new PipelineError(`${what} declares input port ${JSON.stringify(portId)} more than once`);
    }
    inputIds.add(portId);
    inputs.push({ id: portId, source: parsePortSource(entry.source, `${portWhat}`) });
  }

  const outputsRaw = expectArray(raw.outputs, `${what} outputs`);
  const outputs: PipelineV2AgentOutputSpec[] = [];
  const outputIds = new Set<string>();
  for (let index = 0; index < outputsRaw.length; index++) {
    const portWhat = `${what} output port ${index}`;
    const entry = expectObject(outputsRaw[index], portWhat);
    const hasSchema = "schema" in entry;
    expectExactKeys(entry, hasSchema ? ["id", "type", "schema"] : ["id", "type"], portWhat);
    const portId = validateSafeId(entry.id, `${portWhat} id`);
    if (outputIds.has(portId)) {
      throw new PipelineError(`${what} declares output port ${JSON.stringify(portId)} more than once`);
    }
    outputIds.add(portId);
    const type = parsePortType(entry.type, `agent state ${JSON.stringify(id)} output port ${JSON.stringify(portId)} type`);
    outputs.push({
      id: portId,
      type,
      ...parseSchemaField(entry, Object.keys(entry), type, portId, "agent state output port"),
    });
  }

  const transitionsRaw = expectArray(raw.transitions, `${what} transitions`);
  const transitions: PipelineTransitionSpec[] = [];
  for (let index = 0; index < transitionsRaw.length; index++) {
    transitions.push(parseTransition(transitionsRaw[index], id, index));
  }

  return {
    id,
    type: "agent",
    profile: validateProfileReference(expectNonEmptyString(raw.profile, `${what} profile`)),
    prompt: validateBundleRelativePath(
      expectNonEmptyString(raw.prompt, `${what} prompt`),
      `${what} prompt`,
      PipelineError,
    ),
    inputs,
    outputs,
    timeout_seconds: expectPositiveSafeInteger(raw.timeout_seconds, `${what} timeout_seconds`),
    max_attempts: expectPositiveSafeInteger(raw.max_attempts, `${what} max_attempts`),
    transitions,
  };
}

/**
 * Compile a parsed v2 document into the plain v2 spec: exact-field
 * validation at every level, unique safe ids, declared references, graph
 * shape, and input-type derivation from port sources. Input port types are
 * derived — the user never declares or overrides them.
 */
export function compilePipelineV2Spec(parsed: unknown): PipelineV2Spec {
  const obj = expectObject(parsed, "pipeline");
  expectExactKeys(
    obj,
    ["schema_version", "entry_state", "max_transitions", "inputs", "outputs", "states"],
    "pipeline",
  );
  if (obj.schema_version !== PIPELINE_SCHEMA_VERSION_V2) {
    throw new PipelineError(
      `pipeline has schema_version ${JSON.stringify(obj.schema_version)}, expected ${PIPELINE_SCHEMA_VERSION_V2}`,
    );
  }
  const entryState = validateSafeId(obj.entry_state, "pipeline entry_state");
  const maxTransitions = expectPositiveSafeInteger(obj.max_transitions, "pipeline max_transitions");

  const inputsRaw = expectArray(obj.inputs, "pipeline inputs");
  const inputs: PipelineV2InputSpec[] = [];
  const inputIds = new Set<string>();
  for (let index = 0; index < inputsRaw.length; index++) {
    const input = parseV2Input(inputsRaw[index], index);
    if (inputIds.has(input.id)) {
      throw new PipelineError(`pipeline declares input ${JSON.stringify(input.id)} more than once`);
    }
    inputIds.add(input.id);
    inputs.push(input);
  }

  const outputsRaw = expectArray(obj.outputs, "pipeline outputs");
  const outputs: PipelineV2OutputSpec[] = [];
  const outputIds = new Set<string>();
  for (let index = 0; index < outputsRaw.length; index++) {
    const output = parseV2Output(outputsRaw[index], index);
    if (outputIds.has(output.id)) {
      throw new PipelineError(`pipeline declares output ${JSON.stringify(output.id)} more than once`);
    }
    outputIds.add(output.id);
    outputs.push(output);
  }

  const statesRaw = expectArray(obj.states, "pipeline states");
  if (statesRaw.length === 0) {
    throw new PipelineError("pipeline must declare at least one state");
  }
  const states: (PipelineV2AgentStateDraft | TerminalStateSpec)[] = [];
  const stateIds = new Set<string>();
  for (let index = 0; index < statesRaw.length; index++) {
    const entry = expectObject(statesRaw[index], `state ${index}`);
    const type = entry.type;
    if (type !== "agent" && type !== "terminal") {
      throw new PipelineError(
        `state ${index} has unsupported type ${JSON.stringify(type)}, expected "agent" or "terminal"`,
      );
    }
    const state = type === "agent" ? parseV2AgentState(entry) : parseTerminalState(entry);
    if (stateIds.has(state.id)) {
      throw new PipelineError(`pipeline declares state ${JSON.stringify(state.id)} more than once`);
    }
    stateIds.add(state.id);
    states.push(state);
  }

  const graphStates: GraphShapeState[] = states.map((state) => ({
    id: state.id,
    type: state.type,
    transitions: state.type === "agent" ? state.transitions : [],
  }));
  checkGraphShape(entryState, graphStates);

  // Reference resolution and type derivation. Only declared types are read,
  // so self-references and cycles between state outputs compile.
  const outputTypesByState = new Map<string, Map<string, PortType>>();
  for (const state of states) {
    if (state.type !== "agent") {
      continue;
    }
    outputTypesByState.set(
      state.id,
      new Map(state.outputs.map((port) => [port.id, port.type])),
    );
  }
  const resolveSourceType = (source: PipelinePortSource, what: string): PortType => {
    if ("pipeline_input" in source) {
      const declared = inputs.find((entry) => entry.id === source.pipeline_input);
      if (declared === undefined) {
        throw new PipelineError(
          `${what} references undeclared pipeline input ${JSON.stringify(source.pipeline_input)}`,
        );
      }
      return declared.type;
    }
    const stateOutput = source.state_output;
    const declaredOutputs = outputTypesByState.get(stateOutput.state);
    if (declaredOutputs === undefined) {
      if (stateIds.has(stateOutput.state)) {
        throw new PipelineError(
          `${what} references state ${JSON.stringify(stateOutput.state)} which declares no output ports`,
        );
      }
      throw new PipelineError(
        `${what} references undeclared state ${JSON.stringify(stateOutput.state)}`,
      );
    }
    const portType = declaredOutputs.get(stateOutput.output);
    if (portType === undefined) {
      throw new PipelineError(
        `${what} references undeclared output ${JSON.stringify(stateOutput.output)} of state ${JSON.stringify(stateOutput.state)}`,
      );
    }
    return portType;
  };

  const resolvedStates: PipelineV2StateSpec[] = states.map((state) => {
    if (state.type !== "agent") {
      return state;
    }
    return {
      id: state.id,
      type: "agent",
      profile: state.profile,
      prompt: state.prompt,
      inputs: state.inputs.map((port) => ({
        id: port.id,
        source: port.source,
        type: resolveSourceType(
          port.source,
          `agent state ${JSON.stringify(state.id)} input port ${JSON.stringify(port.id)}`,
        ),
      })),
      outputs: state.outputs,
      timeout_seconds: state.timeout_seconds,
      max_attempts: state.max_attempts,
      transitions: state.transitions,
    };
  });
  for (const output of outputs) {
    const sourceType = resolveSourceType(output.source, `pipeline output ${JSON.stringify(output.id)}`);
    if (output.type !== sourceType) {
      throw new PipelineError(
        `pipeline output ${JSON.stringify(output.id)} declares type ${JSON.stringify(output.type)} but its source provides ${JSON.stringify(sourceType)}`,
      );
    }
  }

  return {
    schema_version: 2,
    entry_state: entryState,
    max_transitions: maxTransitions,
    inputs,
    outputs,
    states: resolvedStates,
  };
}

/** Parse a v2 decision document (YAML text) into its plain v2 spec. */
export function parsePipelineV2Spec(raw: string): PipelineV2Spec {
  let parsed: unknown;
  try {
    parsed = Bun.YAML.parse(raw);
  } catch (cause) {
    throw new PipelineError(`pipeline is not valid YAML: ${describeError(cause)}`);
  }
  return compilePipelineV2Spec(parsed);
}

async function loadJsonSchema(
  rootCanonical: string,
  relativePath: string,
  what: string,
): Promise<{ path: string; schema: Record<string, unknown> }> {
  const schemaPath = await requireBundleFileInsideRoot(
    join(rootCanonical, relativePath),
    rootCanonical,
    what,
    PipelineError,
    "pipeline bundle",
  );
  const raw = await readBundleFile(schemaPath, what, PipelineError);
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new PipelineError(
      `${what} ${schemaPath} is not valid JSON: ${describeError(cause)}`,
    );
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new PipelineError(`${what} ${schemaPath} is not a JSON object`);
  }
  return { path: schemaPath, schema: parsed as Record<string, unknown> };
}

/**
 * Load a v2 pipeline bundle: the same fail-closed bundle containment as v1
 * (real directory root, pipeline.yaml inside the root, symlink rules), then
 * prompt and JSON schema files loaded through the same containment. The
 * result is an engine-owned deep-frozen snapshot; mutations of the parsed
 * source objects cannot change it.
 */
export async function loadPipelineV2(bundleRoot: string): Promise<ResolvedPipelineV2> {
  const rootCanonical = await requireCanonicalDirectoryRoot(
    bundleRoot,
    "pipeline bundle root",
    PipelineError,
  );
  const pipelinePath = await requireBundleFileInsideRoot(
    join(rootCanonical, "pipeline.yaml"),
    rootCanonical,
    "pipeline file",
    PipelineError,
    "pipeline bundle",
  );
  const spec = parsePipelineV2Spec(await readBundleFile(pipelinePath, "pipeline file", PipelineError));

  const inputs: ResolvedV2RunInput[] = spec.inputs.map((entry) =>
    deepFreeze({ id: entry.id, type: entry.type, protected: entry.protected }),
  );
  const outputs: ResolvedV2RunOutput[] = [];
  for (const output of spec.outputs) {
    if (output.schema === undefined) {
      outputs.push(
        deepFreeze({
          id: output.id,
          type: output.type,
          required: output.required,
          source: output.source,
        }),
      );
      continue;
    }
    const loaded = await loadJsonSchema(
      rootCanonical,
      output.schema,
      `pipeline output ${JSON.stringify(output.id)} schema`,
    );
    outputs.push(
      deepFreeze({
        id: output.id,
        type: output.type,
        required: output.required,
        source: output.source,
        schemaPath: loaded.path,
        schema: loaded.schema,
      }),
    );
  }

  const states: ResolvedV2State[] = [];
  for (const state of spec.states) {
    if (state.type === "agent") {
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
      const inputPorts: ResolvedV2AgentInputPort[] = state.inputs.map((port) =>
        deepFreeze({ id: port.id, source: port.source, type: port.type }),
      );
      const outputPorts: ResolvedV2AgentOutputPort[] = [];
      for (const port of state.outputs) {
        if (port.schema === undefined) {
          outputPorts.push(deepFreeze({ id: port.id, type: port.type }));
          continue;
        }
        const loaded = await loadJsonSchema(
          rootCanonical,
          port.schema,
          `agent state ${JSON.stringify(state.id)} output port ${JSON.stringify(port.id)} schema`,
        );
        outputPorts.push(
          deepFreeze({
            id: port.id,
            type: port.type,
            schemaPath: loaded.path,
            schema: loaded.schema,
          }),
        );
      }
      states.push(
        deepFreeze({
          id: state.id,
          type: "agent",
          profile: state.profile,
          promptPath,
          promptContent,
          inputs: Object.freeze(inputPorts),
          outputs: Object.freeze(outputPorts),
          timeout_seconds: state.timeout_seconds,
          max_attempts: state.max_attempts,
          transitions: Object.freeze(
            state.transitions.map((transition) =>
              deepFreeze({ outcome: transition.outcome, to: transition.to }),
            ),
          ),
        }),
      );
    } else {
      states.push(deepFreeze({ id: state.id, type: "terminal", result: state.result }));
    }
  }

  return deepFreeze({
    schema_version: 2,
    bundleRoot: rootCanonical,
    entry_state: spec.entry_state,
    max_transitions: spec.max_transitions,
    inputs: Object.freeze(inputs),
    outputs: Object.freeze(outputs),
    states: Object.freeze(states),
  });
}

/** Fixed container locations. Users and agents never name these paths. */
export const PROJECT_MOUNT_TARGET = "/workspace";
export const ACTIVATION_INPUTS_ROOT = "/pipeline/inputs";
export const ACTIVATION_OUTPUTS_ROOT = "/pipeline/outputs";

export interface ActivationInputPortPlan {
  readonly id: string;
  readonly source: PipelinePortSource;
  readonly type: PortType;
  readonly target: string;
  readonly read_only: true;
}

export interface ActivationOutputPortPlan {
  readonly id: string;
  readonly type: PortType;
  readonly target: string;
  readonly read_only: false;
  /** Parsed schema snapshot; present exactly for type json. */
  readonly schema?: Readonly<Record<string, unknown>>;
}

export interface ActivationLayoutPlan {
  readonly state_id: string;
  readonly project: { readonly target: string; readonly read_only: false };
  readonly inputs_root: string;
  readonly outputs_root: string;
  readonly input_ports: readonly ActivationInputPortPlan[];
  readonly output_ports: readonly ActivationOutputPortPlan[];
  readonly reject_undeclared_outputs: true;
}

/**
 * Build the immutable activation layout plan for one agent state of a
 * compiled v2 pipeline. Pure and deterministic: declaration order is
 * preserved, targets are fixed container paths, and every structured value
 * is cloned and frozen so later mutations of the pipeline object (or of the
 * returned view) cannot change the plan. The plan carries only logical and
 * structural data — no bearers, credentials, env values, or host paths.
 */
export function planActivationLayout(
  pipeline: ResolvedPipelineV2,
  stateId: string,
): ActivationLayoutPlan {
  const state = pipeline.states.find((entry) => entry.id === stateId);
  if (state === undefined) {
    throw new PipelineError(`state ${JSON.stringify(stateId)} is not declared by the pipeline`);
  }
  if (state.type !== "agent") {
    throw new PipelineError(
      `state ${JSON.stringify(stateId)} is not an agent state; activation layouts exist for agent states only`,
    );
  }
  const inputPorts: ActivationInputPortPlan[] = state.inputs.map((port) =>
    deepFreeze({
      id: port.id,
      source: structuredClone(port.source) as PipelinePortSource,
      type: port.type,
      target: `${ACTIVATION_INPUTS_ROOT}/${port.id}`,
      read_only: true,
    }),
  );
  const outputPorts: ActivationOutputPortPlan[] = state.outputs.map((port) =>
    port.schema === undefined
      ? deepFreeze({
          id: port.id,
          type: port.type,
          target: `${ACTIVATION_OUTPUTS_ROOT}/${port.id}`,
          read_only: false,
        })
      : deepFreeze({
          id: port.id,
          type: port.type,
          schema: structuredClone(port.schema) as Record<string, unknown>,
          target: `${ACTIVATION_OUTPUTS_ROOT}/${port.id}`,
          read_only: false,
        }),
  );
  return deepFreeze({
    state_id: stateId,
    project: deepFreeze({ target: PROJECT_MOUNT_TARGET, read_only: false }),
    inputs_root: ACTIVATION_INPUTS_ROOT,
    outputs_root: ACTIVATION_OUTPUTS_ROOT,
    input_ports: Object.freeze(inputPorts),
    output_ports: Object.freeze(outputPorts),
    reject_undeclared_outputs: true,
  });
}

/**
 * Session capability contract. Documented here and intentionally not wired
 * into the docker-helper transport yet.
 *
 * Execution Session: scope is the run root; used only by the orchestrator to
 * launch workers; its bearer stays with the orchestrator and is never passed
 * to a worker in any form.
 * Tool Session: scope is the project only; its bearer is handed to the
 * worker, which receives a projected helper socket; nested containers
 * launched through that socket cannot reach pipeline inputs/outputs.
 * No wide Tool Session workaround exists. The docker-helper#8 allowed-roots
 * refinement (RO/RW per mount) is not required by this increment.
 */
export interface SessionCapabilityContract {
  readonly kind: "execution" | "tool";
  readonly scope: "run_root" | "project";
  readonly bearer_shared_with_worker: boolean;
  readonly helper_socket_projected: boolean;
}

export const EXECUTION_SESSION_CONTRACT: SessionCapabilityContract = deepFreeze({
  kind: "execution",
  scope: "run_root",
  bearer_shared_with_worker: false,
  helper_socket_projected: false,
});

export const TOOL_SESSION_CONTRACT: SessionCapabilityContract = deepFreeze({
  kind: "tool",
  scope: "project",
  bearer_shared_with_worker: true,
  helper_socket_projected: true,
});

export interface WorkerMountPlan {
  readonly source: "project" | "prepared_activation_inputs" | "activation_outputs";
  readonly target: string;
  readonly read_only: boolean;
}

export const WORKER_MOUNT_CONTRACT: readonly WorkerMountPlan[] = Object.freeze([
  Object.freeze({ source: "project", target: PROJECT_MOUNT_TARGET, read_only: false }),
  Object.freeze({ source: "prepared_activation_inputs", target: ACTIVATION_INPUTS_ROOT, read_only: true }),
  Object.freeze({ source: "activation_outputs", target: ACTIVATION_OUTPUTS_ROOT, read_only: false }),
]) as readonly WorkerMountPlan[];
