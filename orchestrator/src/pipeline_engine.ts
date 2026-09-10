import type {
  ResolvedAgentState,
  ResolvedPipeline,
} from "./pipeline.ts";
import {
  requireResolvedPipelineV2Provenance,
  type ResolvedPipelineV2,
  type ResolvedV2AgentState,
  type ResolvedV2DecisionState,
} from "./pipeline_v2.ts";

/**
 * Pure graph execution core. It is the single owner of the
 * `cursor -> outcome -> transition -> next-state` mapping for any already
 * loaded pipeline: an executor callback reports only a validated outcome,
 * never the next state. No timestamps, no randomness, no I/O; execution is
 * bounded by the pipeline's `max_transitions`.
 *
 * The execution loop exists exactly once. Version-specific compile adapters
 * (`compileV1Graph` for `ResolvedPipeline`, `compileV2Graph` for
 * `ResolvedPipelineV2`) each build the same internal engine-owned snapshot:
 * `entry_state`, the fixed `max_transitions`, state ids/types, terminal
 * results, the ordered transitions with their original indices, and the
 * frozen, transition-free execution view each state hands to its callback.
 * V2 adds the agent/decision dispatch at compile time; the loop itself,
 * the outcome -> transition mapping, the transition budget, the frozen
 * `TransitionStep` construction, the commit-hook ordering and the cursor
 * movement are one shared implementation for both versions.
 *
 * Before the first callback the engine compiles an immutable, engine-owned
 * snapshot of exactly the data it needs to execute the graph. During
 * execution the engine reads transitions, terminal results, and the
 * transition budget only from that snapshot, so neither mutations of the
 * source pipeline nor of the callback view can redirect the graph.
 */

export type PipelineExecutionReason =
  | "invalid_graph"
  | "missing_state"
  | "unknown_outcome"
  | "invalid_outcome"
  | "invalid_executor"
  | "transition_budget_exhausted";

export class PipelineExecutionError extends Error {
  readonly reason: PipelineExecutionReason;

  constructor(reason: PipelineExecutionReason, message: string) {
    super(message);
    this.name = "PipelineExecutionError";
    this.reason = reason;
  }
}

/**
 * Execution view of a v1 agent state handed to the callback: a frozen fresh
 * copy of the data a step needs (identity, profile, prompt, inputs, result
 * schema, timeout, attempts). It deliberately contains no transitions.
 */
export interface AgentStateView {
  readonly id: string;
  readonly profile: string;
  readonly promptPath: string;
  readonly promptContent: string;
  readonly inputs: readonly string[];
  readonly resultSchemaPath: string;
  readonly resultSchema: ReadonlyJsonValue;
  readonly timeout_seconds: number;
  readonly max_attempts: number;
}

/**
 * Readonly JSON value type for data that crosses the engine/callback boundary
 * (the result schema is a JSON value, not a mutable Record).
 */
export type ReadonlyJsonValue =
  | string
  | number
  | boolean
  | null
  | readonly ReadonlyJsonValue[]
  | { readonly [key: string]: ReadonlyJsonValue };

export type AgentOutcomeExecutor = (
  state: AgentStateView,
) => string | Promise<string>;

/**
 * Execution view of a v2 agent state handed to `executeAgent`: a fresh frozen
 * copy of exactly the fields the v2 agent executor needs. It carries no
 * transitions, no target states, no data ports, no schemas and no runtime
 * authority; the engine never reads graph data back from the view.
 */
export interface V2AgentExecutionView {
  readonly type: "agent";
  readonly id: string;
  readonly profile: string;
  readonly promptPath: string;
  readonly promptContent: string;
  readonly timeout_seconds: number;
  readonly max_attempts: number;
}

/**
 * Execution view of a v2 decision state handed to `executeDecision`: only
 * identity. The engine never sees the decision model, facts, schemas or any
 * data; the production runner closes the trusted pipeline/run data over the
 * callback and returns only the selected outcome string.
 */
export interface V2DecisionExecutionView {
  readonly type: "decision";
  readonly id: string;
}

/**
 * Executors of a v2 pipeline graph. For an agent state only `executeAgent`
 * runs, for a decision state only `executeDecision`, for a terminal state
 * neither. Both return an outcome string; neither selects the next state —
 * the engine resolves the declared transition from its own snapshot. Both
 * functions are captured exactly once into an engine-owned snapshot before
 * graph compilation and before the first callback; reassigning properties
 * of the caller's object during the run cannot change the dispatch.
 */
export interface PipelineV2GraphExecutors {
  readonly executeAgent:
    (state: V2AgentExecutionView) => string | Promise<string>;

  readonly executeDecision:
    (state: V2DecisionExecutionView) => string | Promise<string>;
}

/** The engine-owned executor snapshot captured from the caller once. */
type CapturedV2Executors = {
  readonly agent: PipelineV2GraphExecutors["executeAgent"];
  readonly decision: PipelineV2GraphExecutors["executeDecision"];
};

export interface TransitionStep {
  from: string;
  outcome: string;
  to: string;
  transition_index: number;
}

/**
 * Trusted transition-commit hook, invoked by the engine after it resolved a
 * validated outcome against its immutable graph snapshot. The hook receives
 * the exact immutable TransitionStep and must persist it before the engine
 * allows the next state callback. A rejecting or throwing hook stops the
 * graph immediately: the transition never appears in the trace and the
 * cursor does not move. Two commit outcomes are possible. A `not_committed` failure
 * rejects before the durable write lands: the transition is not recorded
 * anywhere. A `durability_unknown` failure means the rename already landed:
 * the new candidate revision may already be visible on disk even though the
 * hook failed — execution stops immediately either way, the next agent
 * callback never runs, and the caller owns the durability-unknown handling.
 * The hook can never choose the target state and is never given the graph's
 * transitions.
 */
export type TransitionCommitHook = (step: TransitionStep) => void | Promise<void>;

export interface GraphExecutionOptions {
  readonly onTransitionCommit?: TransitionCommitHook;
}

export interface GraphExecutionResult {
  terminalStateId: string;
  terminalResult: "success" | "failed";
  transitionCount: number;
  trace: TransitionStep[];
}

interface CompiledTransition {
  outcome: string;
  to: string;
  index: number;
}

type CompiledAgentView = AgentStateView | V2AgentExecutionView;

/**
 * One compiled transition-bearing state. The `execute` closure binds the
 * version-specific executor to the state's frozen, transition-free view at
 * compile time: an agent state always runs only its agent executor, a
 * decision state only the decision executor, and the executor can return
 * just an outcome — never a target state.
 */
type CompiledExecutableState = {
  readonly id: string;
  readonly kind: "agent" | "decision";
  readonly view: CompiledAgentView | V2DecisionExecutionView;
  readonly transitions: readonly CompiledTransition[];
  readonly execute: () => string | Promise<string>;
};

type CompiledState =
  | CompiledExecutableState
  | { readonly id: string; readonly kind: "terminal"; readonly result: "success" | "failed" };

interface CompiledGraph {
  entryState: string;
  maxTransitions: number;
  states: Map<string, CompiledState>;
}

function contradiction(detail: string): PipelineExecutionError {
  return new PipelineExecutionError(
    "invalid_graph",
    `resolved pipeline graph is internally inconsistent: ${detail}`,
  );
}

function notAJsonValue(stateId: string, detail: string): PipelineExecutionError {
  return new PipelineExecutionError(
    "invalid_graph",
    `agent state ${JSON.stringify(stateId)} result schema is not a JSON value: ${detail}`,
  );
}

function stateKindLabel(kind: "agent" | "decision"): string {
  return kind === "decision" ? "decision state" : "agent state";
}

function isPlainJsonObject(value: object): boolean {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Deterministic, content-free rendering of a non-function executor value. */
function describeExecutorValue(value: unknown): string {
  if (value === null) {
    return "null";
  }
  return typeof value;
}

/**
 * Captures the v2 executor contract exactly once, before graph compilation
 * and before the first callback: both functions are read one time, checked
 * against the executor contract, and stored in an engine-owned frozen
 * snapshot. The compiler binds only the captured functions, so a
 * reassignment of the caller's `executors` object — inside a callback or
 * external while a callback is pending — cannot change the dispatch of a
 * running graph. The caller's object itself is never frozen or modified.
 */
function captureV2Executors(
  executors: PipelineV2GraphExecutors,
): CapturedV2Executors {
  const agent = executors.executeAgent;
  const decision = executors.executeDecision;
  if (typeof agent !== "function") {
    throw new PipelineExecutionError(
      "invalid_executor",
      `pipeline v2 graph executor contract violated: executors.executeAgent must be a function, got ${describeExecutorValue(agent)}`,
    );
  }
  if (typeof decision !== "function") {
    throw new PipelineExecutionError(
      "invalid_executor",
      `pipeline v2 graph executor contract violated: executors.executeDecision must be a function, got ${describeExecutorValue(decision)}`,
    );
  }
  return Object.freeze({ agent, decision });
}

/**
 * Minimal shape used defensively during transition compilation: corrupted
 * runtime objects must fail as an internal graph inconsistency, never as an
 * accidental TypeError.
 */
interface PipelineTransitionSpecLike {
  outcome?: unknown;
  to?: unknown;
}

/**
 * Builds a deep, recursively frozen snapshot of a JSON value. Cycles,
 * non-finite numbers, and any non-JSON value (functions, bigints, undefined,
 * symbols, exotic objects) of an artificially corrupted resolved pipeline are
 * rejected as an internal inconsistency; no accidental TypeError or
 * DataCloneError escapes.
 */
function snapshotJsonValue(
  stateId: string,
  value: unknown,
  onPath: Set<object>,
): ReadonlyJsonValue {
  if (value === null) {
    return null;
  }
  const kind = typeof value;
  if (kind === "string" || kind === "boolean") {
    return value as string | boolean;
  }
  if (kind === "number") {
    if (!Number.isFinite(value as number)) {
      throw notAJsonValue(stateId, "non-finite number");
    }
    return value as number;
  }
  if (kind === "bigint" || kind === "function" || kind === "undefined" || kind === "symbol") {
    throw notAJsonValue(stateId, `non-JSON value of type ${kind}`);
  }
  if (kind !== "object") {
    throw notAJsonValue(stateId, `unsupported value of type ${kind}`);
  }
  const source = value as object;
  if (onPath.has(source)) {
    throw notAJsonValue(stateId, "cyclic reference");
  }
  if (Array.isArray(source)) {
    onPath.add(source);
    try {
      const copy: ReadonlyJsonValue[] = [];
      for (let index = 0; index < source.length; index++) {
        copy.push(snapshotJsonValue(stateId, source[index], onPath));
      }
      return Object.freeze(copy);
    } finally {
      onPath.delete(source);
    }
  }
  if (!isPlainJsonObject(source)) {
    throw notAJsonValue(stateId, "non-JSON value (not a plain object or array)");
  }
  onPath.add(source);
  try {
    // A null-prototype copy keeps hostile keys such as "__proto__" as plain
    // own enumerable data properties: assignment through a `{}` literal would
    // hit the Object.prototype `__proto__` setter instead of creating the
    // field, silently dropping the key and polluting prototypes.
    const copy: Record<string, ReadonlyJsonValue> = Object.create(null) as Record<string, ReadonlyJsonValue>;
    for (const [key, nested] of Object.entries(source)) {
      Object.defineProperty(copy, key, {
        value: snapshotJsonValue(stateId, nested, onPath),
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    return Object.freeze(copy);
  } finally {
    onPath.delete(source);
  }
}

/**
 * Builds the frozen execution view of a v1 agent state: a fresh copy of the
 * data a step needs, deeply isolated from the source pipeline (the result
 * schema is deep-cloned and recursively frozen). It deliberately contains no
 * transitions; freezing it is an additional defense, the primary one being
 * that the engine never reads graph data from the view.
 */
function buildAgentStateView(state: ResolvedAgentState): AgentStateView {
  const view: AgentStateView = {
    id: state.id,
    profile: state.profile,
    promptPath: state.promptPath,
    promptContent: state.promptContent,
    inputs: Object.freeze(Array.isArray(state.inputs) ? [...state.inputs] : []),
    resultSchemaPath: state.resultSchemaPath,
    resultSchema: snapshotJsonValue(state.id, state.resultSchema, new Set()),
    timeout_seconds: state.timeout_seconds,
    max_attempts: state.max_attempts,
  };
  return Object.freeze(view);
}

/**
 * Builds the frozen, fresh execution view of a v2 agent state at graph
 * compile time. It contains exactly the runtime fields of the v2 agent
 * contract and deliberately no transitions, targets, transition indexes,
 * data ports, schemas or credentials.
 */
function buildV2AgentStateView(state: ResolvedV2AgentState): V2AgentExecutionView {
  const view: V2AgentExecutionView = {
    type: "agent",
    id: state.id,
    profile: state.profile,
    promptPath: state.promptPath,
    promptContent: state.promptContent,
    timeout_seconds: state.timeout_seconds,
    max_attempts: state.max_attempts,
  };
  return Object.freeze(view);
}

/**
 * Builds the frozen, fresh execution view of a v2 decision state: identity
 * only. Facts, the decision model, data paths and schemas never cross this
 * boundary; the outcome-producing executor is bound at compile time and the
 * callback returns only an outcome string.
 */
function buildV2DecisionStateView(state: ResolvedV2DecisionState): V2DecisionExecutionView {
  return Object.freeze({
    type: "decision",
    id: state.id,
  });
}

/**
 * Validates exactly the runtime graph data shared by both pipeline versions:
 * a non-empty states list, a positive safe integer transition budget, and a
 * non-empty entry state. This is not the bundle/path/profile/model/schema
 * validation of the pipeline loaders; it only guards the graph data itself
 * and runs before the first callback.
 */
function validateGraphPrologue(
  states: unknown,
  maxTransitions: unknown,
  entryState: unknown,
): void {
  if (!Array.isArray(states) || states.length === 0) {
    throw contradiction("the states list is empty");
  }
  if (
    typeof maxTransitions !== "number" ||
    !Number.isSafeInteger(maxTransitions) ||
    maxTransitions <= 0
  ) {
    throw contradiction(
      `max_transitions must be a positive safe integer, got ${JSON.stringify(maxTransitions)}`,
    );
  }
  if (typeof entryState !== "string" || entryState === "") {
    throw contradiction("entry_state must be a non-empty string");
  }
}

/**
 * Compiles the declared transitions of one transition-bearing state into the
 * engine-owned snapshot. The state-kind label only names the message; the
 * transition data itself is identical for v1 and v2.
 */
function compileTransitions(
  kind: "agent" | "decision",
  stateId: string,
  transitions: readonly unknown[],
): CompiledTransition[] {
  const label = stateKindLabel(kind);
  if (!Array.isArray(transitions)) {
    throw contradiction(
      `${label} ${JSON.stringify(stateId)} transitions must be an array`,
    );
  }
  const compiled: CompiledTransition[] = [];
  const outcomes = new Set<string>();
  for (let index = 0; index < transitions.length; index++) {
    const transition = transitions[index];
    if (
      transition === undefined ||
      transition === null ||
      typeof (transition as PipelineTransitionSpecLike).outcome !== "string" ||
      ((transition as PipelineTransitionSpecLike).outcome as string).trim() === ""
    ) {
      throw contradiction(
        `${label} ${JSON.stringify(stateId)} transition ${index} outcome must be a non-empty string`,
      );
    }
    const outcome = (transition as PipelineTransitionSpecLike).outcome as string;
    if (outcomes.has(outcome)) {
      throw contradiction(
        `state ${JSON.stringify(stateId)} declares outcome ${JSON.stringify(outcome)} more than once`,
      );
    }
    outcomes.add(outcome);
    if (
      typeof (transition as PipelineTransitionSpecLike).to !== "string" ||
      (transition as PipelineTransitionSpecLike).to === ""
    ) {
      throw contradiction(
        `${label} ${JSON.stringify(stateId)} transition ${index} target must be a non-empty string`,
      );
    }
    compiled.push({ outcome, to: (transition as PipelineTransitionSpecLike).to as string, index });
  }
  return compiled;
}

/**
 * Finishes the engine-owned graph snapshot: the entry state must exist,
 * every declared transition target must exist, and at least one terminal
 * state must be declared. The transition budget is captured as a primitive.
 */
function finishCompiledGraph(
  states: Map<string, CompiledState>,
  entryState: string,
  maxTransitions: number,
): CompiledGraph {
  const entry = states.get(entryState);
  if (entry === undefined) {
    throw contradiction(
      `entry_state ${JSON.stringify(entryState)} does not name a declared state`,
    );
  }
  for (const state of states.values()) {
    if (state.kind === "terminal") {
      continue;
    }
    for (const transition of state.transitions) {
      if (!states.has(transition.to)) {
        throw contradiction(
          `state ${JSON.stringify(state.id)} transition outcome ${JSON.stringify(transition.outcome)} targets unknown state ${JSON.stringify(transition.to)}`,
        );
      }
    }
  }
  let hasTerminal = false;
  for (const state of states.values()) {
    if (state.kind === "terminal") {
      hasTerminal = true;
      break;
    }
  }
  if (!hasTerminal) {
    throw contradiction("the pipeline declares no terminal state");
  }

  return {
    entryState: entry.id,
    maxTransitions,
    states,
  };
}

/**
 * v1 compilation adapter: validates the runtime graph shape of a resolved
 * v1 pipeline and compiles the immutable engine-owned snapshot, binding the
 * `executeAgent` callback into each agent state. Decision states cannot
 * exist in a v1 graph, so no decision executor is bound.
 */
function compileV1Graph(
  pipeline: ResolvedPipeline,
  executeAgent: AgentOutcomeExecutor,
): CompiledGraph {
  validateGraphPrologue(pipeline.states, pipeline.max_transitions, pipeline.entry_state);

  const states = new Map<string, CompiledState>();
  for (const state of pipeline.states) {
    if (typeof state.id !== "string" || state.id === "") {
      throw contradiction("every state id must be a non-empty string");
    }
    if (states.has(state.id)) {
      throw contradiction(`state ${JSON.stringify(state.id)} is declared more than once`);
    }
    const stateId: string = state.id;
    // `unknown` copy keeps the defensive type check alive against cast
    // inputs; TypeScript narrowing would collapse it to `never`
    const stateType: unknown = state.type;
    if (state.type === "terminal") {
      if (state.result !== "success" && state.result !== "failed") {
        throw contradiction(
          `terminal state ${JSON.stringify(stateId)} result must be "success" or "failed", got ${JSON.stringify(state.result)}`,
        );
      }
      states.set(state.id, { id: state.id, kind: "terminal", result: state.result });
      continue;
    }
    if (state.type !== "agent") {
      throw contradiction(
        `state ${JSON.stringify(stateId)} has unsupported type ${JSON.stringify(stateType)}, expected "agent" or "terminal"`,
      );
    }
    const transitions = compileTransitions("agent", stateId, state.transitions);
    const view = buildAgentStateView(state);
    states.set(stateId, {
      id: stateId,
      kind: "agent",
      view,
      transitions,
      execute: () => executeAgent(view),
    });
  }

  return finishCompiledGraph(states, pipeline.entry_state, pipeline.max_transitions);
}

/**
 * v2 compilation adapter: after the provenance gate and the executor
 * capture, the same engine-owned graph snapshot as v1, with one additional
 * state kind. An agent state binds the captured `executeAgent` function with
 * its frozen `V2AgentExecutionView`, a decision state only the captured
 * `executeDecision` with an identity-only frozen view. No second pipeline
 * compiler runs: the bundle, data-port, model and schema contracts belong to
 * `loadPipelineV2` and are not repeated here. After the capture no further
 * read of the caller's executors object happens.
 */
function compileV2Graph(
  pipeline: ResolvedPipelineV2,
  executors: CapturedV2Executors,
): CompiledGraph {
  const agentExecutor = executors.agent;
  const decisionExecutor = executors.decision;

  validateGraphPrologue(pipeline.states, pipeline.max_transitions, pipeline.entry_state);

  const states = new Map<string, CompiledState>();
  for (const state of pipeline.states) {
    if (typeof state.id !== "string" || state.id === "") {
      throw contradiction("every state id must be a non-empty string");
    }
    if (states.has(state.id)) {
      throw contradiction(`state ${JSON.stringify(state.id)} is declared more than once`);
    }
    const stateId: string = state.id;
    // `unknown` copy keeps the defensive type check alive against cast
    // inputs; TypeScript narrowing would collapse it to `never`
    const stateType: unknown = state.type;
    if (state.type === "terminal") {
      if (state.result !== "success" && state.result !== "failed") {
        throw contradiction(
          `terminal state ${JSON.stringify(stateId)} result must be "success" or "failed", got ${JSON.stringify(state.result)}`,
        );
      }
      states.set(state.id, { id: state.id, kind: "terminal", result: state.result });
      continue;
    }
    if (state.type === "decision") {
      const transitions = compileTransitions("decision", stateId, state.transitions);
      const view = buildV2DecisionStateView(state);
      states.set(stateId, {
        id: stateId,
        kind: "decision",
        view,
        transitions,
        execute: () => decisionExecutor(view),
      });
      continue;
    }
    if (state.type !== "agent") {
      throw contradiction(
        `state ${JSON.stringify(stateId)} has unsupported type ${JSON.stringify(stateType)}, expected "agent", "decision" or "terminal"`,
      );
    }
    const transitions = compileTransitions("agent", stateId, state.transitions);
    const view = buildV2AgentStateView(state);
    states.set(stateId, {
      id: stateId,
      kind: "agent",
      view,
      transitions,
      execute: () => agentExecutor(view),
    });
  }

  return finishCompiledGraph(states, pipeline.entry_state, pipeline.max_transitions);
}

function findTransition(
  state: CompiledExecutableState,
  outcome: string,
): CompiledTransition | undefined {
  for (const transition of state.transitions) {
    if (transition.outcome === outcome) {
      return transition;
    }
  }
  return undefined;
}

/**
 * The single execution loop of the project. It owns the outcome ->
 * transition -> next-state mapping for every already loaded pipeline: the
 * version-specific compile adapter binds each state's executor and frozen
 * view, and the loop reads transitions, the transition budget, and terminal
 * results only from the engine-owned snapshot. Both v1 and v2 graphs run
 * through this one loop — there is no second execution machine.
 */
async function runCompiledGraph(
  graph: CompiledGraph,
  onTransitionCommit: TransitionCommitHook | undefined,
): Promise<GraphExecutionResult> {
  const trace: TransitionStep[] = [];
  let cursor: string = graph.entryState;
  let transitionCount = 0;

  while (true) {
    const current = graph.states.get(cursor);
    if (current === undefined) {
      throw new PipelineExecutionError(
        "missing_state",
        `pipeline cursor ${JSON.stringify(cursor)} does not name a declared state`,
      );
    }
    if (current.kind === "terminal") {
      return {
        terminalStateId: current.id,
        terminalResult: current.result,
        transitionCount,
        trace,
      };
    }
    if (transitionCount >= graph.maxTransitions) {
      throw new PipelineExecutionError(
        "transition_budget_exhausted",
        `${stateKindLabel(current.kind)} ${JSON.stringify(current.id)} cannot execute: transition budget exhausted (${transitionCount} of ${graph.maxTransitions} transitions already applied)`,
      );
    }
    const outcome = await current.execute();
    if (typeof outcome !== "string" || outcome.trim() === "") {
      throw new PipelineExecutionError(
        "invalid_outcome",
        `${stateKindLabel(current.kind)} ${JSON.stringify(current.id)} produced an invalid outcome ${JSON.stringify(outcome)}; the ${current.kind} reports an outcome and never selects the next state`,
      );
    }
    const match = findTransition(current, outcome);
    if (match === undefined) {
      throw new PipelineExecutionError(
        "unknown_outcome",
        `${current.kind === "decision" ? "decision" : "agent"} result outcome ${JSON.stringify(outcome)} does not match any transition outcome of state ${JSON.stringify(current.id)}`,
      );
    }
    // The step is built and frozen by the engine from its own snapshot: the
    // callback selected only the outcome, never the target.
    const step: TransitionStep = Object.freeze({
      from: current.id,
      outcome,
      to: match.to,
      transition_index: match.index,
    });
    if (onTransitionCommit !== undefined) {
      await onTransitionCommit(step);
    }
    trace.push(step);
    transitionCount += 1;
    cursor = step.to;
  }
}

/**
 * Executes the v1 graph starting strictly at `entry_state`. For an agent
 * state the injected `executeAgent` callback is invoked with the frozen
 * execution view compiled before the run and must return a validated
 * outcome string; the engine resolves the declared transition by outcome
 * from its own snapshot and moves to the declared target state. When
 * `options.onTransitionCommit` is set, the engine hands the hook the exact
 * immutable TransitionStep and waits for it to complete before recording
 * the transition and starting the next state callback; a hook failure stops
 * the graph with no transition recorded. A terminal state ends the
 * execution. Callback failures propagate unchanged: no transition is
 * recorded and the cursor does not move. A terminal `result: "failed"` is a
 * normal graph result, not an engine error.
 */
export async function executePipelineGraph(
  pipeline: ResolvedPipeline,
  executeAgent: AgentOutcomeExecutor,
  options: GraphExecutionOptions = {},
): Promise<GraphExecutionResult> {
  const graph = compileV1Graph(pipeline, executeAgent);
  return runCompiledGraph(graph, options.onTransitionCommit);
}

/**
 * Executes a loaded pipeline v2 graph through the same single execution
 * loop as `executePipelineGraph`. The trusted `ResolvedPipelineV2` snapshot
 * is provenance-checked first (hand-built objects, casts, clones and
 * Proxies are rejected before any content is read), then both executor
 * functions are captured exactly once into an engine-owned snapshot — a
 * missing or non-function executor fails `invalid_executor` here, before
 * graph compilation and before any callback — and the version-specific
 * adapter compiles the same internal engine-owned snapshot: agent and
 * decision states dispatch to their captured executor with a frozen,
 * transition-free view, and the shared core owns the outcome resolution,
 * transition budget, commit-hook ordering and terminal selection.
 */
export async function executePipelineV2Graph(
  pipeline: ResolvedPipelineV2,
  executors: PipelineV2GraphExecutors,
  options: GraphExecutionOptions = {},
): Promise<GraphExecutionResult> {
  requireResolvedPipelineV2Provenance(pipeline, "executePipelineV2Graph");
  const captured = captureV2Executors(executors);
  const graph = compileV2Graph(pipeline, captured);
  return runCompiledGraph(graph, options.onTransitionCommit);
}
