import type {
  ResolvedAgentState,
  ResolvedPipeline,
} from "./pipeline.ts";

/**
 * Pure graph execution core. It owns the outcome -> transition -> next-state
 * mapping for any already loaded pipeline: the agent callback reports only a
 * validated outcome, never the next state. No timestamps, no randomness, no
 * I/O; execution is bounded by the pipeline's `max_transitions`.
 *
 * Before the first callback the engine compiles an immutable, engine-owned
 * snapshot of exactly the data it needs to execute the graph (`entry_state`,
 * `max_transitions`, state ids/types, terminal results, and the ordered
 * transitions with their original indices). During execution the engine reads
 * transitions, terminal results, and the transition budget only from that
 * snapshot, so neither mutations of the source `ResolvedPipeline` nor of the
 * callback view can redirect the graph.
 */

export type PipelineExecutionReason =
  | "invalid_graph"
  | "missing_state"
  | "unknown_outcome"
  | "invalid_outcome"
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
 * Execution view of an agent state handed to the callback: a frozen fresh
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
 * graph: the transition never appears in the trace, the cursor does not move,
 * and the error propagates unchanged. The hook can never choose the target
 * state and is never given the graph's transitions.
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

type CompiledState =
  | { id: string; type: "agent"; view: AgentStateView; transitions: readonly CompiledTransition[] }
  | { id: string; type: "terminal"; result: "success" | "failed" };

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

function isPlainJsonObject(value: object): boolean {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
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
 * Builds the frozen execution view of an agent state: a fresh copy of the
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
 * Compiles the engine-owned immutable execution graph and validates exactly
 * the runtime data needed for safe execution. This is not the bundle/path/
 * profile/result-schema validation of the pipeline loader; it only guards the
 * graph data itself and runs before the first callback.
 */
function compileGraph(pipeline: ResolvedPipeline): CompiledGraph {
  if (!Array.isArray(pipeline.states) || pipeline.states.length === 0) {
    throw contradiction("the states list is empty");
  }
  if (
    typeof pipeline.max_transitions !== "number" ||
    !Number.isSafeInteger(pipeline.max_transitions) ||
    pipeline.max_transitions <= 0
  ) {
    throw contradiction(
      `max_transitions must be a positive safe integer, got ${JSON.stringify(pipeline.max_transitions)}`,
    );
  }
  if (typeof pipeline.entry_state !== "string" || pipeline.entry_state === "") {
    throw contradiction("entry_state must be a non-empty string");
  }

  const states = new Map<string, CompiledState>();
  let hasTerminal = false;
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
      hasTerminal = true;
      states.set(state.id, { id: state.id, type: "terminal", result: state.result });
      continue;
    }
    if (state.type !== "agent") {
      throw contradiction(
        `state ${JSON.stringify(stateId)} has unsupported type ${JSON.stringify(stateType)}, expected "agent" or "terminal"`,
      );
    }
    if (!Array.isArray(state.transitions)) {
      throw contradiction(
        `agent state ${JSON.stringify(stateId)} transitions must be an array`,
      );
    }
    const transitions: CompiledTransition[] = [];
    const outcomes = new Set<string>();
    for (let index = 0; index < state.transitions.length; index++) {
      const transition = state.transitions[index];
      if (
        transition === undefined ||
        typeof transition.outcome !== "string" ||
        transition.outcome.trim() === ""
      ) {
        throw contradiction(
          `agent state ${JSON.stringify(stateId)} transition ${index} outcome must be a non-empty string`,
        );
      }
      if (outcomes.has(transition.outcome)) {
        throw contradiction(
          `state ${JSON.stringify(stateId)} declares outcome ${JSON.stringify(transition.outcome)} more than once`,
        );
      }
      outcomes.add(transition.outcome);
      if (typeof transition.to !== "string" || transition.to === "") {
        throw contradiction(
          `agent state ${JSON.stringify(stateId)} transition ${index} target must be a non-empty string`,
        );
      }
      transitions.push({ outcome: transition.outcome, to: transition.to, index });
    }
    states.set(stateId, {
      id: stateId,
      type: "agent",
      view: buildAgentStateView(state),
      transitions,
    });
  }

  const entry = states.get(pipeline.entry_state);
  if (entry === undefined) {
    throw contradiction(
      `entry_state ${JSON.stringify(pipeline.entry_state)} does not name a declared state`,
    );
  }
  for (const state of states.values()) {
    if (state.type !== "agent") {
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
  if (!hasTerminal) {
    throw contradiction("the pipeline declares no terminal state");
  }

  return {
    entryState: entry.id,
    maxTransitions: pipeline.max_transitions,
    states,
  };
}

function findTransition(
  state: CompiledState & { type: "agent" },
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
 * Executes the graph starting strictly at `entry_state`. For an agent state
 * the injected `executeAgent` callback is invoked with the frozen execution
 * view compiled before the run and must return a validated outcome string;
 * the engine resolves the declared transition by outcome from its own
 * snapshot and moves to the declared target state. When `options`
 * `.onTransitionCommit` is set, the engine hands the hook the exact immutable
 * TransitionStep and waits for it to complete before recording the
 * transition and starting the next state callback; a hook failure stops the
 * graph with no transition recorded. A terminal state ends the execution.
 * Callback failures propagate unchanged: no transition is recorded and the
 * cursor does not move. A terminal `result: "failed"` is a normal graph
 * result, not an engine error.
 */
export async function executePipelineGraph(
  pipeline: ResolvedPipeline,
  executeAgent: AgentOutcomeExecutor,
  options: GraphExecutionOptions = {},
): Promise<GraphExecutionResult> {
  const graph = compileGraph(pipeline);
  const onTransitionCommit = options.onTransitionCommit;

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
    if (current.type === "terminal") {
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
        `agent state ${JSON.stringify(current.id)} cannot execute: transition budget exhausted (${transitionCount} of ${graph.maxTransitions} transitions already applied)`,
      );
    }
    const outcome = await executeAgent(current.view);
    if (typeof outcome !== "string" || outcome.trim() === "") {
      throw new PipelineExecutionError(
        "invalid_outcome",
        `agent state ${JSON.stringify(current.id)} produced an invalid outcome ${JSON.stringify(outcome)}; the agent reports an outcome and never selects the next state`,
      );
    }
    const match = findTransition(current, outcome);
    if (match === undefined) {
      throw new PipelineExecutionError(
        "unknown_outcome",
        `agent result outcome ${JSON.stringify(outcome)} does not match any transition outcome of state ${JSON.stringify(current.id)}`,
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
