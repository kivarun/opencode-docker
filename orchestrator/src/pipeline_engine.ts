import type {
  ResolvedAgentState,
  ResolvedPipeline,
  ResolvedState,
} from "./pipeline.ts";

/**
 * Pure graph execution core. It owns the outcome -> transition -> next-state
 * mapping for any already loaded pipeline: the agent callback reports only a
 * validated outcome, never the next state. No timestamps, no randomness, no
 * I/O; execution is bounded by the pipeline's `max_transitions`.
 */

export type PipelineExecutionReason =
  | "invalid_graph"
  | "missing_state"
  | "unknown_outcome"
  | "invalid_outcome"
  | "transition_budget_exhausted"
  | "transition_exceeds_max_transitions";

export class PipelineExecutionError extends Error {
  readonly reason: PipelineExecutionReason;

  constructor(reason: PipelineExecutionReason, message: string) {
    super(message);
    this.name = "PipelineExecutionError";
    this.reason = reason;
  }
}

export type AgentOutcomeExecutor = (
  state: ResolvedAgentState,
) => string | Promise<string>;

export interface TransitionStep {
  from: string;
  outcome: string;
  to: string;
  transition_index: number;
}

export interface GraphExecutionResult {
  terminalStateId: string;
  terminalResult: "success" | "failed";
  transitionCount: number;
  trace: TransitionStep[];
}

/**
 * Defensive re-validation of an already resolved pipeline graph. `loadPipeline`
 * enforces the same rules, but the engine must stay fail-closed against any
 * internally contradictory resolved graph it is handed.
 */
function validateResolvedGraph(pipeline: ResolvedPipeline): Map<string, ResolvedState> {
  const contradiction = (detail: string): PipelineExecutionError =>
    new PipelineExecutionError("invalid_graph", `resolved pipeline graph is internally inconsistent: ${detail}`);

  if (!Array.isArray(pipeline.states) || pipeline.states.length === 0) {
    throw contradiction("the states list is empty");
  }
  const states = new Map<string, ResolvedState>();
  for (const state of pipeline.states) {
    if (states.has(state.id)) {
      throw contradiction(`state ${JSON.stringify(state.id)} is declared more than once`);
    }
    states.set(state.id, state);
  }
  if (!states.has(pipeline.entry_state)) {
    throw contradiction(
      `entry_state ${JSON.stringify(pipeline.entry_state)} does not name a declared state`,
    );
  }
  let hasTerminal = false;
  for (const state of states.values()) {
    if (state.type === "terminal") {
      hasTerminal = true;
      continue;
    }
    const outcomes = new Set<string>();
    for (const transition of state.transitions) {
      if (outcomes.has(transition.outcome)) {
        throw contradiction(
          `state ${JSON.stringify(state.id)} declares outcome ${JSON.stringify(transition.outcome)} more than once`,
        );
      }
      outcomes.add(transition.outcome);
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
  return states;
}

function findTransition(
  state: ResolvedAgentState,
  outcome: string,
): { transition: { outcome: string; to: string }; index: number } | undefined {
  for (let index = 0; index < state.transitions.length; index++) {
    const transition = state.transitions[index];
    if (transition !== undefined && transition.outcome === outcome) {
      return { transition, index };
    }
  }
  return undefined;
}

/**
 * Executes the graph starting strictly at `entry_state`. For an agent state
 * the injected `executeAgent` callback is invoked and must return a validated
 * outcome string; the engine resolves the declared transition by outcome and
 * moves to the declared target state. A terminal state ends the execution.
 * Callback failures propagate unchanged: no transition is recorded and the
 * cursor does not move. A terminal `result: "failed"` is a normal graph
 * result, not an engine error.
 */
export async function executePipelineGraph(
  pipeline: ResolvedPipeline,
  executeAgent: AgentOutcomeExecutor,
): Promise<GraphExecutionResult> {
  const states = validateResolvedGraph(pipeline);

  const trace: TransitionStep[] = [];
  let cursor: string = pipeline.entry_state;
  let transitionCount = 0;

  while (true) {
    const current = states.get(cursor);
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
    if (transitionCount >= pipeline.max_transitions) {
      throw new PipelineExecutionError(
        "transition_budget_exhausted",
        `agent state ${JSON.stringify(current.id)} cannot execute: transition budget exhausted (${transitionCount} of ${pipeline.max_transitions} transitions already applied)`,
      );
    }
    const outcome = await executeAgent(current);
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
    if (transitionCount + 1 > pipeline.max_transitions) {
      throw new PipelineExecutionError(
        "transition_exceeds_max_transitions",
        `applying outcome ${JSON.stringify(outcome)} of state ${JSON.stringify(current.id)} would exceed max_transitions (${pipeline.max_transitions})`,
      );
    }
    const target = match.transition.to;
    trace.push({
      from: current.id,
      outcome,
      to: target,
      transition_index: match.index,
    });
    transitionCount += 1;
    cursor = target;
  }
}
