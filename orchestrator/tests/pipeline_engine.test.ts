import { describe, expect, test } from "bun:test";
import {
  executePipelineGraph,
  PipelineExecutionError,
  type AgentOutcomeExecutor,
  type GraphExecutionResult,
  type TransitionStep,
} from "../src/pipeline_engine.ts";
import type {
  ResolvedAgentState,
  ResolvedPipeline,
  ResolvedState,
} from "../src/pipeline.ts";

function agentState(
  id: string,
  transitions: { outcome: string; to: string }[],
  overrides: Partial<ResolvedAgentState> = {},
): ResolvedAgentState {
  return {
    id,
    type: "agent",
    profile: "default",
    promptPath: `/bundle/${id}.md`,
    promptContent: `prompt for ${id}`,
    inputs: ["task"],
    resultSchemaPath: `/bundle/${id}.schema.json`,
    resultSchema: {},
    timeout_seconds: 60,
    max_attempts: 1,
    transitions,
    ...overrides,
  };
}

function terminalState(id: string, result: "success" | "failed"): ResolvedState {
  return { id, type: "terminal", result };
}

/**
 * Builds a structurally valid ResolvedPipeline by hand (the same shape the
 * YAML loader produces). No YAML loading is duplicated here; the engine must
 * only ever see resolved graphs.
 */
function syntheticPipeline(
  entryState: string,
  maxTransitions: number,
  states: ResolvedState[],
  overrides: Partial<ResolvedPipeline> = {},
): ResolvedPipeline {
  return {
    schema_version: 1,
    bundleRoot: "/bundle",
    entry_state: entryState,
    max_transitions: maxTransitions,
    inputs: [{ id: "task", path: "TASK.md", protected: true }],
    states,
    ...overrides,
  };
}

function scriptExecutor(
  script: Record<string, string>,
): AgentOutcomeExecutor & { seen: string[] } {
  const seen: string[] = [];
  const executor = (state: ResolvedAgentState): string => {
    seen.push(state.id);
    const outcome = script[state.id];
    if (outcome === undefined) {
      throw new Error(`script has no outcome for state ${JSON.stringify(state.id)}`);
    }
    return outcome;
  };
  return Object.assign(executor, { seen });
}

async function run(
  pipeline: ResolvedPipeline,
  executor: AgentOutcomeExecutor,
): Promise<GraphExecutionResult> {
  return await executePipelineGraph(pipeline, executor);
}

test("one agent -> success terminal", async () => {
  const executor = scriptExecutor({ a: "completed" });
  const result = await run(
    syntheticPipeline("a", 1, [agentState("a", [{ outcome: "completed", to: "done" }]), terminalState("done", "success")]),
    executor,
  );
  expect(result).toEqual({
    terminalStateId: "done",
    terminalResult: "success",
    transitionCount: 1,
    trace: [{ from: "a", outcome: "completed", to: "done", transition_index: 0 }],
  });
  expect(executor.seen).toEqual(["a"]);
});

test("arbitrary state ids", async () => {
  const result = await run(
    syntheticPipeline("st-begin", 1, [
      agentState("st-begin", [{ outcome: "done", to: "st-finish.1" }]),
      terminalState("st-finish.1", "success"),
    ]),
    scriptExecutor({ "st-begin": "done" }),
  );
  expect(result.terminalStateId).toBe("st-finish.1");
  expect(result.terminalResult).toBe("success");
  expect(result.trace[0]).toEqual({ from: "st-begin", outcome: "done", to: "st-finish.1", transition_index: 0 });
});

test("entry terminal with zero transitions and no callback execution", async () => {
  const executor = scriptExecutor({});
  const result = await run(syntheticPipeline("done", 3, [terminalState("done", "success")]), executor);
  expect(result).toEqual({
    terminalStateId: "done",
    terminalResult: "success",
    transitionCount: 0,
    trace: [],
  });
  expect(executor.seen).toEqual([]);
});

test("terminal result failed is a normal graph result, not an exception", async () => {
  const result = await run(
    syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "blocked", to: "halt" }]),
      terminalState("halt", "failed"),
    ]),
    scriptExecutor({ a: "blocked" }),
  );
  expect(result.terminalStateId).toBe("halt");
  expect(result.terminalResult).toBe("failed");
  expect(result.transitionCount).toBe(1);
});

test("sequence of two agent states", async () => {
  const executor = scriptExecutor({ first: "ok", second: "done" });
  const result = await run(
    syntheticPipeline("first", 2, [
      agentState("first", [{ outcome: "ok", to: "second" }]),
      agentState("second", [{ outcome: "done", to: "end" }]),
      terminalState("end", "success"),
    ]),
    executor,
  );
  expect(executor.seen).toEqual(["first", "second"]);
  expect(result.terminalStateId).toBe("end");
  expect(result.terminalResult).toBe("success");
  expect(result.transitionCount).toBe(2);
});

test("branching by different outcomes", async () => {
  const pipeline = syntheticPipeline("choose", 1, [
    agentState("choose", [
      { outcome: "left", to: "halt-left" },
      { outcome: "right", to: "halt-right" },
    ]),
    terminalState("halt-left", "failed"),
    terminalState("halt-right", "success"),
  ]);
  const left = await run(pipeline, scriptExecutor({ choose: "left" }));
  expect(left).toEqual({
    terminalStateId: "halt-left",
    terminalResult: "failed",
    transitionCount: 1,
    trace: [{ from: "choose", outcome: "left", to: "halt-left", transition_index: 0 }],
  });
  const right = await run(pipeline, scriptExecutor({ choose: "right" }));
  expect(right.terminalStateId).toBe("halt-right");
  expect(right.terminalResult).toBe("success");
  expect(right.trace[0]?.to).toBe("halt-right");
});

test("bounded cycle that then exits to the terminal", async () => {
  const executor = scriptExecutor({ worker: "retry", worker2: "retry", worker3: "finish" });
  const result = await run(
    syntheticPipeline("worker", 5, [
      agentState("worker", [
        { outcome: "retry", to: "worker2" },
        { outcome: "finish", to: "end" },
      ]),
      agentState("worker2", [
        { outcome: "retry", to: "worker3" },
        { outcome: "finish", to: "end" },
      ]),
      agentState("worker3", [
        { outcome: "retry", to: "worker" },
        { outcome: "finish", to: "end" },
      ]),
      terminalState("end", "success"),
    ]),
    executor,
  );
  expect(executor.seen).toEqual(["worker", "worker2", "worker3"]);
  expect(result.terminalStateId).toBe("end");
  expect(result.transitionCount).toBe(3);
  const trace: TransitionStep[] = result.trace;
  expect(trace.map((step) => step.from)).toEqual(["worker", "worker2", "worker3"]);
  expect(trace.every((step) => step.outcome === "retry")).toBe(false);
  expect(trace[2]?.outcome).toBe("finish");
});

test("success exactly at the max_transitions boundary", async () => {
  const result = await run(
    syntheticPipeline("a1", 2, [
      agentState("a1", [{ outcome: "next", to: "a2" }]),
      agentState("a2", [{ outcome: "next", to: "end" }]),
      terminalState("end", "success"),
    ]),
    scriptExecutor({ a1: "next", a2: "next" }),
  );
  expect(result.terminalResult).toBe("success");
  expect(result.transitionCount).toBe(2);
});

test("transition budget exhaustion inside a cycle", async () => {
  const executor = scriptExecutor({ loop: "again", loop2: "again" });
  const promise = run(
    syntheticPipeline("loop", 2, [
      agentState("loop", [{ outcome: "again", to: "loop2" }]),
      agentState("loop2", [{ outcome: "again", to: "loop" }]),
      terminalState("end", "success"),
    ]),
    executor,
  );
  await expect(promise).rejects.toThrow(PipelineExecutionError);
  const error: unknown = await promise.catch((cause) => cause);
  if (!(error instanceof PipelineExecutionError)) {
    throw new Error("expected PipelineExecutionError");
  }
  expect(error.reason).toBe("transition_budget_exhausted");
  expect(error.message).toContain("transition budget exhausted");
  // the third agent state is never executed once the budget is exhausted
  expect(executor.seen).toEqual(["loop", "loop2"]);
});

test("unknown outcome does not advance the graph", async () => {
  const executor = scriptExecutor({ a: "mystery" });
  const promise = run(
    syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }]),
      terminalState("done", "success"),
    ]),
    executor,
  );
  const error: unknown = await promise.catch((cause) => cause);
  if (!(error instanceof PipelineExecutionError)) {
    throw new Error("expected PipelineExecutionError");
  }
  expect(error.reason).toBe("unknown_outcome");
  expect(error.message).toContain('"mystery"');
  expect(error.message).toContain('"a"');
  expect(executor.seen).toEqual(["a"]);
});

test("empty and non-string outcomes are rejected", async () => {
  const pipeline = syntheticPipeline("a", 1, [
    agentState("a", [{ outcome: "completed", to: "done" }]),
    terminalState("done", "success"),
  ]);
  for (const badOutcome of ["", "   ", "\n\t"] as string[]) {
    const promise = run(pipeline, () => badOutcome);
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_outcome");
  }
  // a non-string return value from a misbehaving callback is rejected
  // defensively
  const badExecutor: AgentOutcomeExecutor = () => 42 as unknown as string;
  const promise = run(pipeline, badExecutor);
  const error: unknown = await promise.catch((cause) => cause);
  if (!(error instanceof PipelineExecutionError)) {
    throw new Error("expected PipelineExecutionError");
  }
  expect(error.reason).toBe("invalid_outcome");
  expect(error.message).toContain("never selects the next state");
});

test("callback exception propagates without recording a transition", async () => {
  const callbackFailure = new Error("agent run exploded");
  const seen: string[] = [];
  const promise = executePipelineGraph(
    syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }]),
      terminalState("done", "success"),
    ]),
    (state) => {
      seen.push(state.id);
      throw callbackFailure;
    },
  );
  const error: unknown = await promise.catch((cause) => cause);
  expect(error).toBe(callbackFailure);
  expect(seen).toEqual(["a"]);
});

test("transition trace is in exact execution order", async () => {
  const result = await run(
    syntheticPipeline("s1", 3, [
      agentState("s1", [{ outcome: "go", to: "s2" }]),
      agentState("s2", [{ outcome: "go", to: "s3" }]),
      agentState("s3", [{ outcome: "last", to: "finish" }]),
      terminalState("finish", "success"),
    ]),
    scriptExecutor({ s1: "go", s2: "go", s3: "last" }),
  );
  expect(result.trace).toEqual([
    { from: "s1", outcome: "go", to: "s2", transition_index: 0 },
    { from: "s2", outcome: "go", to: "s3", transition_index: 0 },
    { from: "s3", outcome: "last", to: "finish", transition_index: 0 },
  ]);
  expect(result.transitionCount).toBe(3);
});

test("the callback cannot hand the engine an arbitrary next state", async () => {
  // an outcome string that happens to be a state id is still treated purely
  // as an outcome: unknown -> no transition, graph does not move
  const executor = scriptExecutor({ a: "done" });
  const promise = run(
    syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }]),
      terminalState("done", "success"),
    ]),
    executor,
  );
  const error: unknown = await promise.catch((cause) => cause);
  if (!(error instanceof PipelineExecutionError)) {
    throw new Error("expected PipelineExecutionError");
  }
  expect(error.reason).toBe("unknown_outcome");
  // and even with a declared outcome, the target is the declared one, not one
  // the callback might prefer
  const result = await run(
    syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "declared-terminal" }]),
      terminalState("declared-terminal", "success"),
      terminalState("rogue", "success"),
    ]),
    scriptExecutor({ a: "completed" }),
  );
  expect(result.terminalStateId).toBe("declared-terminal");
});

describe("defensive fail-closed against internally contradictory resolved graphs", () => {
  test("duplicate state ids", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "ok", to: "done" }]),
      terminalState("done", "success"),
    ]);
    pipeline.states.push(terminalState("done", "success"));
    const promise = run(pipeline, scriptExecutor({ a: "ok" }));
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_graph");
    expect(error.message).toContain('state "done" is declared more than once');
  });

  test("entry_state does not name a declared state", async () => {
    const pipeline = syntheticPipeline("ghost", 1, [
      agentState("a", [{ outcome: "ok", to: "done" }]),
      terminalState("done", "success"),
    ]);
    const promise = run(pipeline, scriptExecutor({ a: "ok" }));
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_graph");
    expect(error.message).toContain("entry_state");
  });

  test("transition targets an unknown state", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "ok", to: "nowhere" }]),
      terminalState("done", "success"),
    ]);
    const promise = run(pipeline, scriptExecutor({ a: "ok" }));
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_graph");
    expect(error.message).toContain("targets unknown state");
  });

  test("duplicate outcome in one state", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [
        { outcome: "ok", to: "done" },
        { outcome: "ok", to: "halt" },
      ]),
      terminalState("done", "success"),
      terminalState("halt", "failed"),
    ]);
    const promise = run(pipeline, scriptExecutor({ a: "ok" }));
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_graph");
    expect(error.message).toContain("more than once");
  });

  test("no terminal state at all", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "ok", to: "a" }]),
    ]);
    const promise = run(pipeline, scriptExecutor({ a: "ok" }));
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_graph");
    expect(error.message).toContain("no terminal state");
  });

  test("empty states list", async () => {
    const pipeline = syntheticPipeline("a", 1, []);
    const promise = run(pipeline, scriptExecutor({}));
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_graph");
  });
});

test("engine results are deterministic: same inputs, same result", async () => {
  const build = () =>
    syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }]),
      terminalState("done", "success"),
    ]);
  const first = await run(build(), scriptExecutor({ a: "completed" }));
  const second = await run(build(), scriptExecutor({ a: "completed" }));
  expect(second).toEqual(first);
});
