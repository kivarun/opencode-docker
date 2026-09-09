import { describe, expect, test } from "bun:test";
import {
  executePipelineGraph,
  PipelineExecutionError,
  type AgentOutcomeExecutor,
  type AgentStateView,
  type ReadonlyJsonValue,
  type GraphExecutionResult,
  type TransitionCommitHook,
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
  const executor = (state: AgentStateView): string => {
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
  onTransitionCommit?: TransitionCommitHook,
): Promise<GraphExecutionResult> {
  return await executePipelineGraph(pipeline, executor, onTransitionCommit ? { onTransitionCommit } : {});
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

describe("deeply isolated result schema in the callback view", () => {
  function nestedSchemaPipeline(): { pipeline: ResolvedPipeline; sourceSchema: Record<string, unknown> } {
    const sourceSchema: Record<string, unknown> = {
      type: "object",
      properties: {
        status: { type: "string" },
        detail: {
          type: "array",
          items: { type: "string" },
        },
      },
      required: ["status", "summary"],
    };
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }], { resultSchema: sourceSchema }),
      terminalState("done", "success"),
    ]);
    return { pipeline, sourceSchema };
  }

  test("callback cannot mutate nested schema objects (properties.status)", async () => {
    const { pipeline, sourceSchema } = nestedSchemaPipeline();
    let seenSchema: unknown = null;
    const result = await run(pipeline, (state) => {
      seenSchema = state.resultSchema;
      const properties = (state.resultSchema as { properties: Record<string, unknown> }).properties;
      expect(() => {
        (properties.status as { type: string }).type = "hijacked";
      }).toThrow(TypeError);
      expect((sourceSchema.properties as Record<string, unknown>).status).toEqual({ type: "string" });
      return "completed";
    });
    expect(result.terminalResult).toBe("success");
    const properties = (seenSchema as unknown as { properties: Record<string, unknown> }).properties;
    expect(properties.status).toEqual({ type: "string" });
    expect(Object.isFrozen(properties)).toBe(true);
    expect(Object.isFrozen(properties.status)).toBe(true);
  });

  test("callback cannot push into nested required arrays", async () => {
    const { pipeline, sourceSchema } = nestedSchemaPipeline();
    const result = await run(pipeline, (state) => {
      const required = (state.resultSchema as { required: string[] }).required;
      expect(() => {
        required.push("rogue");
      }).toThrow(TypeError);
      expect(sourceSchema.required).toEqual(["status", "summary"]);
      return "completed";
    });
    expect(result.terminalResult).toBe("success");
  });

  test("callback cannot mutate nested arrays (items)", async () => {
    const { pipeline, sourceSchema } = nestedSchemaPipeline();
    const result = await run(pipeline, (state) => {
      const items = (state.resultSchema as {
        properties: { detail: { items: unknown } };
      }).properties.detail.items;
      expect(() => {
        (items as { type: string }).type = "number";
      }).toThrow(TypeError);
      const sourceItems = (sourceSchema.properties as Record<string, unknown>).detail as {
        items: unknown;
      };
      expect(sourceItems.items).toEqual({ type: "string" });
      return "completed";
    });
    expect(result.terminalResult).toBe("success");
  });

  test("external mutation of the source nested schema while the callback is pending does not reach the view", async () => {
    const { pipeline, sourceSchema } = nestedSchemaPipeline();
    const gate: { release: () => void } = { release: () => {} };
    let viewStatus: unknown = null;
    let viewSummary: unknown = null;
    const pending = executePipelineGraph(pipeline, async (state) => {
      // external mutation happens while this callback is blocked below
      await new Promise<void>((resolve) => {
        gate.release = resolve;
      });
      const properties = (state.resultSchema as { properties: Record<string, unknown> }).properties;
      viewStatus = properties.status;
      viewSummary = (state.resultSchema as { properties: Record<string, unknown> }).properties.summary;
      return "completed";
    });

    // mutate the source schema deeply while the callback is blocked
    (sourceSchema.properties as Record<string, unknown>).status = { type: "number" };
    const summary = { type: "string" } as unknown;
    (sourceSchema.properties as Record<string, unknown>).summary = summary;
    gate.release();

    await pending;
    expect(viewStatus).toEqual({ type: "string" });
    // a nested key added to the source after compilation is absent in the view
    expect(viewSummary).toBeUndefined();
  });

  test("attempted view mutations do not change the source pipeline", async () => {
    const { pipeline, sourceSchema } = nestedSchemaPipeline();
    const result = await run(pipeline, (state) => {
      expect(Object.isFrozen(state)).toBe(true);
      expect(() => {
        (state.resultSchema as { type: string }).type = "hijacked";
      }).toThrow(TypeError);
      return "completed";
    });
    expect(result.terminalResult).toBe("success");
    expect(sourceSchema.type).toBe("object");
  });

  test("corrupted non-JSON schemas are rejected as invalid_graph before the callback", async () => {
    const cases: unknown[] = [];
    // cyclic reference
    const cyclic: Record<string, unknown> = { type: "object" };
    cyclic.properties = cyclic;
    cases.push(cyclic);
    // function value
    cases.push({ type: "object", validate: () => true });
    // bigint value
    cases.push({ type: "object", max: 10n });
    // non-finite number
    cases.push({ type: "object", multipleOf: Number.NaN });
    // exotic non-plain object (Date)
    cases.push({ type: "object", pattern: new Date() });
    // Map instance
    cases.push({ type: "object", mapping: new Map() });
    // undefined property value
    cases.push({ type: "object", hole: undefined });
    for (const schema of cases) {
      let callbackRuns = 0;
      const pipeline = syntheticPipeline("a", 1, [
        agentState("a", [{ outcome: "completed", to: "done" }], {
          resultSchema: schema as Record<string, unknown>,
        }),
        terminalState("done", "success"),
      ]);
      const promise = executePipelineGraph(pipeline, () => {
        callbackRuns += 1;
        return "completed";
      });
      const error: unknown = await promise.catch((cause) => cause);
      if (!(error instanceof PipelineExecutionError)) {
        throw new Error("expected PipelineExecutionError");
      }
      expect(error.reason).toBe("invalid_graph");
      expect(error.message).toContain("result schema is not a JSON value");
      expect(callbackRuns).toBe(0);
    }
  });

  test("valid JSON schema values (array root, string root, null root) compile", async () => {
    for (const schema of [{ type: "object" }, ["flat"], "named-schema", null]) {
      const pipeline = syntheticPipeline("a", 1, [
        agentState("a", [{ outcome: "completed", to: "done" }], {
          resultSchema: schema as unknown as Record<string, unknown>,
        }),
        terminalState("done", "success"),
      ]);
      const result = await run(pipeline, (state) => {
        expect(state.resultSchema).toEqual(schema);
        return "completed";
      });
      expect(result.terminalResult).toBe("success");
    }
  });

  test("hostile JSON keys (__proto__, constructor, prototype) survive as own enumerable properties", async () => {
    // The exact shape a JSON.parse'd schema file produces: every key is a
    // plain own data property, including "__proto__". The source is built
    // from a raw JSON string on purpose — an object literal with a
    // "__proto__:" key would trigger the prototype setter and never create
    // the own property (that is precisely the bug class being guarded).
    const sourceSchema: Record<string, unknown> = JSON.parse(`{
      "type": "object",
      "__proto__": { "hijacked": true },
      "constructor": { "const": "safe" },
      "prototype": { "const": "safe" },
      "properties": { "status": { "type": "string" } }
    }`);
    // sanity: JSON.parse really produced them as own keys
    expect(Object.keys(sourceSchema).sort()).toEqual(
      ["__proto__", "constructor", "properties", "prototype", "type"],
    );
    expect((sourceSchema as { properties: { status: unknown } }).properties.status).toEqual({ type: "string" });

    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }], {
        resultSchema: sourceSchema,
      }),
      terminalState("done", "success"),
    ]);
    const result = await run(pipeline, (state) => {
      const schema = state.resultSchema as Record<string, unknown>;
      // every JSON key is an own enumerable property of the frozen view
      expect(Object.keys(schema).sort()).toEqual(
        ["__proto__", "constructor", "properties", "prototype", "type"],
      );
      expect(Object.getOwnPropertyDescriptor(schema, "__proto__")).toEqual({
        value: { hijacked: true },
        enumerable: true,
        writable: false,
        configurable: false,
      });
      expect(schema.constructor).toEqual({ const: "safe" } as unknown as Function);
      expect(schema.prototype).toEqual({ const: "safe" });
      expect(schema.__proto__).toEqual({ hijacked: true });
      // the view copy must not inherit from Object.prototype at all
      expect(Object.getPrototypeOf(schema)).toBe(null);
      expect(Object.getPrototypeOf(schema.properties as object)).toBe(null);
      // deep freeze
      expect(Object.isFrozen(schema)).toBe(true);
      expect(Object.isFrozen(schema.properties as object)).toBe(true);
      // source isolation: the source is unchanged and untriggered
      expect((sourceSchema as { properties: { status: unknown } }).properties.status).toEqual({ type: "string" });
      expect(() => {
        (schema as { type: string }).type = "hijacked";
      }).toThrow(TypeError);
      // the prototype machinery itself is untouched
      expect(({} as { constructor: unknown }).constructor).toBe(Object);
      expect(({} as { hasOwnProperty: unknown }).hasOwnProperty).toBe(
        Object.prototype.hasOwnProperty,
      );
      const probe: Record<string, unknown> = {};
      expect(Object.getPrototypeOf(probe)).toBe(Object.prototype);
      expect(Object.keys(probe)).toEqual([]);
      return "completed";
    });
    expect(result.terminalResult).toBe("success");
    // the source schema keeps its own keys, and Object.prototype is clean
    expect(Object.keys(sourceSchema).sort()).toEqual(
      ["__proto__", "constructor", "properties", "prototype", "type"],
    );
    expect(Object.keys(Object.prototype).length).toBe(0);
    expect(Object.getOwnPropertyNames(Object.prototype)).toEqual(
      Object.getOwnPropertyNames(Object.prototype).filter((name) => name !== "hijacked"),
    );
  });
});

describe("trusted transition-commit hook", () => {
  const twoStepPipeline = () =>
    syntheticPipeline("first", 2, [
      agentState("first", [{ outcome: "ok", to: "second" }]),
      agentState("second", [{ outcome: "done", to: "end" }]),
      terminalState("end", "success"),
    ]);

  test("the hook receives the exact engine-produced immutable transition", async () => {
    const steps: TransitionStep[] = [];
    const result = await run(
      syntheticPipeline("a", 1, [
        agentState("a", [{ outcome: "completed", to: "declared" }]),
        terminalState("declared", "success"),
      ]),
      scriptExecutor({ a: "completed" }),
      (step) => {
        expect(Object.isFrozen(step)).toBe(true);
        steps.push(step);
      },
    );
    expect(steps).toEqual([
      { from: "a", outcome: "completed", to: "declared", transition_index: 0 },
    ]);
    expect(result.trace).toEqual(steps);
    expect(result.terminalStateId).toBe("declared");
  });

  test("the second agent callback does not start before the hook completes", async () => {
    let releaseHook: (() => void) | null = null;
    const hookGate = new Promise<void>((resolve) => {
      releaseHook = resolve;
    });
    const events: string[] = [];
    const pending = executePipelineGraph(
      twoStepPipeline(),
      (state) => {
        events.push(`callback:${state.id}`);
        return state.id === "first" ? "ok" : "done";
      },
      {
        onTransitionCommit: async (step) => {
          events.push(`hook:${step.from}->${step.to}`);
          await hookGate;
          events.push(`hook-done:${step.from}->${step.to}`);
        },
      },
    );
    // let the microtasks settle: the first callback ran, the hook is pending
    await Bun.sleep(5);
    expect(events).toEqual(["callback:first", "hook:first->second"]);
    releaseHook!();
    const result = await pending;
    expect(events).toEqual([
      "callback:first",
      "hook:first->second",
      "hook-done:first->second",
      "callback:second",
      "hook:second->end",
      "hook-done:second->end",
    ]);
    expect(result.transitionCount).toBe(2);
  });

  test("a rejecting hook stops the graph and propagates unchanged", async () => {
    const hookFailure = new Error("durable transition commit failed");
    let callbackRuns = 0;
    const promise = executePipelineGraph(
      twoStepPipeline(),
      (state) => {
        callbackRuns += 1;
        return state.id === "first" ? "ok" : "done";
      },
      {
        onTransitionCommit: (step) => {
          if (step.from === "first") {
            return Promise.reject(hookFailure);
          }
          throw new Error("the second callback must never run");
        },
      },
    );
    const error: unknown = await promise.catch((cause) => cause);
    expect(error).toBe(hookFailure);
    expect(callbackRuns).toBe(1);
  });

  test("a throwing hook stops the graph; the transition is not recorded", async () => {
    const hookFailure = new Error("store exploded");
    let callbackRuns = 0;
    const promise = executePipelineGraph(
      syntheticPipeline("a", 1, [
        agentState("a", [{ outcome: "completed", to: "done" }]),
        terminalState("done", "success"),
      ]),
      (state) => {
        callbackRuns += 1;
        expect(state.id).toBe("a");
        return "completed";
      },
      {
        onTransitionCommit: () => {
          throw hookFailure;
        },
      },
    );
    const error: unknown = await promise.catch((cause) => cause);
    expect(error).toBe(hookFailure);
    expect(error).not.toBeInstanceOf(PipelineExecutionError);
    expect(callbackRuns).toBe(1);
    // no result and no trace is observable: the graph never finished
  });

  test("the hook cannot select the target state or mutate the recorded step", async () => {
    const steps: TransitionStep[] = [];
    const result = await run(
      syntheticPipeline("a", 1, [
        agentState("a", [{ outcome: "completed", to: "declared" }]),
        terminalState("declared", "success"),
        terminalState("rogue", "success"),
      ]),
      scriptExecutor({ a: "completed" }),
      (step) => {
        // any attempt to redirect the graph from the hook has no effect: the
        // step is a frozen plain value and the engine owns the mapping
        steps.push({ ...step, to: "rogue" });
      },
    );
    expect(result.terminalStateId).toBe("declared");
    expect(result.trace[0]?.to).toBe("declared");
    expect(steps[0]?.to).toBe("rogue"); // the hook's own copy changed, nothing else
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

describe("mutation resistance: the engine owns the compiled graph", () => {
  test("callback mutation of the original pipeline cannot redirect the graph", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "declared" }]),
      terminalState("declared", "success"),
    ]);
    const result = await run(pipeline, (state) => {
      // mutate the source graph data from inside the callback
      const agent = pipeline.states[0];
      if (agent !== undefined && agent.type === "agent") {
        agent.transitions[0] = { outcome: "completed", to: "rogue" };
      }
      const terminal = pipeline.states[1];
      if (terminal !== undefined && terminal.type === "terminal") {
        terminal.result = "failed";
      }
      expect(state.id).toBe("a");
      return "completed";
    });
    expect(result).toEqual({
      terminalStateId: "declared",
      terminalResult: "success",
      transitionCount: 1,
      trace: [{ from: "a", outcome: "completed", to: "declared", transition_index: 0 }],
    });
  });

  test("callback mutation of max_transitions does not change the budget", async () => {
    const pipeline = syntheticPipeline("a1", 2, [
      agentState("a1", [{ outcome: "next", to: "a2" }]),
      agentState("a2", [{ outcome: "next", to: "end" }]),
      terminalState("end", "success"),
    ]);
    const result = await run(pipeline, (state) => {
      pipeline.max_transitions = 0;
      expect(state.id).toMatch(/a[12]/);
      return "next";
    });
    // the compiled budget is 2; the terminal is still reached at the boundary
    expect(result.terminalStateId).toBe("end");
    expect(result.terminalResult).toBe("success");
    expect(result.transitionCount).toBe(2);
  });

  test("external mutation while an async callback is pending stays bound to the snapshot", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [
        { outcome: "completed", to: "declared" },
        { outcome: "halt", to: "halt-terminal" },
      ]),
      terminalState("declared", "success"),
      terminalState("halt-terminal", "failed"),
    ]);
    const gate: { release: () => void } = { release: () => {} };
    let callbackRuns = 0;
    const pending = executePipelineGraph(pipeline, async (state) => {
      callbackRuns += 1;
      expect(state.id).toBe("a");
      await new Promise<void>((resolve) => {
        gate.release = resolve;
      });
      return "completed";
    });

    // while the callback promise is pending, external code mutates every
    // piece of graph data the engine is supposed to own
    const agent = pipeline.states[0];
    if (agent === undefined || agent.type !== "agent") {
      throw new Error("expected the agent state");
    }
    agent.transitions.reverse();
    agent.transitions[0] = { outcome: "completed", to: "rogue" };
    const declared = pipeline.states[1];
    if (declared === undefined || declared.type !== "terminal") {
      throw new Error("expected the declared terminal");
    }
    declared.result = "failed";
    pipeline.max_transitions = 0;
    gate.release();

    const result = await pending;
    expect(callbackRuns).toBe(1);
    expect(result).toEqual({
      terminalStateId: "declared",
      terminalResult: "success",
      transitionCount: 1,
      trace: [{ from: "a", outcome: "completed", to: "declared", transition_index: 0 }],
    });
  });

  test("the callback view is frozen, transition-free, and isolated from the source", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }]),
      terminalState("done", "success"),
    ]);
    const result = await run(pipeline, (state) => {
      expect("transitions" in state).toBe(false);
      expect(state.id).toBe("a");
      expect(state.inputs).toEqual(["task"]);
      // frozen: view mutations cannot even happen
      expect(() => {
        (state as { id: string }).id = "hijacked";
      }).toThrow();
      expect(() => {
        (state.inputs as string[]).push("extra");
      }).toThrow();
      // source mutation cannot reach the view either
      const agent = pipeline.states[0];
      if (agent !== undefined && agent.type === "agent") {
        agent.promptContent = "tampered";
        agent.inputs.push("sneaky");
      }
      expect(state.promptContent).toBe("prompt for a");
      expect(state.inputs).toEqual(["task"]);
      return "completed";
    });
    expect(result.terminalResult).toBe("success");
  });
});

describe("defensive compile-time bounds (invalid_graph before any callback)", () => {
  test("invalid max_transitions values are rejected", async () => {
    const cases: unknown[] = [0, -3, 1.5, NaN, Infinity, 2 ** 53];
    for (const maxTransitions of cases) {
      let callbackRuns = 0;
      const pipeline = syntheticPipeline("a", 1, [
        agentState("a", [{ outcome: "completed", to: "done" }]),
        terminalState("done", "success"),
      ]);
      pipeline.max_transitions = maxTransitions as number;
      const promise = executePipelineGraph(pipeline, () => {
        callbackRuns += 1;
        return "completed";
      });
      const error: unknown = await promise.catch((cause) => cause);
      if (!(error instanceof PipelineExecutionError)) {
        throw new Error(`expected PipelineExecutionError for ${String(maxTransitions)}`);
      }
      expect(error.reason).toBe("invalid_graph");
      expect(error.message).toContain("max_transitions");
      expect(callbackRuns).toBe(0);
    }
  });

  test("invalid runtime state shapes are rejected, not a random TypeError", async () => {
    // unsupported state type
    const wrongType = syntheticPipeline("x", 1, [
      { id: "x", type: "banana" } as unknown as ResolvedState,
      terminalState("done", "success"),
    ]);
    // garbage terminal result
    const wrongResult = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }]),
      { id: "done", type: "terminal", result: "weird" } as unknown as ResolvedState,
    ]);
    // transitions not an array
    const wrongTransitions = syntheticPipeline("a", 1, [
      {
        ...agentState("a", []),
        transitions: "nope" as unknown as [{ outcome: string; to: string }],
      },
      terminalState("done", "success"),
    ]);
    // empty outcome
    const emptyOutcome = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "", to: "done" } as { outcome: string; to: string }]),
      terminalState("done", "success"),
    ]);
    // non-string state id
    const wrongId = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: "done" }]),
      terminalState("done", "success"),
    ]);
    (wrongId.states[1] as unknown as { id: unknown }).id = 42;
    const pipelines = [wrongType, wrongResult, wrongTransitions, emptyOutcome, wrongId];
    for (const pipeline of pipelines) {
      let callbackRuns = 0;
      const promise = executePipelineGraph(pipeline, () => {
        callbackRuns += 1;
        return "completed";
      });
      const error: unknown = await promise.catch((cause) => cause);
      if (!(error instanceof PipelineExecutionError)) {
        throw new Error("expected PipelineExecutionError");
      }
      expect(error.reason).toBe("invalid_graph");
      expect(callbackRuns).toBe(0);
    }
  });

  test("non-string transition target is rejected", async () => {
    const pipeline = syntheticPipeline("a", 1, [
      agentState("a", [{ outcome: "completed", to: 42 } as unknown as { outcome: string; to: string }]),
      terminalState("done", "success"),
    ]);
    let callbackRuns = 0;
    const promise = executePipelineGraph(pipeline, () => {
      callbackRuns += 1;
      return "completed";
    });
    const error: unknown = await promise.catch((cause) => cause);
    if (!(error instanceof PipelineExecutionError)) {
      throw new Error("expected PipelineExecutionError");
    }
    expect(error.reason).toBe("invalid_graph");
    expect(callbackRuns).toBe(0);
  });
});
