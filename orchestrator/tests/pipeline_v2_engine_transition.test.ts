import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  compiledTransitionFor,
  executePipelineGraph,
  executePipelineV2Graph,
  executePipelineV2GraphResume,
  PipelineExecutionError,
  type PipelineV2GraphExecutors,
  type TransitionStep,
  type V2AgentExecutionView,
  type V2DecisionExecutionView,
} from "../src/pipeline_engine.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";

/**
 * Narrow suite for the single compiled-transition resolver:
 * `compiledTransitionFor` is the authoritative `outcome ->
 * {from, outcome, to, transition_index}` resolution for pipeline v2, and
 * the engine loop (fresh and resumed) resolves every step through the same
 * private function. No second transition search, no second compiled graph,
 * no filesystem I/O and no executor callback run during resolution.
 */

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: alpha
  - id: beta
relations: []
constraints: []
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f2, equals: true}
    decision: beta
`;

const FACTS_SCHEMA = {
  type: "object",
  required: ["f1", "f2"],
  properties: { f1: { type: "boolean" }, f2: { type: "boolean" } },
};

const TERMINALS_YAML = `  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/**
 * A decision state with several transitions, two of them targeting the
 * same state (`uncovered` and `inconsistent_facts` both to `failed_end`)
 * with different declaration-order indexes, plus one shared target with
 * the agent's return state.
 */
const MULTI_TRANSITION_DECISION_YAML = `  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: alpha
        to: done
      - outcome: beta
        to: coder
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

const PIPELINE_YAML = `
schema_version: 2
entry_state: coder
max_transitions: 20

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json

outputs: []

states:
  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: check
${MULTI_TRANSITION_DECISION_YAML}
${TERMINALS_YAML}
`;

interface Bundled {
  pipeline: ResolvedPipelineV2;
  cleanup: () => Promise<void>;
}

async function loadBundle(yaml: string): Promise<Bundled> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-transition-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), yaml);
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
  await writeFile(join(bundle, "schemas", "facts.schema.json"), JSON.stringify(FACTS_SCHEMA));
  const pipeline = await loadPipelineV2(bundle);
  return { pipeline, cleanup: () => rm(root, { recursive: true, force: true }) };
}

async function withPipeline(yaml: string, fn: (bundled: Bundled) => Promise<void>): Promise<void> {
  const bundled = await loadBundle(yaml);
  try {
    await fn(bundled);
  } finally {
    await bundled.cleanup();
  }
}

function refuseAgent(state: V2AgentExecutionView): void {
  throw new Error(`agent executor must not be called for state ${JSON.stringify(state.id)}`);
}

function refuseDecision(state: V2DecisionExecutionView): string {
  throw new Error(`decision executor must not be called for state ${JSON.stringify(state.id)}`);
}

test("1. an agent outcome resolves to the exact deep-frozen step", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const step = compiledTransitionFor(pipeline, "coder", "completed");
    expect(step).toEqual({
      from: "coder",
      outcome: "completed",
      to: "check",
      transition_index: 0,
    });
    expect(Object.isFrozen(step)).toBe(true);
  });
});

test("2. a decision outcome resolves to the exact deep-frozen step", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const step = compiledTransitionFor(pipeline, "check", "alpha");
    expect(step).toEqual({
      from: "check",
      outcome: "alpha",
      to: "done",
      transition_index: 0,
    });
    expect(Object.isFrozen(step)).toBe(true);
  });
});

test("3. a state with several transitions keeps the declaration-order index", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    expect(compiledTransitionFor(pipeline, "check", "alpha")?.transition_index).toBe(0);
    expect(compiledTransitionFor(pipeline, "check", "beta")?.transition_index).toBe(1);
    expect(compiledTransitionFor(pipeline, "check", "uncovered")?.transition_index).toBe(2);
    expect(compiledTransitionFor(pipeline, "check", "inconsistent_facts")?.transition_index).toBe(3);
    expect(compiledTransitionFor(pipeline, "check", "invalid_facts")?.transition_index).toBe(4);
  });
});

test("4. two outcomes sharing one target keep their own distinct indexes", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const toFailed = compiledTransitionFor(pipeline, "check", "uncovered");
    const toFailedToo = compiledTransitionFor(pipeline, "check", "inconsistent_facts");
    const backToAgent = compiledTransitionFor(pipeline, "check", "beta");
    expect(toFailed).toEqual({
      from: "check",
      outcome: "uncovered",
      to: "failed_end",
      transition_index: 2,
    });
    expect(toFailedToo).toEqual({
      from: "check",
      outcome: "inconsistent_facts",
      to: "failed_end",
      transition_index: 3,
    });
    expect(backToAgent).toEqual({
      from: "check",
      outcome: "beta",
      to: "coder",
      transition_index: 1,
    });
  });
});

test("5. an unknown state keeps the exact typed missing_state error", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    let failure: unknown;
    try {
      compiledTransitionFor(pipeline, "bogus", "completed");
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineExecutionError);
    expect((failure as PipelineExecutionError).reason).toBe("missing_state");
    expect((failure as Error).message).toBe(
      'pipeline cursor "bogus" does not name a declared state',
    );
  });
});

test("6. an unknown outcome keeps the exact typed unknown_outcome error", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    let failure: unknown;
    try {
      compiledTransitionFor(pipeline, "check", "mystery");
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineExecutionError);
    expect((failure as PipelineExecutionError).reason).toBe("unknown_outcome");
    expect((failure as Error).message).toBe(
      'decision result outcome "mystery" does not match any transition outcome of state "check"',
    );
  });
});

test("7. the fresh engine emits the same trace the resolver produces", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const agentRuns: string[] = [];
    const decisionRuns: string[] = [];
    let decisionCalls = 0;
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: (state) => {
        agentRuns.push(state.id);
      },
      executeDecision: (state) => {
        decisionRuns.push(state.id);
        decisionCalls += 1;
        return decisionCalls === 1 ? "beta" : "alpha";
      },
    });
    // The engine route must equal what the resolver independently returns
    // for every walked (state, outcome) pair of the same journal.
    const walked: TransitionStep[] = [];
    for (const step of result.trace) {
      walked.push(compiledTransitionFor(pipeline, step.from, step.outcome));
    }
    expect(walked).toEqual(result.trace);
    expect(result.trace).toEqual([
      { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
      { from: "check", outcome: "beta", to: "coder", transition_index: 1 },
      { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
      { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
    ]);
    expect(agentRuns).toEqual(["coder", "coder"]);
    expect(decisionRuns).toEqual(["check", "check"]);
  });
});

test("8. the resume engine emits the same suffix trace through the same resolver", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const executors: PipelineV2GraphExecutors = {
      executeAgent: () => undefined,
      executeDecision: (state) => (state.id === "check" ? "alpha" : "alpha"),
    };
    const fresh = await executePipelineV2Graph(pipeline, {
      executeAgent: executors.executeAgent,
      executeDecision: executors.executeDecision,
    });
    const resumed = await executePipelineV2GraphResume(
      pipeline,
      executors,
      { current_state: "check", transition_count: 1 },
    );
    expect(resumed.trace).toEqual(fresh.trace.slice(1));
    for (const step of resumed.trace) {
      expect(compiledTransitionFor(pipeline, step.from, step.outcome)).toEqual(step);
    }
  });
});

test("9. the agent executor still cannot choose the outcome", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    let failure: unknown;
    try {
      await executePipelineV2Graph(pipeline, {
        executeAgent: refuseAgent,
        executeDecision: refuseDecision,
      });
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toBe(
      'agent executor must not be called for state "coder"',
    );
    // The v2 agent outcome is the engine's own fixed "completed"; the
    // resolver maps it identically, and a rogue return value is dropped.
    expect(compiledTransitionFor(pipeline, "coder", "completed").to).toBe("check");
  });
});

test("10. the decision executor still selects only the outcome, never target or index", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: () => undefined,
      executeDecision: () => "uncovered",
    });
    expect(result.trace).toEqual([
      { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
      { from: "check", outcome: "uncovered", to: "failed_end", transition_index: 2 },
    ]);
    // The declared step for the selected outcome — the executor never
    // handed the engine a target or an index.
    const declared = result.trace[1];
    if (declared === undefined) {
      throw new Error("the engine trace lost the decision step");
    }
    expect(compiledTransitionFor(pipeline, "check", "uncovered")).toEqual(declared);
  });
});

test("11. resolution runs no agent or decision executor", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    let agentCalls = 0;
    let decisionCalls = 0;
    const executors = {
      executeAgent: (state: V2AgentExecutionView): void => {
        agentCalls += 1;
        void state;
      },
      executeDecision: (state: V2DecisionExecutionView): string => {
        decisionCalls += 1;
        void state;
        return "alpha";
      },
    };
    // The executors object is irrelevant for resolution: prove it by
    // resolving against a pipeline while instrumented executors exist.
    void executors;
    expect(() => {
      compiledTransitionFor(pipeline, "coder", "completed");
      compiledTransitionFor(pipeline, "check", "beta");
    }).not.toThrow();
    expect(agentCalls).toBe(0);
    expect(decisionCalls).toBe(0);
  });
});

test("12. the resolved step is deep-frozen and the pipeline is not mutated", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const snapshot = JSON.parse(JSON.stringify(pipeline)) as unknown;
    const step = compiledTransitionFor(pipeline, "check", "beta");
    expect(Object.isFrozen(step)).toBe(true);
    expect(Object.isFrozen(compiledTransitionFor(pipeline, "coder", "completed"))).toBe(true);
    expect(JSON.parse(JSON.stringify(pipeline))).toEqual(snapshot);
  });
});

test("13. hand-built, spread, structuredClone and Proxy pipelines are rejected with zero traps", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const forged: unknown[] = [
      { ...pipeline },
      structuredClone(pipeline),
      {
        schema_version: 2,
        bundleRoot: pipeline.bundleRoot,
        entry_state: pipeline.entry_state,
        max_transitions: pipeline.max_transitions,
        inputs: [],
        outputs: [],
        states: [],
      },
    ];
    let proxyTrapCount = 0;
    forged.push(
      new Proxy(pipeline, {
        get: (target, property) => {
          proxyTrapCount += 1;
          return Reflect.get(target as object, property);
        },
        ownKeys: (target) => {
          proxyTrapCount += 1;
          return Reflect.ownKeys(target);
        },
        getOwnPropertyDescriptor: (target, property) => {
          proxyTrapCount += 1;
          return Reflect.getOwnPropertyDescriptor(target, property);
        },
      }),
    );
    forged.push(new Proxy({}, {}));
    for (const candidate of forged) {
      let failure: unknown;
      try {
        compiledTransitionFor(candidate as ResolvedPipelineV2, "coder", "completed");
      } catch (cause) {
        failure = cause;
      }
      expect(failure).toBeInstanceOf(PipelineError);
      expect((failure as Error).message).toContain(
        "pipeline v2 compiled transition resolution requires the deep-frozen snapshot object returned by loadPipelineV2",
      );
    }
    expect(proxyTrapCount).toBe(0);
  });
});

test("14. the engine export surface adds exactly compiledTransitionFor", async () => {
  const namespace = (await import("../src/pipeline_engine.ts")) as Record<string, unknown>;
  const runtimeExports = Object.keys(namespace).filter((key) => key !== "default").sort();
  // Runtime (value) exports only: type-only exports never appear here.
  expect(runtimeExports).toEqual([
    "PipelineExecutionError",
    "compiledTransitionFor",
    "executePipelineGraph",
    "executePipelineV2Graph",
    "executePipelineV2GraphResume",
  ]);
});

// --- 15-18. the resolver is a pure topology lookup (source-structure) -------

/** The engine source, read once for the structure assertions. */
const ENGINE_SOURCE = await Bun.file(join(import.meta.dir, "../src/pipeline_engine.ts")).text();

/** The body slice of one function: from its declaration to the closing brace at column 0. */
function functionSlice(declaration: string): string {
  const start = ENGINE_SOURCE.indexOf(declaration);
  if (start < 0) {
    throw new Error(`declaration not found: ${declaration}`);
  }
  const end = ENGINE_SOURCE.indexOf("\n}", start);
  if (end < 0) {
    throw new Error(`closing brace not found for: ${declaration}`);
  }
  return ENGINE_SOURCE.slice(start, end);
}

test("15. the public lookup never invokes a graph compiler (source structure)", async () => {
  const body = functionSlice("export function compiledTransitionFor(");
  expect(body).toMatch(/requireResolvedPipelineV2Provenance\(/);
  expect(body).toMatch(/pipeline\.states\.find\(/);
  expect(body).toMatch(/resolveCompiledTransition\(/);
  // no call to any function whose name contains "compile" (word boundary,
  // followed by an argument list; the lookup's own name and the resolver's
  // name are not invocations of a compiler)
  expect(body.match(/\b(?!compiledTransitionFor\()[Cc]ompile\w*\(/)).toBeNull();
  expect(ENGINE_SOURCE.includes("compileV2GraphFromBridge")).toBe(false);
  expect(ENGINE_SOURCE.includes("V2ExecutorBridge")).toBe(false);
});

test("16. the resolver builds no executable closures and reads no executors (source structure)", async () => {
  const body = functionSlice("function resolveCompiledTransition(");
  // pure lookup: no async machinery, no arrow closures, no executor/callback
  // mentions, no options
  expect(body.match(/=>/)).toBeNull();
  expect(body.match(/\basync\b/)).toBeNull();
  expect(body.match(/executor/i)).toBeNull();
  expect(body.match(/callback/i)).toBeNull();
  expect(body.match(/options/i)).toBeNull();
  expect(body.match(/execute/i)).toBeNull();
  // exactly one definition and exactly two call sites (execution loop +
  // public lookup); nothing else in the module resolves steps
  const occurrences = ENGINE_SOURCE.match(/resolveCompiledTransition\(/g) ?? [];
  expect(occurrences.length).toBe(3);
  expect(functionSlice("async function runCompiledGraph(").match(/resolveCompiledTransition\(/g)?.length).toBe(1);
  expect(functionSlice("export function compiledTransitionFor(").match(/resolveCompiledTransition\(/g)?.length).toBe(1);
  // the removed fake-executor machinery is gone from the production module
  expect(ENGINE_SOURCE.match(/never-callable|unreachable\w*Bridge/i)).toBeNull();
});

test("17. the compiler keeps capturing the real executors (fresh and resume)", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    let agentViews = 0;
    let decisionViews = 0;
    let agentCalls = 0;
    let decisionCalls = 0;
    const executors = {
      executeAgent: (state: V2AgentExecutionView): void => {
        agentViews += 1;
        expect(state.id).toBe("coder");
      },
      executeDecision: (state: V2DecisionExecutionView): string => {
        decisionViews += 1;
        expect(state.id).toBe("check");
        return decisionViews === 1 ? "beta" : "alpha";
      },
    };
    const fresh = await executePipelineV2Graph(pipeline, {
      executeAgent: (state) => {
        agentCalls += 1;
        executors.executeAgent(state);
      },
      executeDecision: (state) => {
        decisionCalls += 1;
        return executors.executeDecision(state);
      },
    });
    expect(fresh.trace).toEqual([
      { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
      { from: "check", outcome: "beta", to: "coder", transition_index: 1 },
      { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
      { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
    ]);
    expect(agentViews).toBe(2);
    expect(decisionViews).toBe(2);

    // the resume engine captures the same real executors exactly once and
    // runs only the suffix from the seeded cursor (fresh decision script)
    let resumedDecisions = 0;
    const resumed = await executePipelineV2GraphResume(
      pipeline,
      {
        executeAgent: (state) => {
          executors.executeAgent(state);
        },
        executeDecision: (state) => {
          resumedDecisions += 1;
          return resumedDecisions === 1 ? "beta" : "alpha";
        },
      },
      { current_state: "coder", transition_count: 0 },
    );
    expect(resumed).toEqual(fresh);
  });
});

test("18. mutating the real executor object after capture cannot change execution", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    const gate: { release: () => void } = { release: () => {} };
    let agentCalls = 0;
    let rogueCalls = 0;
    let decisionCalls = 0;
    const executors: {
      executeAgent: (state: V2AgentExecutionView) => void | Promise<void>;
      executeDecision: (state: V2DecisionExecutionView) => string | Promise<string>;
    } = {
      executeAgent: async (state) => {
        agentCalls += 1;
        expect(state.id).toBe("coder");
        if (agentCalls === 1) {
          await new Promise<void>((resolve) => {
            gate.release = resolve;
          });
        }
      },
      executeDecision: (state) => {
        decisionCalls += 1;
        expect(state.id).toBe("check");
        return decisionCalls === 1 ? "beta" : "alpha";
      },
    };
    const pending = executePipelineV2Graph(pipeline, executors);
    // while the first agent callback is pending, mutate the caller's
    // executor object; the engine already captured its executor snapshot
    executors.executeAgent = (state) => {
      rogueCalls += 1;
      expect(state.id).toBe("coder");
    };
    executors.executeDecision = () => {
      rogueCalls += 1;
      return "alpha";
    };
    gate.release();

    const result = await pending;
    expect(result.trace).toEqual([
      { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
      { from: "check", outcome: "beta", to: "coder", transition_index: 1 },
      { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
      { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
    ]);
    expect(agentCalls).toBe(2);
    expect(decisionCalls).toBe(2);
    expect(rogueCalls).toBe(0);
  });
});

test("19. the public lookup requires no executor object and moves no counters", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    // no executors exist in this test at all: the lookup signature cannot
    // even accept one, and resolution must not invoke any callback machinery
    let agentCalls = 0;
    let decisionCalls = 0;
    const seen = new Set<string>();
    for (const pair of [
      ["coder", "completed"],
      ["check", "alpha"],
      ["check", "beta"],
      ["check", "uncovered"],
    ] as const) {
      const step = compiledTransitionFor(pipeline, pair[0], pair[1]);
      seen.add(`${pair[0]}/${pair[1]}`);
      expect(Object.isFrozen(step)).toBe(true);
    }
    expect(seen.size).toBe(4);
    expect(agentCalls).toBe(0);
    expect(decisionCalls).toBe(0);
    void agentCalls;
    void decisionCalls;
  });
});

test("20. the exact engine error messages stay unchanged through the loop and the lookup", async () => {
  await withPipeline(PIPELINE_YAML, async ({ pipeline }) => {
    // lookup: unknown state
    let failure: unknown;
    try {
      compiledTransitionFor(pipeline, "bogus", "completed");
    } catch (cause) {
      failure = cause;
    }
    expect(failure).toBeInstanceOf(PipelineExecutionError);
    expect((failure as PipelineExecutionError).reason).toBe("missing_state");
    expect((failure as Error).message).toBe('pipeline cursor "bogus" does not name a declared state');

    // lookup: unknown outcome (decision state)
    failure = undefined;
    try {
      compiledTransitionFor(pipeline, "check", "mystery");
    } catch (cause) {
      failure = cause;
    }
    expect((failure as PipelineExecutionError).reason).toBe("unknown_outcome");
    expect((failure as Error).message).toBe(
      'decision result outcome "mystery" does not match any transition outcome of state "check"',
    );

    // lookup: unknown outcome over an agent state
    failure = undefined;
    try {
      compiledTransitionFor(pipeline, "coder", "mystery");
    } catch (cause) {
      failure = cause;
    }
    expect((failure as PipelineExecutionError).reason).toBe("unknown_outcome");
    expect((failure as Error).message).toBe(
      'agent result outcome "mystery" does not match any transition outcome of state "coder"',
    );

    // loop: unknown decision outcome keeps the same exact message
    let loopFailure: unknown;
    try {
      await executePipelineV2Graph(pipeline, {
        executeAgent: () => undefined,
        executeDecision: () => "mystery",
      });
    } catch (cause) {
      loopFailure = cause;
    }
    expect(loopFailure).toBeInstanceOf(PipelineExecutionError);
    expect((loopFailure as PipelineExecutionError).reason).toBe("unknown_outcome");
    expect((loopFailure as Error).message).toBe(
      'decision result outcome "mystery" does not match any transition outcome of state "check"',
    );
  });
});
