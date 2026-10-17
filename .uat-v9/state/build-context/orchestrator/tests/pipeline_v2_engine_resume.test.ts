import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  executePipelineGraph,
  executePipelineV2Graph,
  executePipelineV2GraphResume,
  PipelineExecutionError,
  type GraphExecutionResult,
  type PipelineV2GraphExecutors,
  type PipelineV2GraphResumeSeed,
  type TransitionStep,
  type V2AgentExecutionView,
  type V2DecisionExecutionView,
} from "../src/pipeline_engine.ts";
import { loadPipelineV2, type ResolvedPipelineV2 } from "../src/pipeline_v2.ts";

/**
 * Focused tests for `executePipelineV2GraphResume`: continuation of a
 * pipeline v2 graph from an existing run's durable cursor through the same
 * single execution loop `executePipelineV2Graph` uses. The transition
 * budget is shared between the durable prefix and the new invocation, the
 * commit hook runs only for the new suffix, the trace carries only the
 * suffix, and the final transition count is the total. The fresh v2 entry
 * point, its messages and the v1 path stay unchanged.
 */

const MODEL_YAML = `
schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: alpha
  - id: beta
relations:
  - id: r1
    assert:
      not:
        all:
          - {fact: f1, equals: true}
          - {fact: f2, equals: true}
constraints:
  - id: c1
    when: {fact: f1, equals: true}
    forbid: [beta]
rules:
  - id: rule-a
    when: {fact: f1, equals: true}
    decision: alpha
  - id: rule-b
    when: {fact: f2, equals: true}
    decision: beta
`;

/** beta re-enters the agent state: an agent/decision cycle with an exit. */
const DECISION_TRANSITIONS_YAML = `      - outcome: alpha
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

/** agent coder -> decision check -> done / failed_end, with a cycle to coder. */
const PIPELINE_CYCLE = `
schema_version: 2
entry_state: coder
max_transitions: 6

inputs:
  - id: facts_seed
    type: json
    protected: false
    schema: schemas/loose.schema.json

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
  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
${DECISION_TRANSITIONS_YAML}
  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

/** A two-agent chain for a plain agent-suffix resume. */
const PIPELINE_TWO_AGENTS = `
schema_version: 2
entry_state: first
max_transitions: 6

inputs: []

outputs: []

states:
  - id: first
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: second
  - id: second
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs: []
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;

interface Bundled {
  pipeline: ResolvedPipelineV2;
  cleanup: () => Promise<void>;
}

async function loadBundle(yaml: string): Promise<Bundled> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-engine-resume-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "pipeline.yaml"), yaml);
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
  await writeFile(join(bundle, "schemas", "loose.schema.json"), "{}");
  const pipeline = await loadPipelineV2(bundle);
  return { pipeline, cleanup: () => rm(root, { recursive: true, force: true }) };
}

/** Executor recording: which state ids were called, in order. */
interface CallLog {
  agents: string[];
  decisions: string[];
}

function executorsFor(
  log: CallLog,
  decisionOutcome: (state: V2DecisionExecutionView) => string = () => "alpha",
): PipelineV2GraphExecutors {
  return {
    executeAgent: async (state: V2AgentExecutionView): Promise<void> => {
      log.agents.push(state.id);
    },
    executeDecision: async (state: V2DecisionExecutionView): Promise<string> => {
      log.decisions.push(state.id);
      return decisionOutcome(state);
    },
  };
}

function recordingHook(calls: TransitionStep[]): (step: TransitionStep) => Promise<void> {
  return async (step: TransitionStep): Promise<void> => {
    calls.push({ ...step });
  };
}

async function runWith(
  pipeline: ResolvedPipelineV2,
  log: CallLog,
  hook: TransitionStep[] = [],
  decisionOutcome?: (state: V2DecisionExecutionView) => string,
): Promise<GraphExecutionResult> {
  return await executePipelineV2Graph(
    pipeline,
    executorsFor(log, decisionOutcome),
    { onTransitionCommit: recordingHook(hook) },
  );
}

test("a resume from the entry state with count 0 equals a fresh run", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    const freshCalls: CallLog = { agents: [], decisions: [] };
    const freshHook: TransitionStep[] = [];
    const fresh = await runWith(bundled.pipeline, freshCalls, freshHook);

    const resumeCalls: CallLog = { agents: [], decisions: [] };
    const resumeHook: TransitionStep[] = [];
    const resume = await executePipelineV2GraphResume(
      bundled.pipeline,
      executorsFor(resumeCalls),
      { current_state: "coder", transition_count: 0 },
      { onTransitionCommit: recordingHook(resumeHook) },
    );
    expect(resume).toEqual(fresh);
    expect(resumeCalls.agents).toEqual(freshCalls.agents);
    expect(resumeCalls.decisions).toEqual(freshCalls.decisions);
    expect(resumeHook).toEqual(freshHook);
  } finally {
    await bundled.cleanup();
  }
});

test("a mid-graph resume runs only the suffix: hook, trace and final count", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    // The full fresh run establishes the durable prefix: first transition
    // coder -completed-> check, second check -alpha-> done.
    const freshCalls: CallLog = { agents: [], decisions: [] };
    const freshHook: TransitionStep[] = [];
    const fresh = await runWith(bundled.pipeline, freshCalls, freshHook);
    expect(fresh.terminalStateId).toBe("done");
    expect(fresh.transitionCount).toBe(2);
    expect(freshCalls.agents).toEqual(["coder"]);
    expect(freshCalls.decisions).toEqual(["check"]);

    // Resume from the decision state with the durable count of 1.
    const first = freshHook[0]!;
    expect(first).toEqual({ from: "coder", outcome: "completed", to: "check", transition_index: 0 });
    const resumeCalls: CallLog = { agents: [], decisions: [] };
    const resumeHook: TransitionStep[] = [];
    const resume = await executePipelineV2GraphResume(
      bundled.pipeline,
      executorsFor(resumeCalls),
      { current_state: "check", transition_count: 1 },
      { onTransitionCommit: recordingHook(resumeHook) },
    );
    // only the decision callback ran; the agent was never called again
    expect(resumeCalls.agents).toEqual([]);
    expect(resumeCalls.decisions).toEqual(["check"]);
    // the hook ran only for the new suffix
    const secondCommit = freshHook[1];
    if (secondCommit === undefined) {
      throw new Error("the fresh run committed fewer than two transitions");
    }
    expect(resumeHook).toEqual([secondCommit]);
    // the trace carries only the suffix; the final count is the total
    expect(resume.trace).toEqual(fresh.trace.slice(1));
    expect(resume.transitionCount).toBe(fresh.transitionCount);
    expect(resume.terminalStateId).toBe("done");
    expect(resume.terminalResult).toBe("success");
  } finally {
    await bundled.cleanup();
  }
});

test("a fresh cycle with a non-exiting decision exhausts the shared budget", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    const calls: CallLog = { agents: [], decisions: [] };
    const hook: TransitionStep[] = [];
    const error = await executePipelineV2Graph(
      bundled.pipeline,
      executorsFor(calls, () => "beta"),
      { onTransitionCommit: recordingHook(hook) },
    ).catch((cause: unknown) => cause);
    expect(error).toBeInstanceOf(PipelineExecutionError);
    expect((error as PipelineExecutionError).reason).toBe("transition_budget_exhausted");
    expect((error as PipelineExecutionError).message).toContain("6 of 6");
    expect(hook).toHaveLength(6);
    expect(calls.agents).toEqual(["coder", "coder", "coder"]);
    expect(calls.decisions).toEqual(["check", "check", "check"]);
  } finally {
    await bundled.cleanup();
  }
});

test("resuming a cycle mid-way keeps the shared budget and exhausts it", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    const calls: CallLog = { agents: [], decisions: [] };
    const hook: TransitionStep[] = [];
    // Durable prefix of 4 transitions, cursor back at the agent state.
    const result = await executePipelineV2GraphResume(
      bundled.pipeline,
      executorsFor(calls, () => "beta"),
      { current_state: "coder", transition_count: 4 },
      { onTransitionCommit: recordingHook(hook) },
    ).catch((cause: unknown) => cause);
    expect(result).toBeInstanceOf(PipelineExecutionError);
    const error = result as PipelineExecutionError;
    expect(error.reason).toBe("transition_budget_exhausted");
    // The agent was called once at count 4 and the decision once at count
    // 5; the next agent entry at the boundary is rejected before the
    // callback.
    expect(calls.agents).toEqual(["coder"]);
    expect(calls.decisions).toEqual(["check"]);
    expect(hook).toHaveLength(2);
    expect(hook.map((step) => step.transition_index)).toEqual([0, 1]);
    // and the budget error names the shared count
    expect(error.message).toContain("6 of 6");
  } finally {
    await bundled.cleanup();
  }
});

test("a terminal seed returns immediately with zero callbacks and the seeded count", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    const calls: CallLog = { agents: [], decisions: [] };
    const hook: TransitionStep[] = [];
    let seen = 0;
    const result = await executePipelineV2GraphResume(
      bundled.pipeline,
      {
        executeAgent: async () => {
          seen += 1;
        },
        executeDecision: async () => {
          seen += 1;
          return "alpha";
        },
      },
      { current_state: "done", transition_count: 5 },
      { onTransitionCommit: recordingHook(hook) },
    );
    expect(seen).toBe(0);
    expect(hook).toHaveLength(0);
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 5,
      trace: [],
    });
    // the failed terminal works the same way
    const failed = await executePipelineV2GraphResume(
      bundled.pipeline,
      executorsFor(calls),
      { current_state: "failed_end", transition_count: 3 },
    );
    expect(failed).toEqual({
      terminalStateId: "failed_end",
      terminalResult: "failed",
      transitionCount: 3,
      trace: [],
    });
  } finally {
    await bundled.cleanup();
  }
});

test("the shared budget boundary: an executable state is rejected before its callback", async () => {
  const bundled = await loadBundle(PIPELINE_TWO_AGENTS);
  try {
    const calls: CallLog = { agents: [], decisions: [] };
    let seen = 0;
    const error = await executePipelineV2GraphResume(
      bundled.pipeline,
      {
        executeAgent: async () => {
          seen += 1;
        },
        executeDecision: async () => "alpha",
      },
      { current_state: "second", transition_count: 6 },
    ).catch((cause: unknown) => cause);
    expect(error).toBeInstanceOf(PipelineExecutionError);
    expect((error as PipelineExecutionError).reason).toBe("transition_budget_exhausted");
    expect(seen).toBe(0);
    expect(calls.agents).toEqual([]);
    // the message names the shared durable count
    expect((error as PipelineExecutionError).message).toContain("6 of 6 transitions already applied");
  } finally {
    await bundled.cleanup();
  }
});

test("a terminal at the budget boundary is allowed and returns immediately", async () => {
  const bundled = await loadBundle(PIPELINE_TWO_AGENTS);
  try {
    let seen = 0;
    const result = await executePipelineV2GraphResume(
      bundled.pipeline,
      {
        executeAgent: async () => {
          seen += 1;
        },
        executeDecision: async () => "alpha",
      },
      { current_state: "done", transition_count: 6 },
    );
    expect(seen).toBe(0);
    expect(result.transitionCount).toBe(6);
    expect(result.terminalStateId).toBe("done");
  } finally {
    await bundled.cleanup();
  }
});

test("an unknown cursor state fails missing_state before any callback", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    let seen = 0;
    const error = await executePipelineV2GraphResume(
      bundled.pipeline,
      {
        executeAgent: async () => {
          seen += 1;
        },
        executeDecision: async () => "alpha",
      },
      { current_state: "nope", transition_count: 0 },
    ).catch((cause: unknown) => cause);
    expect(error).toBeInstanceOf(PipelineExecutionError);
    expect((error as PipelineExecutionError).reason).toBe("missing_state");
    expect((error as PipelineExecutionError).message).toContain('"nope"');
    expect(seen).toBe(0);
  } finally {
    await bundled.cleanup();
  }
});

test("invalid seed shapes fail invalid_graph before any callback", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    let seen = 0;
    const executors: PipelineV2GraphExecutors = {
      executeAgent: async () => {
        seen += 1;
      },
      executeDecision: async () => "alpha",
    };
    const invalidSeeds: Array<{ label: string; seed: unknown }> = [
      { label: "negative count", seed: { current_state: "coder", transition_count: -1 } },
      { label: "non-integer count", seed: { current_state: "coder", transition_count: 1.5 } },
      { label: "count over budget", seed: { current_state: "coder", transition_count: 7 } },
      { label: "empty state", seed: { current_state: "", transition_count: 0 } },
      { label: "non-string state", seed: { current_state: 42, transition_count: 0 } },
      { label: "missing count", seed: { current_state: "coder" } },
      { label: "null seed", seed: null },
    ];
    for (const { label, seed } of invalidSeeds) {
      const error = await executePipelineV2GraphResume(
        bundled.pipeline,
        executors,
        seed as PipelineV2GraphResumeSeed,
      ).catch((cause: unknown) => cause);
      expect(error, label).toBeInstanceOf(PipelineExecutionError);
      expect((error as PipelineExecutionError).reason, label).toBe("invalid_graph");
    }
    // a throwing getter is the same contract violation, never propagated
    const hostile = {
      get current_state(): string {
        throw new Error("SEED-EXPLODED");
      },
      get transition_count(): number {
        throw new Error("SEED-EXPLODED");
      },
    };
    const error = await executePipelineV2GraphResume(
      bundled.pipeline,
      executors,
      hostile as unknown as PipelineV2GraphResumeSeed,
    ).catch((cause: unknown) => cause);
    expect(error).toBeInstanceOf(PipelineExecutionError);
    expect((error as PipelineExecutionError).reason).toBe("invalid_graph");
    expect((error as PipelineExecutionError).message).not.toContain("SEED-EXPLODED");
    expect(seen).toBe(0);
  } finally {
    await bundled.cleanup();
  }
});

test("mutating the caller seed after capture cannot influence the run", async () => {
  const bundled = await loadBundle(PIPELINE_TWO_AGENTS);
  try {
    const calls: CallLog = { agents: [], decisions: [] };
    const seed = { current_state: "second", transition_count: 1 };
    const first = executePipelineV2GraphResume(
      bundled.pipeline,
      executorsFor(calls),
      seed,
    );
    // reassign the seed fields while the run is in flight
    seed.current_state = "done";
    seed.transition_count = 99;
    const result = await first;
    expect(calls.agents).toEqual(["second"]);
    expect(result.transitionCount).toBe(2);
    expect(result.terminalStateId).toBe("done");
  } finally {
    await bundled.cleanup();
  }
});

test("reassigning the caller executors object during a resumed run cannot change the dispatch", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    const calls: CallLog = { agents: [], decisions: [] };
    let releaseDecision: () => void = () => undefined;
    const decisionGate = new Promise<void>((resolve) => {
      releaseDecision = resolve;
    });
    const rogue = async (): Promise<string> => {
      throw new Error("ROGUE-EXECUTOR");
    };
    const executors: PipelineV2GraphExecutors = {
      executeAgent: async (): Promise<void> => {
        throw new Error("ROGUE-EXECUTOR");
      },
      executeDecision: async (): Promise<string> => {
        calls.decisions.push("check");
        await decisionGate;
        return "alpha";
      },
    };
    const run = executePipelineV2GraphResume(
      bundled.pipeline,
      executors,
      { current_state: "check", transition_count: 1 },
    );
    // the decision callback is pending inside the captured original
    expect(calls.decisions).toEqual(["check"]);
    // reassign both members of the caller's object while it is pending
    (executors as { executeDecision: unknown }).executeDecision = rogue;
    (executors as { executeAgent: unknown }).executeAgent = rogue;
    releaseDecision();
    const result = await run;
    expect(result.terminalStateId).toBe("done");
    expect(result.transitionCount).toBe(2);
    expect(calls.decisions).toEqual(["check"]);
    expect(calls.agents).toEqual([]);
  } finally {
    await bundled.cleanup();
  }
});

test("the resume API is provenance-gated like the fresh API: clones and Proxies are rejected before any read", async () => {
  const bundled = await loadBundle(PIPELINE_CYCLE);
  try {
    let trapHits = 0;
    const proxy = new Proxy(bundled.pipeline, {
      get(target, property, receiver) {
        trapHits += 1;
        return Reflect.get(target, property, receiver);
      },
    });
    let seen = 0;
    const executors: PipelineV2GraphExecutors = {
      executeAgent: async () => {
        seen += 1;
      },
      executeDecision: async () => "alpha",
    };
    const seed: PipelineV2GraphResumeSeed = { current_state: "coder", transition_count: 0 };
    for (const forged of [
      { ...bundled.pipeline },
      structuredClone(bundled.pipeline) as unknown as ResolvedPipelineV2,
      proxy,
    ]) {
      const error = await executePipelineV2GraphResume(
        forged,
        executors,
        seed,
      ).catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(PipelineError);
    }
    // a Proxy pipeline is rejected before any getter or trap fires
    expect(trapHits).toBe(0);
    expect(seen).toBe(0);
    // the genuine pipeline still runs
    const result = await executePipelineV2GraphResume(
      bundled.pipeline,
      executors,
      { current_state: "coder", transition_count: 0 },
    );
    expect(result.terminalStateId).toBe("done");
  } finally {
    await bundled.cleanup();
  }
});

test("the fresh v2 entry point and the v1 path stay unchanged beside the resume API", async () => {
  const bundled = await loadBundle(PIPELINE_TWO_AGENTS);
  try {
    const v2Calls: CallLog = { agents: [], decisions: [] };
    const v2 = await executePipelineV2Graph(
      bundled.pipeline,
      executorsFor(v2Calls),
    );
    expect(v2.transitionCount).toBe(2);
    expect(v2Calls.agents).toEqual(["first", "second"]);
    // a fresh v2 run keeps its own executor-capture contract
    const missing = await executePipelineV2Graph(
      bundled.pipeline,
      { executeAgent: async () => undefined } as unknown as PipelineV2GraphExecutors,
    ).catch((cause: unknown) => cause);
    expect(missing).toBeInstanceOf(PipelineExecutionError);
    expect((missing as PipelineExecutionError).reason).toBe("invalid_executor");
    // the v1 entry point compiles and runs its own agent executor unchanged
    const v1Pipeline = {
      entry_state: "solo",
      max_transitions: 3,
      states: [
        {
          id: "solo",
          type: "agent" as const,
          profile: "coder",
          promptPath: "prompts/coder.md",
          promptContent: "implement\n",
          inputs: [] as string[],
          resultSchemaPath: "schemas/r.json",
          resultSchema: {} as Record<string, never>,
          timeout_seconds: 60,
          max_attempts: 1,
          transitions: [{ outcome: "completed", to: "end" }],
        },
        { id: "end", type: "terminal" as const, result: "success" as const },
      ],
    } as unknown as Parameters<typeof executePipelineGraph>[0];
    const v1Agents: string[] = [];
    const v1 = await executePipelineGraph(v1Pipeline, async (state) => {
      v1Agents.push(state.id);
      return "completed";
    });
    expect(v1Agents).toEqual(["solo"]);
    expect(v1).toEqual({
      terminalStateId: "end",
      terminalResult: "success",
      transitionCount: 1,
      trace: [{ from: "solo", outcome: "completed", to: "end", transition_index: 0 }],
    });
  } finally {
    await bundled.cleanup();
  }
});
