import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { PipelineError } from "../src/pipeline.ts";
import {
  executePipelineV2Graph,
  PipelineExecutionError,
  type PipelineV2GraphExecutors,
  type V2AgentExecutionView,
  type V2DecisionExecutionView,
} from "../src/pipeline_engine.ts";
import {
  loadPipelineV2,
  type PipelineDecisionStateResult,
  type ResolvedPipelineV2,
} from "../src/pipeline_v2.ts";
import {
  acceptActivationOutputs,
  evaluateDecisionStateFromData,
  prepareActivationData,
  snapshotRunInputs,
  type AcceptedStateOutput,
} from "../src/pipeline_v2_runtime.ts";

/**
 * Focused tests for `executePipelineV2Graph`: the pipeline-v2 compilation
 * adapter feeding the same single pure graph execution core that owns the
 * outcome -> transition -> next-state mapping, the transition budget, the
 * commit-hook ordering and the terminal selection for both pipeline
 * versions. The executors return only outcome strings and can never select
 * the next state; the production runner is not wired (schema v2 is still
 * rejected before Launcher auth and before any Session).
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

const INPUT_SEED_YAML = `  - id: facts_seed
    type: json
    protected: false
    schema: schemas/facts.schema.json
`;

const OUTPUTS_DIGEST_YAML = `  - id: facts_digest
    required: false
    source:
      state_output:
        state: coder
        output: facts
`;

/** beta re-enters nothing here: every outcome terminates the graph. */
const DECISION_TRANSITIONS_YAML = `      - outcome: alpha
        to: done
      - outcome: beta
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

/** beta re-enters the agent state: an agent/decision cycle with an exit. */
const DECISION_TRANSITIONS_CYCLE_YAML = `      - outcome: alpha
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

const DECISION_TRANSITIONS_SELF_CYCLE_YAML = `      - outcome: alpha
        to: done
      - outcome: beta
        to: check
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

const TERMINALS_YAML = `  - id: done
    type: terminal
    result: success
  - id: failed_end
    type: terminal
    result: failed
`;

const AGENT_TO_DECISION_STATES_YAML = `  - id: coder
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: facts
        type: json
        schema: schemas/facts.schema.json
      - id: report
        type: file
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
          state_output:
            state: coder
            output: facts
    transitions:
${DECISION_TRANSITIONS_YAML}`;

function decisionStateYaml(transitions: string): string {
  return `  - id: check
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
${transitions}`;
}

function pipelineYaml(
  entry: string,
  maxTransitions: number,
  states: string,
  inputs: string = INPUT_SEED_YAML,
  outputs: string = "",
): string {
  return `
schema_version: 2
entry_state: ${entry}
max_transitions: ${maxTransitions}

inputs:
${inputs === "" ? "  []" : inputs}

outputs:
${outputs === "" ? "  []" : outputs}

states:
${states}`;
}

const PIPELINE_AGENT_TO_DECISION = pipelineYaml(
  "coder",
  20,
  AGENT_TO_DECISION_STATES_YAML + TERMINALS_YAML,
  "",
  OUTPUTS_DIGEST_YAML,
);

const PIPELINE_ENTRY_DECISION = pipelineYaml("check", 20, decisionStateYaml(DECISION_TRANSITIONS_YAML) + TERMINALS_YAML);

const DECISION_CHAIN_STATES_YAML = `  - id: d1
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: alpha
        to: d2
      - outcome: beta
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
  - id: d2
    type: decision
    model: decisions/model.yaml
    inputs:
      - id: facts
        source:
          pipeline_input: facts_seed
    transitions:
      - outcome: beta
        to: done
      - outcome: alpha
        to: failed_end
      - outcome: uncovered
        to: failed_end
      - outcome: inconsistent_facts
        to: failed_end
      - outcome: invalid_facts
        to: failed_end
`;

const PIPELINE_DECISION_CHAIN = pipelineYaml("d1", 20, DECISION_CHAIN_STATES_YAML + TERMINALS_YAML);

const PIPELINE_SELF_CYCLE = pipelineYaml(
  "check",
  2,
  decisionStateYaml(DECISION_TRANSITIONS_SELF_CYCLE_YAML) + TERMINALS_YAML,
);

const PIPELINE_BOUNDARY = pipelineYaml("check", 1, decisionStateYaml(DECISION_TRANSITIONS_YAML) + TERMINALS_YAML);

/** The state-output-fed variant used by the real-data integration test. */
const PIPELINE_STATE_OUTPUT_YAML = pipelineYaml(
  "coder",
  20,
  AGENT_TO_DECISION_STATES_YAML + TERMINALS_YAML,
  "",
  OUTPUTS_DIGEST_YAML,
);

/** The agent/decision cycle variant with an exit (test 7 and the capture tests). */
const PIPELINE_AGENT_DECISION_CYCLE = pipelineYaml(
  "coder",
  20,
  AGENT_TO_DECISION_STATES_YAML.replace(DECISION_TRANSITIONS_YAML, DECISION_TRANSITIONS_CYCLE_YAML) + TERMINALS_YAML,
  "",
  OUTPUTS_DIGEST_YAML,
);

interface BundleDirs {
  root: string;
  bundle: string;
}

async function makeBundle(): Promise<BundleDirs> {
  const root = await mkdtemp(join(tmpdir(), "pipeline-v2-engine-"));
  const bundle = join(root, "bundle");
  await mkdir(join(bundle, "prompts"), { recursive: true });
  await mkdir(join(bundle, "decisions"), { recursive: true });
  await mkdir(join(bundle, "schemas"), { recursive: true });
  await writeFile(join(bundle, "prompts", "coder.md"), "implement the task\n");
  await writeFile(
    join(bundle, "schemas", "facts.schema.json"),
    JSON.stringify(FACTS_SCHEMA),
  );
  await writeFile(join(bundle, "decisions", "model.yaml"), MODEL_YAML);
  return { root, bundle };
}

const FACTS_SCHEMA = {
  type: "object",
  required: ["f1", "f2"],
  properties: { f1: { type: "boolean" }, f2: { type: "boolean" } },
};

interface BundledPipeline {
  dirs: BundleDirs;
  pipeline: ResolvedPipelineV2;
  cleanup: () => Promise<void>;
}

async function loadBundlePipeline(yaml: string): Promise<BundledPipeline> {
  const dirs = await makeBundle();
  await writeFile(join(dirs.bundle, "pipeline.yaml"), yaml);
  const pipeline = await loadPipelineV2(dirs.bundle);
  return {
    dirs,
    pipeline,
    cleanup: async () => {
      await rm(dirs.root, { recursive: true, force: true });
    },
  };
}

async function withPipeline(
  yaml: string,
  fn: (bundled: BundledPipeline) => Promise<void>,
): Promise<void> {
  const bundled = await loadBundlePipeline(yaml);
  try {
    await fn(bundled);
  } finally {
    await bundled.cleanup();
  }
}

interface TrackedExecutors {
  executeAgent: (state: V2AgentExecutionView) => string;
  executeDecision: (state: V2DecisionExecutionView) => string;
  agentSeen: string[];
  decisionSeen: string[];
}

function scriptedExecutors(
  agentScript: Record<string, string>,
  decisionScript: Record<string, string>,
): TrackedExecutors {
  const agentSeen: string[] = [];
  const decisionSeen: string[] = [];
  return {
    executeAgent: (state) => {
      agentSeen.push(state.id);
      const outcome = agentScript[state.id];
      if (outcome === undefined) {
        throw new Error(`agent script has no outcome for state ${JSON.stringify(state.id)}`);
      }
      return outcome;
    },
    executeDecision: (state) => {
      decisionSeen.push(state.id);
      const outcome = decisionScript[state.id];
      if (outcome === undefined) {
        throw new Error(`decision script has no outcome for state ${JSON.stringify(state.id)}`);
      }
      return outcome;
    },
    agentSeen,
    decisionSeen,
  };
}

/**
 * Mutable view of the v2 executor contract used by the reassignment tests:
 * the engine treats the caller's object as untrusted runtime input and
 * dispatches only its own captured executor snapshot.
 */
interface MutableV2Executors {
  executeAgent: (state: V2AgentExecutionView) => string | Promise<string>;
  executeDecision: (state: V2DecisionExecutionView) => string | Promise<string>;
}

/** An executor that must never be reached; reaching it fails the run. */
function refuse(kind: "agent" | "decision"): (state: { readonly id: string }) => string {
  return (state) => {
    throw new Error(`${kind} executor must not be called for state ${JSON.stringify(state.id)}`);
  };
}

async function rejectEngine(
  run: () => Promise<unknown> | unknown,
): Promise<PipelineExecutionError> {
  let failure: unknown;
  try {
    await run();
  } catch (cause) {
    failure = cause;
  }
  if (!(failure instanceof PipelineExecutionError)) {
    throw new Error(`expected PipelineExecutionError, got ${String(failure)}`);
  }
  return failure;
}

/** Attempts to mutate a frozen object; returns whether the assignment threw. */
function mutationThrew(mutate: () => void): boolean {
  try {
    mutate();
    return false;
  } catch {
    return true;
  }
}

// --- 1. agent -> decision -> success terminal ------------------------------

test("1. agent -> decision -> success terminal through the shared core", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    const executors = scriptedExecutors({ coder: "completed" }, { check: "alpha" });
    const result = await executePipelineV2Graph(pipeline, executors);
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 2,
      trace: [
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
      ],
    });
    expect(executors.agentSeen).toEqual(["coder"]);
    expect(executors.decisionSeen).toEqual(["check"]);
  });
});

// --- 2. entry decision without agent callback ------------------------------

test("2. an entry decision state runs without the agent executor", async () => {
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: refuse("agent"),
      executeDecision: () => "alpha",
    });
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 1,
      trace: [{ from: "check", outcome: "alpha", to: "done", transition_index: 0 }],
    });
  });
});

// --- 3. entry terminal: zero callbacks and transitions ---------------------

test("3. an entry terminal executes zero callbacks and records no transitions", async () => {
  await withPipeline(
    pipelineYaml("done", 3, `  - id: done\n    type: terminal\n    result: success\n`),
    async ({ pipeline }) => {
      const result = await executePipelineV2Graph(pipeline, {
        executeAgent: refuse("agent"),
        executeDecision: refuse("decision"),
      });
      expect(result).toEqual({
        terminalStateId: "done",
        terminalResult: "success",
        transitionCount: 0,
        trace: [],
      });
    },
  );
});

// --- 4. the selected decision outcome routes by the declared transition ----

test("4. the selected decision outcome is routed by the declared transition only", async () => {
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    let decisionRuns = 0;
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: refuse("agent"),
      executeDecision: () => {
        decisionRuns += 1;
        return "beta";
      },
    });
    expect(result).toEqual({
      terminalStateId: "failed_end",
      terminalResult: "failed",
      transitionCount: 1,
      trace: [{ from: "check", outcome: "beta", to: "failed_end", transition_index: 1 }],
    });
    expect(decisionRuns).toBe(1);
  });
});

// --- 5. reserved outcomes are ordinary declared outcomes -------------------

test("5. uncovered, inconsistent_facts and invalid_facts route as declared outcomes", async () => {
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    const indexes: Record<string, number> = {
      uncovered: 2,
      inconsistent_facts: 3,
      invalid_facts: 4,
    };
    for (const outcome of ["uncovered", "inconsistent_facts", "invalid_facts"] as const) {
      const index = indexes[outcome];
      if (index === undefined) {
        throw new Error(`no index for outcome ${outcome}`);
      }
      let decisionRuns = 0;
      const result = await executePipelineV2Graph(pipeline, {
        executeAgent: refuse("agent"),
        executeDecision: (state) => {
          decisionRuns += 1;
          expect(state.id).toBe("check");
          return outcome;
        },
      });
      expect(result).toEqual({
        terminalStateId: "failed_end",
        terminalResult: "failed",
        transitionCount: 1,
        trace: [{ from: "check", outcome, to: "failed_end", transition_index: index }],
      });
      expect(decisionRuns).toBe(1);
    }
  });
});

// --- 6. decision -> decision chain -----------------------------------------

test("6. a decision state can hand over to another decision state", async () => {
  await withPipeline(PIPELINE_DECISION_CHAIN, async ({ pipeline }) => {
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: refuse("agent"),
      executeDecision: (state) => (state.id === "d1" ? "alpha" : "beta"),
    });
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 2,
      trace: [
        { from: "d1", outcome: "alpha", to: "d2", transition_index: 0 },
        { from: "d2", outcome: "beta", to: "done", transition_index: 0 },
      ],
    });
  });
});

// --- 7. agent/decision cycle with an exit -----------------------------------

test("7. an agent/decision cycle is bounded by the shared budget and exits", async () => {
  await withPipeline(PIPELINE_AGENT_DECISION_CYCLE, async ({ pipeline }) => {
    const decisionOutcomes = ["beta", "alpha"];
    let agentRuns = 0;
    let decisionRuns = 0;
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: (state) => {
        agentRuns += 1;
        expect(state.id).toBe("coder");
        return "completed";
      },
      executeDecision: (state) => {
        decisionRuns += 1;
        expect(state.id).toBe("check");
        const outcome = decisionOutcomes[decisionRuns - 1];
        if (outcome === undefined) {
          throw new Error("decision script exhausted");
        }
        return outcome;
      },
    });
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 4,
      trace: [
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "beta", to: "coder", transition_index: 1 },
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
      ],
    });
    expect(agentRuns).toBe(2);
    expect(decisionRuns).toBe(2);
  });
});

// --- 8. budget exhaustion before the decision callback ----------------------

test("8. an exhausted budget rejects a decision state before its callback", async () => {
  await withPipeline(PIPELINE_SELF_CYCLE, async ({ pipeline }) => {
    let decisionRuns = 0;
    const error = await rejectEngine(() =>
      executePipelineV2Graph(pipeline, {
        executeAgent: refuse("agent"),
        executeDecision: () => {
          decisionRuns += 1;
          return "beta";
        },
      }),
    );
    expect(error.reason).toBe("transition_budget_exhausted");
    expect(error.message).toBe(
      'decision state "check" cannot execute: transition budget exhausted (2 of 2 transitions already applied)',
    );
    expect(decisionRuns).toBe(2);
  });
});

// --- 9. a terminal exactly at the budget boundary ---------------------------

test("9. a terminal is reached exactly at the budget boundary", async () => {
  await withPipeline(PIPELINE_BOUNDARY, async ({ pipeline }) => {
    let decisionRuns = 0;
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: refuse("agent"),
      executeDecision: () => {
        decisionRuns += 1;
        return "alpha";
      },
    });
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 1,
      trace: [{ from: "check", outcome: "alpha", to: "done", transition_index: 0 }],
    });
    expect(decisionRuns).toBe(1);
  });
});

// --- 10. unknown agent outcome ----------------------------------------------

test("10. an unknown agent outcome is rejected without moving the cursor", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    let agentRuns = 0;
    const error = await rejectEngine(() =>
      executePipelineV2Graph(
        pipeline,
        {
          executeAgent: () => {
            agentRuns += 1;
            return "bogus";
          },
          executeDecision: refuse("decision"),
        },
        {
          onTransitionCommit: () => {
            throw new Error("hook must not run for an unknown outcome");
          },
        },
      ),
    );
    expect(error.reason).toBe("unknown_outcome");
    expect(error.message).toBe(
      'agent result outcome "bogus" does not match any transition outcome of state "coder"',
    );
    expect(agentRuns).toBe(1);
  });
});

// --- 11. unknown decision outcome -------------------------------------------

test("11. an unknown decision outcome is rejected", async () => {
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    let decisionRuns = 0;
    const error = await rejectEngine(() =>
      executePipelineV2Graph(pipeline, {
        executeAgent: refuse("agent"),
        executeDecision: () => {
          decisionRuns += 1;
          return "bogus";
        },
      }),
    );
    expect(error.reason).toBe("unknown_outcome");
    expect(error.message).toBe(
      'decision result outcome "bogus" does not match any transition outcome of state "check"',
    );
    expect(decisionRuns).toBe(1);
  });
});

// --- 12. empty/whitespace/non-string outcomes from both executors -----------

test("12. empty, whitespace and non-string outcomes are rejected from both executors", async () => {
  const invalid: unknown[] = ["", "   ", 42, undefined, null];
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    for (const outcome of invalid) {
      let decisionRuns = 0;
      const error = await rejectEngine(() =>
        executePipelineV2Graph(pipeline, {
          executeAgent: refuse("agent"),
          executeDecision: () => {
            decisionRuns += 1;
            return outcome as string;
          },
        }),
      );
      expect(error.reason).toBe("invalid_outcome");
      expect(error.message).toBe(
        `decision state "check" produced an invalid outcome ${JSON.stringify(outcome)}; the decision reports an outcome and never selects the next state`,
      );
    }
  });
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    for (const outcome of ["", "   ", 42, undefined, null]) {
      const error = await rejectEngine(() =>
        executePipelineV2Graph(pipeline, {
          executeAgent: () => outcome as string,
          executeDecision: refuse("decision"),
        }),
      );
      expect(error.reason).toBe("invalid_outcome");
      expect(error.message).toBe(
        `agent state "coder" produced an invalid outcome ${JSON.stringify(outcome)}; the agent reports an outcome and never selects the next state`,
      );
    }
  });
});

// --- 13. dispatch exclusivity ------------------------------------------------

test("13. agent and decision executors are never called for each other's states", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    const executors = scriptedExecutors({ coder: "completed" }, { check: "beta" });
    const result = await executePipelineV2Graph(pipeline, executors);
    expect(result.terminalStateId).toBe("failed_end");
    expect(executors.agentSeen).toEqual(["coder"]);
    expect(executors.decisionSeen).toEqual(["check"]);
  });
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: refuse("agent"),
      executeDecision: () => "uncovered",
    });
    expect(result.terminalStateId).toBe("failed_end");
  });
});

// --- 14. callback exception keeps its identity, cursor does not move --------

test("14. a callback exception propagates with its identity and records nothing", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    const thrown = new Error("agent exploded");
    let agentRuns = 0;
    const failure: unknown = await executePipelineV2Graph(
      pipeline,
      {
        executeAgent: () => {
          agentRuns += 1;
          throw thrown;
        },
        executeDecision: refuse("decision"),
      },
      { onTransitionCommit: () => undefined },
    ).then(
      () => null,
      (cause: unknown) => cause,
    );
    expect(failure).toBe(thrown);
    expect(agentRuns).toBe(1);
  });
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    const thrown = new Error("decision exploded");
    let decisionRuns = 0;
    let hookRuns = 0;
    const failure: unknown = await executePipelineV2Graph(
      pipeline,
      {
        executeAgent: refuse("agent"),
        executeDecision: () => {
          decisionRuns += 1;
          throw thrown;
        },
      },
      {
        onTransitionCommit: () => {
          hookRuns += 1;
        },
      },
    ).then(
      () => "no error",
      (cause: unknown) => cause,
    );
    expect(failure).toBe(thrown);
    expect(decisionRuns).toBe(1);
    expect(hookRuns).toBe(0);
  });
});

// --- 15. commit hook ordering: after the outcome, before the next callback ---

test("15. the commit hook runs after the outcome resolution and before the next callback", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    const events: string[] = [];
    const result = await executePipelineV2Graph(
      pipeline,
      {
        executeAgent: (state) => {
          expect(state.id).toBe("coder");
          events.push("agent-callback");
          return "completed";
        },
        executeDecision: (state) => {
          expect(state.id).toBe("check");
          events.push("decision-callback");
          return "alpha";
        },
      },
      {
        onTransitionCommit: (step) => {
          events.push(`hook:${step.from}->${step.to}`);
        },
      },
    );
    // the hook for a transition runs after the executor that produced the
    // outcome and before the callback of the next state
    expect(events).toEqual([
      "agent-callback",
      "hook:coder->check",
      "decision-callback",
      "hook:check->done",
    ]);
    expect(result.terminalStateId).toBe("done");
  });
});

// --- 16. a failing hook never starts the next callback -----------------------

test("16. a hook failure stops the graph before the next executor runs", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    const hookFailure = new Error("persist failed");
    let hookRuns = 0;
    let decisionRuns = 0;
    const failure: unknown = await executePipelineV2Graph(
      pipeline,
      {
        executeAgent: () => "completed",
        executeDecision: () => {
          decisionRuns += 1;
          return "alpha";
        },
      },
      {
        onTransitionCommit: () => {
          hookRuns += 1;
          throw hookFailure;
        },
      },
    ).then(
      () => null,
      (cause: unknown) => cause,
    );
    expect(failure).toBe(hookFailure);
    expect(hookRuns).toBe(1);
    expect(decisionRuns).toBe(0);
  });
});

// --- 17. exact transition indexes and ordered trace --------------------------

test("17. the trace preserves the exact original transition indexes in order", async () => {
  await withPipeline(PIPELINE_DECISION_CHAIN, async ({ pipeline }) => {
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: refuse("agent"),
      executeDecision: (state) => (state.id === "d1" ? "alpha" : "alpha"),
    });
    expect(result.terminalStateId).toBe("failed_end");
    expect(result.terminalResult).toBe("failed");
    expect(result.trace).toEqual([
      { from: "d1", outcome: "alpha", to: "d2", transition_index: 0 },
      { from: "d2", outcome: "alpha", to: "failed_end", transition_index: 1 },
    ]);
  });
});

// --- 18. views are frozen and transition-free --------------------------------

test("18. executor views are frozen, fresh and contain no graph data", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    let agentViews = 0;
    let decisionViews = 0;
    await executePipelineV2Graph(pipeline, {
      executeAgent: (state) => {
        agentViews += 1;
        expect(Object.isFrozen(state)).toBe(true);
        expect(Object.keys(state).sort()).toEqual([
          "id",
          "max_attempts",
          "profile",
          "promptContent",
          "promptPath",
          "timeout_seconds",
          "type",
        ]);
        expect("transitions" in state).toBe(false);
        expect(state.type).toBe("agent");
        expect(state.id).toBe("coder");
        expect(state.profile).toBe("coder");
        expect(state.promptContent).toBe("implement the task\n");
        expect(state.timeout_seconds).toBe(60);
        expect(state.max_attempts).toBe(1);
        return "completed";
      },
      executeDecision: (state) => {
        decisionViews += 1;
        expect(Object.isFrozen(state)).toBe(true);
        expect(Object.keys(state).sort()).toEqual(["id", "type"]);
        expect(state.type).toBe("decision");
        expect(state.id).toBe("check");
        expect("transitions" in state).toBe(false);
        return "alpha";
      },
    });
    expect(agentViews).toBe(1);
    expect(decisionViews).toBe(1);
  });
});

// --- 19. callback mutation attempts cannot redirect the graph ---------------

test("19. callback mutation attempts of the pipeline or view never redirect the graph", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: (state) => {
        // the trusted snapshot is deeply frozen: every mutation attempt throws
        expect(mutationThrew(() => {
          (pipeline as { max_transitions: number }).max_transitions = 999;
        })).toBe(true);
        expect(mutationThrew(() => {
          const agent = pipeline.states.find((entry) => entry.type === "agent");
          if (agent === undefined || agent.type !== "agent") {
            throw new Error("expected the agent state");
          }
          (agent.transitions as unknown as { push(entry: unknown): void }).push({
            outcome: "rogue",
            to: "done",
          });
        })).toBe(true);
        expect(mutationThrew(() => {
          (state as { id: string }).id = "hijacked";
        })).toBe(true);
        expect(mutationThrew(() => {
          (state as { promptContent: string }).promptContent = "tampered";
        })).toBe(true);
        expect(state.id).toBe("coder");
        expect(pipeline.max_transitions).toBe(20);
        return "completed";
      },
      executeDecision: (state) => {
        expect(state.id).toBe("check");
        return "alpha";
      },
    });
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 2,
      trace: [
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
      ],
    });
  });
});

// --- 20. source mutation while a callback is pending stays snapshot-bound ---

test("20. mutation attempts while a callback is pending cannot change the engine snapshot", async () => {
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    const gate: { release: () => void } = { release: () => {} };
    let decisionRuns = 0;
    const pending = executePipelineV2Graph(pipeline, {
      executeAgent: refuse("agent"),
      executeDecision: async (state) => {
        expect(state.id).toBe("check");
        expect(Object.isFrozen(state)).toBe(true);
        await new Promise<void>((resolve) => {
          gate.release = resolve;
        });
        decisionRuns += 1;
        return "alpha";
      },
    });

    // While the callback is pending, every graph field is attempted to be
    // changed. The deep-frozen trusted snapshot rejects each attempt, and
    // the engine reads budget, transitions and terminal results only from
    // its own compiled snapshot anyway.
    const budgetMutated = mutationThrew(() => {
      (pipeline as { max_transitions: number }).max_transitions = 0;
    });
    const transitionMutated = mutationThrew(() => {
      const checkState = pipeline.states.find((state) => state.type === "decision");
      if (checkState === undefined || checkState.type !== "decision") {
        throw new Error("expected the check state");
      }
      (checkState.transitions as unknown as { push(entry: unknown): void }).push({
        outcome: "rogue",
        to: "done",
      });
    });
    const terminalMutated = mutationThrew(() => {
      const doneState = pipeline.states.find((state) => state.id === "done");
      if (doneState === undefined || doneState.type !== "terminal") {
        throw new Error("expected the done terminal");
      }
      (doneState as { result: string }).result = "failed";
    });
    expect(budgetMutated).toBe(true);
    expect(transitionMutated).toBe(true);
    expect(terminalMutated).toBe(true);
    expect(pipeline.max_transitions).toBe(20);
    gate.release();

    const result = await pending;
    expect(decisionRuns).toBe(1);
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 1,
      trace: [{ from: "check", outcome: "alpha", to: "done", transition_index: 0 }],
    });
  });
});

// --- 21. forged pipelines are rejected by the provenance gate ---------------

test("21. clone, spread and Proxy pipelines are rejected before any callback", async () => {
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    let agentRuns = 0;
    let decisionRuns = 0;
    const executors = {
      executeAgent: (_state: V2AgentExecutionView): string => {
        agentRuns += 1;
        return "completed";
      },
      executeDecision: (_state: V2DecisionExecutionView): string => {
        decisionRuns += 1;
        return "alpha";
      },
    };
    const clones: unknown[] = [
      structuredClone(pipeline),
      { ...pipeline },
    ];
    let trapCount = 0;
    const proxied = new Proxy(pipeline, {
      get: (target, property) => {
        trapCount += 1;
        return (target as unknown as Record<string | symbol, unknown>)[property];
      },
    });
    clones.push(proxied);
    for (const clone of clones) {
      let failure: unknown;
      try {
        await executePipelineV2Graph(clone as ResolvedPipelineV2, executors);
      } catch (cause) {
        failure = cause;
      }
      if (!(failure instanceof PipelineError)) {
        throw new Error(`expected PipelineError for a forged pipeline, got ${String(failure)}`);
      }
      expect(failure.message).toMatch(
        /executePipelineV2Graph requires the deep-frozen snapshot object returned by loadPipelineV2/,
      );
    }
    expect(trapCount).toBe(0);
    expect(agentRuns).toBe(0);
    expect(decisionRuns).toBe(0);
  });
});

// --- 23-26. executor snapshot capture ---------------------------------------

test("23. the captured decision executor survives an in-callback reassignment", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    let agentCalls = 0;
    let originalDecisionCalls = 0;
    let rogueCalls = 0;
    const executors: MutableV2Executors = {
      executeAgent: (state) => {
        agentCalls += 1;
        expect(state.id).toBe("coder");
        // the agent callback swaps the decision executor: the engine must
        // keep dispatching to the function captured before compilation
        (executors as { executeDecision: (state: V2DecisionExecutionView) => string }).executeDecision = () => {
          rogueCalls += 1;
          return "beta";
        };
        return "completed";
      },
      executeDecision: (state) => {
        originalDecisionCalls += 1;
        expect(state.id).toBe("check");
        return "alpha";
      },
    };
    const result = await executePipelineV2Graph(pipeline, executors);
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 2,
      trace: [
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
      ],
    });
    expect(agentCalls).toBe(1);
    expect(originalDecisionCalls).toBe(1);
    expect(rogueCalls).toBe(0);
  });
});

test("24. an external decision-executor reassignment while a callback is pending cannot change dispatch", async () => {
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    const gate: { release: () => void } = { release: () => {} };
    let agentCalls = 0;
    let originalDecisionCalls = 0;
    let rogueCalls = 0;
    const executors: MutableV2Executors = {
      executeAgent: async (state) => {
        agentCalls += 1;
        expect(state.id).toBe("coder");
        await new Promise<void>((resolve) => {
          gate.release = resolve;
        });
        return "completed";
      },
      executeDecision: (state) => {
        originalDecisionCalls += 1;
        expect(state.id).toBe("check");
        return "alpha";
      },
    };
    const pending = executePipelineV2Graph(pipeline, executors);
    // while the agent callback is pending, external code swaps the decision
    // executor; the engine already holds its own executor snapshot
    const rogue = (state: V2DecisionExecutionView): string => {
      rogueCalls += 1;
      expect(state.id).toBe("check");
      return "beta";
    };
    executors.executeDecision = rogue;
    gate.release();

    const result = await pending;
    expect(executors.executeDecision).toBe(rogue);
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 2,
      trace: [
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
      ],
    });
    expect(agentCalls).toBe(1);
    expect(originalDecisionCalls).toBe(1);
    expect(rogueCalls).toBe(0);
  });
});

test("25. an agent-executor swap after the first activation cannot change a revisit", async () => {
  await withPipeline(PIPELINE_AGENT_DECISION_CYCLE, async ({ pipeline }) => {
    const decisionOutcomes = ["beta", "alpha"];
    let agentCalls = 0;
    let rogueAgentCalls = 0;
    let decisionCalls = 0;
    const executors: MutableV2Executors = {
      executeAgent: (state) => {
        agentCalls += 1;
        expect(state.id).toBe("coder");
        if (agentCalls === 1) {
          // swap the agent executor after the first activation: the revisit
          // of the same agent state must still run the captured original
          const rogue = (rogueState: V2AgentExecutionView): string => {
            rogueAgentCalls += 1;
            expect(rogueState.id).toBe("coder");
            return "bogus";
          };
          executors.executeAgent = rogue;
        }
        return "completed";
      },
      executeDecision: (state) => {
        decisionCalls += 1;
        expect(state.id).toBe("check");
        const outcome = decisionOutcomes[decisionCalls - 1];
        if (outcome === undefined) {
          throw new Error("decision script exhausted");
        }
        return outcome;
      },
    };
    const result = await executePipelineV2Graph(pipeline, executors);
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 4,
      trace: [
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "beta", to: "coder", transition_index: 1 },
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
      ],
    });
    expect(agentCalls).toBe(2);
    expect(rogueAgentCalls).toBe(0);
    expect(decisionCalls).toBe(2);
  });
});

test("26. a missing or non-function executor fails invalid_executor before any callback", async () => {
  const describeValue = (value: unknown): string =>
    value === null ? "null" : typeof value;

  // non-function executeAgent values on an entry-agent pipeline: the
  // executor contract is validated before compilation and before any callback
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    for (const bad of [undefined, null, 42, "nope", {}]) {
      let decisionCalls = 0;
      let failure: unknown;
      try {
        await executePipelineV2Graph(
          pipeline,
          {
            executeAgent: bad,
            executeDecision: () => {
              decisionCalls += 1;
              return "alpha";
            },
          } as unknown as PipelineV2GraphExecutors,
        );
      } catch (cause) {
        failure = cause;
      }
      if (!(failure instanceof PipelineExecutionError)) {
        throw new Error(`expected PipelineExecutionError, got ${String(failure)}`);
      }
      expect(failure.reason).toBe("invalid_executor");
      expect(failure.message).toBe(
        `pipeline v2 graph executor contract violated: executors.executeAgent must be a function, got ${describeValue(bad)}`,
      );
      expect(decisionCalls).toBe(0);
    }
  });

  // an entry decision state uses executeDecision only, yet a bad
  // executeAgent is still rejected before the first callback
  await withPipeline(PIPELINE_ENTRY_DECISION, async ({ pipeline }) => {
    let decisionCalls = 0;
    let failure: unknown;
    try {
      await executePipelineV2Graph(pipeline, {
        executeAgent: undefined as unknown as (() => string),
        executeDecision: () => {
          decisionCalls += 1;
          return "alpha";
        },
      } as unknown as PipelineV2GraphExecutors);
    } catch (cause) {
      failure = cause;
    }
    if (!(failure instanceof PipelineExecutionError)) {
      throw new Error(`expected PipelineExecutionError, got ${String(failure)}`);
    }
    expect(failure.reason).toBe("invalid_executor");
    expect(failure.message).toBe(
      "pipeline v2 graph executor contract violated: executors.executeAgent must be a function, got undefined",
    );
    expect(decisionCalls).toBe(0);
  });

  // an entry terminal never runs callbacks, yet the executor contract is
  // still validated before the graph compiles
  await withPipeline(
    pipelineYaml("done", 3, `  - id: done\n    type: terminal\n    result: success\n`),
    async ({ pipeline }) => {
      let failure: unknown;
      try {
        await executePipelineV2Graph(
          pipeline,
          { executeAgent: 42, executeDecision: null } as unknown as PipelineV2GraphExecutors,
        );
      } catch (cause) {
        failure = cause;
      }
      if (!(failure instanceof PipelineExecutionError)) {
        throw new Error(`expected PipelineExecutionError, got ${String(failure)}`);
      }
      expect(failure.reason).toBe("invalid_executor");
      expect(failure.message).toBe(
        "pipeline v2 graph executor contract violated: executors.executeAgent must be a function, got number",
      );
    },
  );

  // missing executeDecision on an entry-agent pipeline
  await withPipeline(PIPELINE_AGENT_TO_DECISION, async ({ pipeline }) => {
    let agentCalls = 0;
    let failure: unknown;
    try {
      await executePipelineV2Graph(pipeline, {
        executeAgent: () => {
          agentCalls += 1;
          return "completed";
        },
        executeDecision: undefined as unknown as (() => string),
      } as unknown as PipelineV2GraphExecutors);
    } catch (cause) {
      failure = cause;
    }
    if (!(failure instanceof PipelineExecutionError)) {
      throw new Error(`expected PipelineExecutionError, got ${String(failure)}`);
    }
    expect(failure.reason).toBe("invalid_executor");
    expect(failure.message).toBe(
      "pipeline v2 graph executor contract violated: executors.executeDecision must be a function, got undefined",
    );
    expect(agentCalls).toBe(0);
  });
});

// --- 22. integration: the real decision data adapter as executor ------------

test("22. the real evaluateDecisionStateFromData feeds the shared engine to the right terminal", async () => {
  await withPipeline(PIPELINE_STATE_OUTPUT_YAML, async ({ pipeline }) => {
    const runRoot = join(pipeline.bundleRoot, "..", "run");
    await mkdir(runRoot, { recursive: true });
    await mkdir(join(runRoot, "project"), { mode: 0o700 });
    const snapshot = await snapshotRunInputs(pipeline, [], runRoot);

    const accepted: AcceptedStateOutput[] = [];
    let nextActivationIndex = 1;
    const seen: string[] = [];

    const result = await executePipelineV2Graph(pipeline, {
      executeAgent: async (state) => {
        seen.push(state.id);
        const prepared = await prepareActivationData(
          pipeline,
          snapshot,
          accepted,
          state.id,
          nextActivationIndex,
        );
        nextActivationIndex += 1;
        await writeFile(join(prepared.outputs_root, "facts"), JSON.stringify({ f1: true, f2: false }));
        await writeFile(join(prepared.outputs_root, "report"), "REPORT");
        // acceptance mints one runner-owned record per declared output
        const records = await acceptActivationOutputs(pipeline, prepared);
        accepted.push(...records);
        return "completed";
      },
      executeDecision: async (state) => {
        seen.push(`decision:${state.id}`);
        const decision: PipelineDecisionStateResult = await evaluateDecisionStateFromData(
          pipeline,
          snapshot,
          accepted,
          state.id,
          nextActivationIndex,
        );
        if (decision.status !== "selected") {
          throw new Error(`expected a selected decision outcome, got ${decision.status}`);
        }
        return decision.outcome;
      },
    });

    expect(seen).toEqual(["coder", "decision:check"]);
    expect(result).toEqual({
      terminalStateId: "done",
      terminalResult: "success",
      transitionCount: 2,
      trace: [
        { from: "coder", outcome: "completed", to: "check", transition_index: 0 },
        { from: "check", outcome: "alpha", to: "done", transition_index: 0 },
      ],
    });
    expect(accepted).toHaveLength(2);
    const factsRecord = accepted.find((record) => record.output === "facts");
    if (factsRecord === undefined) {
      throw new Error("expected the accepted facts record");
    }
    expect(factsRecord.state).toBe("coder");
    expect(factsRecord.activation_index).toBe(1);
    expect(factsRecord.digest).toMatch(/^[0-9a-f]{64}$/);
  });
});
