import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import {
  DECISION_LIMITS,
  DecisionFactValidationError,
  DecisionModelError,
  compileDecisionModel,
  evaluateDecision,
  loadDecisionModel,
  parseDecisionModel,
  type CompiledDecisionModel,
  type DecisionFactValidationReason,
} from "../src/decision.ts";

const REPO_ROOT = join(import.meta.dir, "..", "..");
const DEFAULT_BUNDLE = join(REPO_ROOT, "pipelines", "default");
const DEFAULT_DECISION_PATH = "decisions/architect.yaml";

interface OracleTable {
  input_fields: { name: string }[];
  consistency_relations: { id: string }[];
  hard_constraints: {
    id: string;
    when: Record<string, boolean>;
    only?: string[];
    forbid?: string[];
  }[];
  rules_in_priority_order: { id: string; decision: string; when: Record<string, boolean> }[];
}

interface OracleVectors {
  bit_order: string[];
  total_assignments: number;
  relation_rejected: number;
  consistent_vectors: number;
  selected: number;
  uncovered: number;
  rows: { id: string; input: string; expected: string | null; status: string }[];
}

const oracleTable: OracleTable = JSON.parse(
  await Bun.file(join(REPO_ROOT, "docs", "pipeline-oracle", "decision-table.json")).text(),
);
const oracleVectors: OracleVectors = JSON.parse(
  await Bun.file(join(REPO_ROOT, "docs", "pipeline-oracle", "decision-vectors.json")).text(),
);

function flagsFromBits(bitOrder: readonly string[], bits: number): Record<string, boolean> {
  const flags: Record<string, boolean> = {};
  for (let index = 0; index < bitOrder.length; index++) {
    const name = bitOrder[index];
    if (name === undefined) {
      throw new Error(`missing bit_order entry at index ${index}`);
    }
    flags[name] = ((bits >> (bitOrder.length - 1 - index)) & 1) === 1;
  }
  return flags;
}

function bitsFromFlags(bitOrder: readonly string[], flags: Record<string, boolean>): string {
  let encoding = "";
  for (const name of bitOrder) {
    encoding += flags[name] === true ? "1" : "0";
  }
  return encoding;
}

/**
 * Independent mechanical transcription of the oracle table (mirrors
 * audit.py): the allowed set starts as every declared decision, active hard
 * constraints narrow it in declaration order, and the first rule whose
 * condition matches and whose decision is still allowed wins.
 */
function independentDecision(
  flags: Record<string, boolean>,
  table: OracleTable,
): { decision: string; rule_id: string } | null {
  const matches = (when: Record<string, boolean>): boolean =>
    Object.entries(when).every(([name, value]) => flags[name] === value);
  const allowed = new Set(table.rules_in_priority_order.map((rule) => rule.decision));
  for (const constraint of table.hard_constraints) {
    if (!matches(constraint.when)) {
      continue;
    }
    if (constraint.only) {
      const keep = new Set(constraint.only);
      for (const decision of [...allowed]) {
        if (!keep.has(decision)) {
          allowed.delete(decision);
        }
      }
    } else if (constraint.forbid) {
      for (const decision of constraint.forbid) {
        allowed.delete(decision);
      }
    }
  }
  for (const rule of table.rules_in_priority_order) {
    if (allowed.has(rule.decision) && matches(rule.when)) {
      return { decision: rule.decision, rule_id: rule.id };
    }
  }
  return null;
}

/** Independent transcription of the six extracted relations (audit.py source_relations). */
function independentViolations(flags: Record<string, boolean>): string[] {
  const t = flags.requires_task_change;
  const i = flags.issues_exist;
  const m = flags.has_major_issue;
  const e = flags.stage_cannot_continue_without_external_input;
  const a = flags.acceptance_criteria_satisfied;
  const predicates = [
    flags.has_only_minor_issues === (i && !m),
    flags.no_blocking_issues === (!m && !e && !t),
    flags.can_close_normally === (a && !i),
    !m || i,
    a || m,
    !e || !t,
  ];
  const violated: string[] = [];
  predicates.forEach((valid, index) => {
    if (!valid) {
      violated.push(`FC${index + 1}`);
    }
  });
  return violated;
}

interface DocOptions {
  factIds?: string[];
  decisionIds?: string[];
  relations?: unknown[];
  constraints?: unknown[];
  rules?: unknown[];
  schemaVersion?: unknown;
  extra?: Record<string, unknown>;
}

function buildDoc(options: DocOptions = {}): Record<string, unknown> {
  const factIds = options.factIds ?? ["f1", "f2"];
  const decisionIds = options.decisionIds ?? ["d1", "d2"];
  const doc: Record<string, unknown> = {
    schema_version: options.schemaVersion ?? 1,
    facts: factIds.map((id) => ({ id })),
    decisions: decisionIds.map((id) => ({ id })),
    relations: options.relations ?? [],
    constraints: options.constraints ?? [],
    rules:
      options.rules ?? [
        { id: "r1", when: { fact: "f1", equals: true }, decision: "d1" },
        { id: "r2", when: { fact: "f2", equals: true }, decision: "d2" },
      ],
  };
  return Object.assign(doc, options.extra ?? {});
}

function baseFacts(overrides: Record<string, boolean> = {}): Record<string, boolean> {
  return { f1: false, f2: false, ...overrides };
}

const MINIMAL_YAML = `schema_version: 1
facts:
  - id: f1
  - id: f2
decisions:
  - id: d1
  - id: d2
relations: []
constraints: []
rules:
  - id: r1
    when:
      fact: f1
      equals: true
    decision: d1
`;

async function tempDir(prefix: string): Promise<string> {
  return await mkdtemp(join(tmpdir(), prefix));
}

async function cleanup(path: string): Promise<void> {
  await rm(path, { recursive: true, force: true });
}

test("compiles a generic model with arbitrary fact and decision ids (no legacy hardcoding)", () => {
  const model = compileDecisionModel(
    buildDoc({
      factIds: ["zz_flag_one", "aa_condition_two"],
      decisionIds: ["qq_action_x", "mm_hold_y"],
      relations: [
        {
          id: "rel_zz",
          assert: {
            any: [{ fact: "zz_flag_one", equals: true }, { fact: "aa_condition_two", equals: true }],
          },
        },
      ],
      constraints: [
        { id: "guard_one", when: { fact: "zz_flag_one", equals: true }, only: ["qq_action_x"] },
      ],
      rules: [
        { id: "rule_first", when: { fact: "zz_flag_one", equals: true }, decision: "qq_action_x" },
        {
          id: "rule_second",
          when: { fact: "aa_condition_two", equals: true },
          decision: "mm_hold_y",
        },
      ],
    }),
  );
  expect(model.factIds).toEqual(["zz_flag_one", "aa_condition_two"]);
  expect(model.decisionIds).toEqual(["qq_action_x", "mm_hold_y"]);
  const selected = evaluateDecision(model, { zz_flag_one: false, aa_condition_two: true });
  expect(selected).toEqual({
    status: "selected",
    decision: "mm_hold_y",
    rule_id: "rule_second",
    active_constraint_ids: [],
  });
});

test("the first matching rule in priority order wins", () => {
  const model = compileDecisionModel(
    buildDoc({
      rules: [
        { id: "rule_later", when: { fact: "f1", equals: true }, decision: "d2" },
        { id: "rule_earlier", when: { fact: "f1", equals: true }, decision: "d1" },
      ],
    }),
  );
  expect(evaluateDecision(model, baseFacts({ f1: true }))).toEqual({
    status: "selected",
    decision: "d2",
    rule_id: "rule_later",
    active_constraint_ids: [],
  });
});

test("a matched but forbidden rule is skipped, never terminal", () => {
  const model = compileDecisionModel(
    buildDoc({
      constraints: [{ id: "guard_only_two", when: { fact: "f1", equals: true }, only: ["d2"] }],
      rules: [
        { id: "rule_d1", when: { fact: "f1", equals: true }, decision: "d1" },
        { id: "rule_d2", when: { fact: "f1", equals: true }, decision: "d2" },
      ],
    }),
  );
  expect(evaluateDecision(model, baseFacts({ f1: true }))).toEqual({
    status: "selected",
    decision: "d2",
    rule_id: "rule_d2",
    active_constraint_ids: ["guard_only_two"],
  });
});

test("multiple only constraints intersect", () => {
  const model = compileDecisionModel(
    buildDoc({
      decisionIds: ["d1", "d2", "d3"],
      constraints: [
        { id: "only_ab", when: { fact: "f1", equals: true }, only: ["d1", "d2"] },
        { id: "only_bc", when: { fact: "f2", equals: true }, only: ["d2", "d3"] },
      ],
      rules: [
        { id: "rule_d1", when: { fact: "f1", equals: true }, decision: "d1" },
        { id: "rule_d2", when: { fact: "f1", equals: true }, decision: "d2" },
      ],
    }),
  );
  expect(evaluateDecision(model, { f1: true, f2: true })).toEqual({
    status: "selected",
    decision: "d2",
    rule_id: "rule_d2",
    active_constraint_ids: ["only_ab", "only_bc"],
  });
});

test("forbid and only constraints compose in declaration order", () => {
  const model = compileDecisionModel(
    buildDoc({
      decisionIds: ["d1", "d2", "d3"],
      constraints: [
        { id: "forbid_ad", when: { fact: "f1", equals: true }, forbid: ["d1", "d3"] },
        { id: "only_bd", when: { fact: "f2", equals: true }, only: ["d3", "d2"] },
      ],
      rules: [
        { id: "rule_d1", when: { fact: "f1", equals: true }, decision: "d1" },
        { id: "rule_d2", when: { fact: "f1", equals: true }, decision: "d2" },
      ],
    }),
  );
  expect(evaluateDecision(model, { f1: true, f2: true })).toEqual({
    status: "selected",
    decision: "d2",
    rule_id: "rule_d2",
    active_constraint_ids: ["forbid_ad", "only_bd"],
  });
});

test("an exhausted allowed set yields uncovered with every active constraint id", () => {
  const model = compileDecisionModel(
    buildDoc({
      constraints: [
        { id: "only_d1", when: { fact: "f1", equals: true }, only: ["d1"] },
        { id: "only_d2", when: { fact: "f2", equals: true }, only: ["d2"] },
      ],
      rules: [{ id: "rule_d1", when: { fact: "f1", equals: true }, decision: "d1" }],
    }),
  );
  expect(evaluateDecision(model, { f1: true, f2: true })).toEqual({
    status: "uncovered",
    active_constraint_ids: ["only_d1", "only_d2"],
  });
});

test("active constraint ids keep declaration order", () => {
  const model = compileDecisionModel(
    buildDoc({
      decisionIds: ["d1", "d2", "d3"],
      constraints: [
        { id: "declared_second", when: { fact: "f1", equals: true }, forbid: ["d1"] },
        { id: "declared_first", when: { fact: "f1", equals: true }, forbid: ["d2"] },
      ],
      rules: [{ id: "rule_free", when: { fact: "f2", equals: true }, decision: "d3" }],
    }),
  );
  const outcome = evaluateDecision(model, { f1: true, f2: true });
  expect(outcome.status).toBe("selected");
  if (outcome.status !== "selected") {
    throw new Error("expected selected");
  }
  expect(outcome.decision).toBe("d3");
  expect(outcome.rule_id).toBe("rule_free");
  expect(outcome.active_constraint_ids).toEqual(["declared_second", "declared_first"]);
});

test("violated relation ids are returned in declaration order", () => {
  const model = compileDecisionModel(
    buildDoc({
      relations: [
        { id: "zz_violated", assert: { fact: "f1", equals: false } },
        { id: "aa_violated", assert: { fact: "f1", equals: false } },
      ],
      rules: [],
    }),
  );
  expect(evaluateDecision(model, { f1: true, f2: false })).toEqual({
    status: "inconsistent_facts",
    violated_relation_ids: ["zz_violated", "aa_violated"],
  });
});

test("outcome shapes are exact", () => {
  const model = BASE_MODEL();
  const selected = evaluateDecision(model, baseFacts({ f1: true }));
  expect(selected.status).toBe("selected");
  if (selected.status !== "selected") {
    throw new Error("expected selected");
  }
  expect(Object.keys(selected).sort()).toEqual(["active_constraint_ids", "decision", "rule_id", "status"]);

  const uncovered = evaluateDecision(model, baseFacts());
  expect(Object.keys(uncovered).sort()).toEqual(["active_constraint_ids", "status"]);
  if (uncovered.status !== "uncovered") {
    throw new Error("expected uncovered");
  }
  expect(uncovered.active_constraint_ids).toEqual([]);

  const inconsistent = evaluateDecision(
    compileDecisionModel(
      buildDoc({ relations: [{ id: "rel", assert: { fact: "f1", equals: true } }] }),
    ),
    baseFacts({ f1: false }),
  );
  expect(Object.keys(inconsistent).sort()).toEqual(["status", "violated_relation_ids"]);
  if (inconsistent.status !== "inconsistent_facts") {
    throw new Error("expected inconsistent_facts");
  }
  expect(inconsistent.violated_relation_ids).toEqual(["rel"]);
});

test("missing, extra and non-boolean facts fail closed instead of evaluating", () => {
  const model = BASE_MODEL();
  expect(() => evaluateDecision(model, { f1: true })).toThrow(/is missing/);
  expect(() => evaluateDecision(model, { f1: true, f2: false, extra: true })).toThrow(
    /unknown fact id/,
  );
  expect(() => evaluateDecision(model, { f1: "true", f2: false })).toThrow(/must be a boolean/);
  expect(() => evaluateDecision(model, { f1: 1, f2: 0 })).toThrow(/must be a boolean/);
  expect(() => evaluateDecision(model, null as unknown as Record<string, unknown>)).toThrow(
    /must be a mapping/,
  );
  expect(() => evaluateDecision(model, ["f1"] as unknown as Record<string, unknown>)).toThrow(
    /must be a mapping/,
  );
  expect(() => evaluateDecision(model, "facts" as unknown as Record<string, unknown>)).toThrow(
    /must be a mapping/,
  );
});

test("fact-assignment failures throw the typed error with stable canary-free reasons", () => {
  const model = BASE_MODEL();
  const cases: { facts: unknown; reason: DecisionFactValidationReason; fact_id?: string; actual_type?: string }[] = [
    { facts: null, reason: "not_mapping" },
    { facts: "body", reason: "not_mapping" },
    { facts: [true], reason: "not_mapping" },
    { facts: { f1: true, f2: false, "CANARY-KEY": "x" }, reason: "unknown_fact" },
    { facts: { f1: true }, reason: "missing_fact", fact_id: "f2" },
    { facts: { f1: "CLASSIFIED", f2: false }, reason: "non_boolean_fact", fact_id: "f1", actual_type: "string" },
    { facts: { f1: 1, f2: 0 }, reason: "non_boolean_fact", fact_id: "f1", actual_type: "number" },
  ];
  for (const expected of cases) {
    let thrown: unknown;
    try {
      evaluateDecision(model, expected.facts as Readonly<Record<string, unknown>>);
    } catch (cause) {
      thrown = cause;
    }
    if (!(thrown instanceof DecisionFactValidationError)) {
      throw new Error(`expected DecisionFactValidationError for ${JSON.stringify(expected)}`);
    }
    expect(thrown).toBeInstanceOf(DecisionModelError);
    expect(thrown.reason).toBe(expected.reason);
    expect(thrown.message).not.toContain("CANARY");
    expect(thrown.message).not.toContain("CLASSIFIED");
    if (expected.fact_id === undefined) {
      expect("fact_id" in thrown).toBe(false);
    } else {
      expect(thrown.fact_id).toBe(expected.fact_id);
    }
    if (expected.actual_type === undefined) {
      expect("actual_type" in thrown).toBe(false);
    } else {
      expect(thrown.actual_type).toBe(expected.actual_type);
    }
  }
});

test("unknown fact and decision references fail compilation", () => {
  expect(() =>
    compileDecisionModel(
      buildDoc({ relations: [{ id: "rel", assert: { fact: "ghost", equals: true } }] }),
    ),
  ).toThrow(/references undeclared fact "ghost"/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [{ id: "c1", when: { fact: "f1", equals: true }, only: ["missing_decision"] }],
      }),
    ),
  ).toThrow(/references undeclared decision "missing_decision"/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [{ id: "c1", when: { fact: "f1", equals: true }, forbid: ["ghost"] }],
      }),
    ),
  ).toThrow(/references undeclared decision "ghost"/);
  expect(() =>
    compileDecisionModel(
      buildDoc({ rules: [{ id: "r1", when: { fact: "f1", equals: true }, decision: "ghost" }] }),
    ),
  ).toThrow(/references undeclared decision "ghost"/);
});

test("duplicate ids are rejected inside every collection", () => {
  expect(() =>
    compileDecisionModel(buildDoc({ factIds: ["f1", "f1"] })),
  ).toThrow(/more than once/);
  expect(() =>
    compileDecisionModel(buildDoc({ decisionIds: ["d1", "d1"] })),
  ).toThrow(/more than once/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: [
          { id: "same", assert: { fact: "f1", equals: true } },
          { id: "same", assert: { fact: "f1", equals: false } },
        ],
      }),
    ),
  ).toThrow(/more than once/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [
          { id: "same", when: { fact: "f1", equals: true }, forbid: ["d1"] },
          { id: "same", when: { fact: "f1", equals: true }, only: ["d1"] },
        ],
      }),
    ),
  ).toThrow(/more than once/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        rules: [
          { id: "same", when: { fact: "f1", equals: true }, decision: "d1" },
          { id: "same", when: { fact: "f2", equals: true }, decision: "d2" },
        ],
      }),
    ),
  ).toThrow(/more than once/);
});

test("ids may repeat across collections (each collection is its own namespace)", () => {
  const model = compileDecisionModel(
    buildDoc({
      relations: [{ id: "same", assert: { fact: "f1", equals: true } }],
      constraints: [{ id: "same", when: { fact: "f1", equals: true }, forbid: ["d1"] }],
      rules: [{ id: "same", when: { fact: "f2", equals: true }, decision: "d2" }],
    }),
  );
  expect(evaluateDecision(model, { f1: true, f2: true })).toEqual({
    status: "selected",
    decision: "d2",
    rule_id: "same",
    active_constraint_ids: ["same"],
  });
});

test("unknown fields are rejected at every level", () => {
  expect(() => compileDecisionModel(buildDoc({ extra: { notes: "x" } }))).toThrow(/unknown field/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: [{ id: "rel", assert: { fact: "f1", equals: true }, note: "x" }],
      }),
    ),
  ).toThrow(/unknown field/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [{ id: "c1", when: { fact: "f1", equals: true }, forbid: ["d1"], why: "x" }],
      }),
    ),
  ).toThrow(/unknown field/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        rules: [{ id: "r1", when: { fact: "f1", equals: true }, decision: "d1", comment: "x" }],
      }),
    ),
  ).toThrow(/unknown field/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: [{ id: "rel", assert: { fact: "f1", equals: true, note: "x" } }],
      }),
    ),
  ).toThrow(/exactly the fields "fact" and "equals"/);
});

test("constraints must declare exactly one of only/forbid with non-empty unique references", () => {
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [
          { id: "c1", when: { fact: "f1", equals: true }, only: ["d1"], forbid: ["d2"] },
        ],
      }),
    ),
  ).toThrow(/exactly one of "only" or "forbid"/);
  expect(() =>
    compileDecisionModel(
      buildDoc({ constraints: [{ id: "c1", when: { fact: "f1", equals: true } }] }),
    ),
  ).toThrow(/exactly one of "only" or "forbid"/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [{ id: "c1", when: { fact: "f1", equals: true }, only: [] }],
      }),
    ),
  ).toThrow(/must not be empty/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [{ id: "c1", when: { fact: "f1", equals: true }, forbid: [] }],
      }),
    ),
  ).toThrow(/must not be empty/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [{ id: "c1", when: { fact: "f1", equals: true }, only: ["d1", "d1"] }],
      }),
    ),
  ).toThrow(/more than once/);
});

test("expression nodes must be mappings of exactly one DSL form", () => {
  const invalid: unknown[] = [
    "f1 and f2",
    "true",
    true,
    3,
    null,
    [],
    { all: [] },
    { any: [] },
    { not: "f1" },
    { not: 7 },
    { not: null },
    { fact: "f1" },
    { fact: "f1", equals: "true" },
    { fact: "f1", equals: 1 },
    { fact: "f1", equals: true, any: [{ fact: "f1", equals: true }] },
    { all: [{ fact: "f1", equals: true }], any: [{ fact: "f1", equals: true }] },
    { unknown_form: true },
    {},
  ];
  for (const expression of invalid) {
    expect(() =>
      compileDecisionModel(
        buildDoc({ relations: [{ id: "rel", assert: expression }] }),
      ),
    ).toThrow(DecisionModelError);
  }
});

test("empty groups, missing equals and scalar operands fail compilation with clear messages", () => {
  expect(() =>
    compileDecisionModel(buildDoc({ relations: [{ id: "rel", assert: { fact: "f1" } }] })),
  ).toThrow(/fact expression must have exactly the fields "fact" and "equals"/);
  expect(() =>
    compileDecisionModel(buildDoc({ relations: [{ id: "rel", assert: { all: [] } }] })),
  ).toThrow(/must contain at least one expression/);
  expect(() =>
    compileDecisionModel(buildDoc({ relations: [{ id: "rel", assert: { any: [] } }] })),
  ).toThrow(/must contain at least one expression/);
  expect(() =>
    compileDecisionModel(buildDoc({ relations: [{ id: "rel", assert: "f1 and f2" }] })),
  ).toThrow(/must be an expression mapping/);
  expect(() =>
    compileDecisionModel(buildDoc({ relations: [{ id: "rel", assert: true }] })),
  ).toThrow(/must be an expression mapping/);
  expect(() =>
    compileDecisionModel(buildDoc({ relations: [{ id: "rel", assert: { not: "f1" } }] })),
  ).toThrow(/operand/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: [{ id: "rel", assert: { fact: "f1", equals: "true" } }],
      }),
    ),
  ).toThrow(/must be a boolean/);
  expect(() =>
    compileDecisionModel(
      buildDoc({ relations: [{ id: "rel", assert: { all: [{ fact: "f1", equals: true }], not: {} } }] }),
    ),
  ).toThrow(/exactly one form/);
});

test("malformed documents fail compilation", () => {
  expect(() => compileDecisionModel(buildDoc({ schemaVersion: 2 }))).toThrow(/schema_version/);
  expect(() => compileDecisionModel(buildDoc({ schemaVersion: "1" }))).toThrow(/schema_version/);
  expect(() => compileDecisionModel(buildDoc({ extra: { notes: "x" } }))).toThrow(/unknown field/);
  expect(() => compileDecisionModel("scalar")).toThrow(/is not a YAML mapping/);
  expect(() => compileDecisionModel(null)).toThrow(/is not a YAML mapping/);
  expect(() => compileDecisionModel([])).toThrow(/is not a YAML mapping/);
  expect(() => compileDecisionModel(buildDoc({ factIds: [] }))).toThrow(/must not be empty/);
  expect(() => compileDecisionModel(buildDoc({ decisionIds: [] }))).toThrow(/must not be empty/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: [{ id: "rel", assert: { fact: "f1", equals: true } }],
        rules: [],
      }),
    ),
  ).not.toThrow();
});

test("collection size limits fail closed", () => {
  expect(() =>
    compileDecisionModel(
      buildDoc({ factIds: Array.from({ length: DECISION_LIMITS.maxFacts + 1 }, (_, i) => `f${i}`) }),
    ),
  ).toThrow(/more than the maximum/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        decisionIds: Array.from({ length: DECISION_LIMITS.maxDecisions + 1 }, (_, i) => `d${i}`),
      }),
    ),
  ).toThrow(/more than the maximum/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: Array.from({ length: DECISION_LIMITS.maxRelations + 1 }, (_, index) => ({
          id: `rel${index}`,
          assert: { fact: "f1", equals: true },
        })),
      }),
    ),
  ).toThrow(/more than the maximum/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: Array.from({ length: DECISION_LIMITS.maxConstraints + 1 }, (_, index) => ({
          id: `c${index}`,
          when: { fact: "f1", equals: true },
          forbid: ["d1"],
        })),
      }),
    ),
  ).toThrow(/more than the maximum/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        rules: Array.from({ length: DECISION_LIMITS.maxRules + 1 }, (_, index) => ({
          id: `r${index}`,
          when: { fact: "f1", equals: true },
          decision: "d1",
        })),
      }),
    ),
  ).toThrow(/more than the maximum/);
});

test("unsafe or oversized ids fail compilation", () => {
  expect(() =>
    compileDecisionModel(buildDoc({ factIds: ["../traversal", "f2"], rules: [] })),
  ).toThrow(/is not a safe identifier/);
  expect(() =>
    compileDecisionModel(buildDoc({ factIds: ["has space", "f2"], rules: [] })),
  ).toThrow(/is not a safe identifier/);
  expect(() =>
    compileDecisionModel(buildDoc({ factIds: ["x".repeat(129), "f2"], rules: [] })),
  ).toThrow(/is not a safe identifier/);
  expect(() =>
    compileDecisionModel(buildDoc({ factIds: ["x".repeat(128), "f2"], rules: [] })),
  ).not.toThrow();
});

function nestedNotChain(count: number): Record<string, unknown> {
  let expression: Record<string, unknown> = { fact: "f1", equals: true };
  for (let index = 0; index < count; index++) {
    expression = { not: expression };
  }
  return expression;
}

test("expression depth limit fails closed exactly at the boundary", () => {
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [
          { id: "deep", when: nestedNotChain(DECISION_LIMITS.maxExpressionDepth), forbid: ["d1"] },
        ],
      }),
    ),
  ).toThrow(/maximum expression depth/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        constraints: [
          {
            id: "at_limit",
            when: nestedNotChain(DECISION_LIMITS.maxExpressionDepth - 1),
            forbid: ["d1"],
          },
        ],
      }),
    ),
  ).not.toThrow();
});

test("per-expression node limit fails closed exactly at the boundary", () => {
  const allWithChildren = (count: number): unknown[] =>
    Array.from({ length: count }, () => ({ fact: "f1", equals: true }));
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: [{ id: "wide", assert: { all: allWithChildren(DECISION_LIMITS.maxExpressionNodes) } }],
      }),
    ),
  ).toThrow(/expression nodes in one expression/);
  expect(() =>
    compileDecisionModel(
      buildDoc({
        relations: [
          {
            id: "wide",
            assert: { all: allWithChildren(DECISION_LIMITS.maxExpressionNodes - 1) },
          },
        ],
      }),
    ),
  ).not.toThrow();
});

test("total expression node limit fails closed", () => {
  const wideRelation = (id: string) => ({
    id,
    assert: {
      all: Array.from({ length: DECISION_LIMITS.maxExpressionNodes - 1 }, () => ({
        fact: "f1",
        equals: true,
      })),
    },
  });
  const relationsAtLimit = Array.from({ length: 8 }, (_, index) => wideRelation(`rel${index}`));
  expect(() =>
    compileDecisionModel(buildDoc({ relations: relationsAtLimit, constraints: [], rules: [] })),
  ).not.toThrow();
  expect(() =>
    compileDecisionModel(
      buildDoc({ relations: [...relationsAtLimit, wideRelation("rel_extra")], constraints: [], rules: [] }),
    ),
  ).toThrow(/maximum total/);
});

test("parseDecisionModel rejects invalid YAML text and accepts valid text", () => {
  expect(() => parseDecisionModel("{{{{ not yaml")).toThrow(/not valid YAML/);
  expect(() => parseDecisionModel("just a scalar")).toThrow(/is not a YAML mapping/);
  expect(() => parseDecisionModel("")).toThrow(/is not a YAML mapping/);
  const model = parseDecisionModel(MINIMAL_YAML);
  expect(model.factIds).toEqual(["f1", "f2"]);
  expect(evaluateDecision(model, { f1: true, f2: false })).toEqual({
    status: "selected",
    decision: "d1",
    rule_id: "r1",
    active_constraint_ids: [],
  });
});

test("loader accepts a real bundle root and an internal symlink, rejects symlink escape", async () => {
  const bundle = await tempDir("decision-bundle-");
  const outside = await tempDir("decision-outside-");
  try {
    await writeFile(join(bundle, "model.yaml"), MINIMAL_YAML);
    const loaded = await loadDecisionModel(bundle, "model.yaml");
    expect(loaded.factIds).toEqual(["f1", "f2"]);

    await symlink(join(bundle, "model.yaml"), join(bundle, "linked.yaml"));
    const viaSymlink = await loadDecisionModel(bundle, "linked.yaml");
    expect(viaSymlink.factIds).toEqual(["f1", "f2"]);

    await writeFile(join(outside, "outside.yaml"), MINIMAL_YAML);
    await symlink(join(outside, "outside.yaml"), join(bundle, "escape.yaml"));
    await expect(loadDecisionModel(bundle, "escape.yaml")).rejects.toThrow(
      /resolves outside the bundle root/,
    );
  } finally {
    await cleanup(bundle);
    await cleanup(outside);
  }
});

test("loader fails closed on bad roots, paths and file kinds", async () => {
  const bundle = await tempDir("decision-bundle-");
  try {
    await writeFile(join(bundle, "model.yaml"), MINIMAL_YAML);
    await mkdir(join(bundle, "nested"), { recursive: true });
    await writeFile(join(bundle, "nested", "model.yaml"), MINIMAL_YAML);
    await mkdir(join(bundle, "dir.yaml"));

    await expect(loadDecisionModel(bundle, "nested/model.yaml")).resolves.toBeDefined();
    await expect(loadDecisionModel(join(bundle, "missing-root"), "model.yaml")).rejects.toThrow(
      /cannot be canonicalized/,
    );
    await expect(loadDecisionModel(join(bundle, "model.yaml"), "model.yaml")).rejects.toThrow(
      /is not a directory/,
    );
    await expect(loadDecisionModel("relative/root", "model.yaml")).rejects.toThrow(
      /must be an absolute path/,
    );
    await expect(loadDecisionModel(bundle, "dir.yaml")).rejects.toThrow(/not a regular file/);
    await expect(loadDecisionModel(bundle, "absent.yaml")).rejects.toThrow(/not accessible/);
    await expect(loadDecisionModel(bundle, "model.yml")).rejects.toThrow(/must end with .yaml/);
    await expect(loadDecisionModel(bundle, "model")).rejects.toThrow(/must end with .yaml/);
    await expect(loadDecisionModel(bundle, "../model.yaml")).rejects.toThrow(
      /clean bundle-relative path/,
    );
    await expect(loadDecisionModel(bundle, "./model.yaml")).rejects.toThrow(
      /clean bundle-relative path/,
    );
    await expect(loadDecisionModel(bundle, "nested//model.yaml")).rejects.toThrow(
      /clean bundle-relative path/,
    );
    await expect(loadDecisionModel(bundle, "nested/../model.yaml")).rejects.toThrow(
      /clean bundle-relative path/,
    );
    await expect(loadDecisionModel(bundle, "~other/model.yaml")).rejects.toThrow(
      /must be a bundle-relative path/,
    );
    await expect(
      loadDecisionModel(bundle, join(bundle, "model.yaml")),
    ).rejects.toThrow(/must be a bundle-relative path/);
  } finally {
    await cleanup(bundle);
  }
});

function BASE_MODEL(): CompiledDecisionModel {
  return compileDecisionModel(buildDoc());
}

test("mutations of the parsed source object cannot change evaluation after compile", () => {
  const raw = buildDoc({
    relations: [{ id: "rel", assert: { fact: "f1", equals: true } }],
    constraints: [{ id: "guard", when: { fact: "f1", equals: true }, forbid: ["d2"] }],
    rules: [
      { id: "rule_d1", when: { fact: "f1", equals: true }, decision: "d1" },
      { id: "rule_d2", when: { fact: "f2", equals: true }, decision: "d2" },
    ],
  });
  const model = compileDecisionModel(raw);
  const input = { f1: true, f2: true };
  const before = evaluateDecision(model, input);
  expect(before).toEqual({
    status: "selected",
    decision: "d1",
    rule_id: "rule_d1",
    active_constraint_ids: ["guard"],
  });

  const rules = raw.rules as { when: unknown; decision: string }[];
  const firstRule = rules[0];
  if (firstRule === undefined) {
    throw new Error("missing rule to mutate");
  }
  firstRule.when = { fact: "f1", equals: false };
  firstRule.decision = "d2";
  const relation = (raw.relations as { assert: unknown }[])[0];
  if (relation === undefined) {
    throw new Error("missing relation to mutate");
  }
  relation.assert = { fact: "f1", equals: false };
  const constraint = (raw.constraints as { when: unknown }[])[0];
  if (constraint === undefined) {
    throw new Error("missing constraint to mutate");
  }
  constraint.when = { fact: "f1", equals: false };
  raw.facts = [];
  raw.decisions = [];

  const after = evaluateDecision(model, input);
  expect(after).toEqual(before);
  expect(Object.isFrozen(model)).toBe(true);
  expect(Object.isFrozen(model.relations)).toBe(true);
  expect(Object.isFrozen(model.constraints)).toBe(true);
  expect(Object.isFrozen(model.rules)).toBe(true);
});

test("repeated evaluation of the same input yields structurally identical results", () => {
  const model = compileDecisionModel(
    buildDoc({
      relations: [{ id: "rel", assert: { all: [{ fact: "f1", equals: true }] } }],
      constraints: [{ id: "guard", when: { fact: "f2", equals: true }, forbid: ["d1"] }],
    }),
  );
  const first = evaluateDecision(model, { f1: true, f2: true });
  const second = evaluateDecision(model, { f1: true, f2: true });
  expect(first).toEqual(second);
  expect(first).not.toBe(second);
  const other = evaluateDecision(model, { f1: true, f2: false });
  if (other.status !== "selected" || first.status !== "selected") {
    throw new Error("expected selected outcomes");
  }
  expect(other.decision).not.toBe(first.decision);
});

test(
  "oracle equivalence: all 2048 assignments match the frozen vectors and the independent transcription",
  async () => {
    expect(oracleTable.input_fields.map((field) => field.name)).toEqual(oracleVectors.bit_order);
    expect(oracleVectors.total_assignments).toBe(2048);
    expect(oracleVectors.relation_rejected).toBe(1952);
    expect(oracleVectors.consistent_vectors).toBe(96);
    expect(oracleVectors.selected).toBe(82);
    expect(oracleVectors.uncovered).toBe(14);

    const model = await loadDecisionModel(DEFAULT_BUNDLE, DEFAULT_DECISION_PATH);
    const fields = oracleVectors.bit_order;
    const stored = new Map<string, OracleVectors["rows"][number]>();
    for (const row of oracleVectors.rows) {
      expect(row.id).toBe(`DV-${row.input}`);
      stored.set(row.input, row);
    }
    expect(stored.size).toBe(96);

    let inconsistent = 0;
    let selected = 0;
    let uncovered = 0;
    const counts = new Map<string, number>();

    for (let bits = 0; bits < 2 ** fields.length; bits++) {
      const flags = flagsFromBits(fields, bits);
      const encoding = bitsFromFlags(fields, flags);
      const outcome = evaluateDecision(model, flags);
      const row = stored.get(encoding);
      const independent = independentDecision(flags, oracleTable);

      if (row === undefined) {
        expect(outcome.status).toBe("inconsistent_facts");
        if (outcome.status !== "inconsistent_facts") {
          throw new Error("expected inconsistent_facts");
        }
        expect(outcome.violated_relation_ids).toEqual(independentViolations(flags));
        inconsistent++;
        continue;
      }

      if (row.status === "selected") {
        expect(row.expected).not.toBeNull();
        expect(independent).not.toBeNull();
        if (independent === null) {
          throw new Error("independent transcription produced no decision");
        }
        const expectedDecision = row.expected;
        if (expectedDecision === null) {
          throw new Error("selected vector without an expected decision");
        }
        if (outcome.status !== "selected") {
          throw new Error(`expected selected for ${encoding}, got ${outcome.status}`);
        }
        expect(outcome.decision).toBe(expectedDecision);
        expect(outcome.rule_id).toBe(independent.rule_id);
        expect(independent.decision).toBe(expectedDecision);
        selected++;
        counts.set(outcome.decision, (counts.get(outcome.decision) ?? 0) + 1);
      } else {
        expect(row.expected).toBeNull();
        expect(independent).toBeNull();
        expect(outcome.status).toBe("uncovered");
        if (outcome.status !== "uncovered") {
          throw new Error(`expected uncovered for ${encoding}, got ${outcome.status}`);
        }
        uncovered++;
        counts.set("uncovered", (counts.get("uncovered") ?? 0) + 1);
      }
    }

    expect(inconsistent).toBe(1952);
    expect(selected).toBe(82);
    expect(uncovered).toBe(14);
    expect(selected + uncovered).toBe(96);
    expect(inconsistent + selected + uncovered).toBe(2048);
    expect(Object.fromEntries(counts)).toEqual({
      close_stage: 14,
      close_stage_ignore_minor: 7,
      rework_same_stage: 3,
      rework_change_stage_contract: 2,
      rework_change_pipeline_plan: 16,
      architectural_proposal: 32,
      architectural_warning: 8,
      uncovered: 14,
    });
  },
  { timeout: 30000 },
);

test("an oracle uncovered witness stays uncovered with no fallback", async () => {
  const model = await loadDecisionModel(DEFAULT_BUNDLE, DEFAULT_DECISION_PATH);
  // DV-00101010101: major issue at the iteration limit, no TASK/plan/contract
  // change needed, no external input -> no applicable decision.
  const outcome = evaluateDecision(
    model,
    flagsFromBits(oracleVectors.bit_order, Number.parseInt("00101010101", 2)),
  );
  expect(outcome.status).toBe("uncovered");
  if (outcome.status !== "uncovered") {
    throw new Error("expected uncovered");
  }
  expect(Array.isArray(outcome.active_constraint_ids)).toBe(true);
  expect("decision" in outcome).toBe(false);
});
