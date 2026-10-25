/**
 * The immutable low-level apply plan: the single plan schema/digest owner
 * (`pipeline_v2_apply_plan_internal.ts`) over the exact canonical document
 * the apply-all planner publishes and the prepared-package loader
 * verifies.
 *
 * Covered here: the exact shape at every level, the closed step
 * vocabulary, dense step/payload indexes, per-kind field coherence, the
 * per-path step-sequence rules, safe relative paths and ids, the payload
 * binding rules, the change-coverage rule, digest determinism, domain
 * separation and digest sensitivity, canonical-JSON serialization and the
 * absence of host paths or content in the canonical document.
 */
import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  PIPELINE_V2_APPLY_PLAN_DIGEST_DOMAIN,
  PIPELINE_V2_APPLY_PLAN_KIND,
  PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION,
  PIPELINE_V2_APPLY_POLICY_VERSION,
  PIPELINE_V2_APPLY_STEP_KINDS,
  PipelineV2ApplyPlanError,
  applyPlanCanonicalJson,
  applyPlanDigest,
  parseApplyPlan,
  validateApplyPlan,
  type PipelineV2ApplyPlan,
} from "../src/pipeline_v2_apply_plan_internal.ts";

const sha = (content: string): string => createHash("sha256").update(content).digest("hex");
const digest64 = sha("x");
const fileNode = (content: string) => ({ kind: "regular_file", mode: 0o600, byte_count: content.length, sha256: sha(content) }) as const;
const dirNode = (mode: number) => ({ kind: "directory", mode }) as const;
const symlinkNode = (target: string) => ({ kind: "symlink", target }) as const;

function basePlan(): Record<string, unknown> {
  return {
    schema_version: PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION,
    kind: PIPELINE_V2_APPLY_PLAN_KIND,
    apply_id: "applyone",
    run_id: "runone",
    project_id: "proj",
    project_root_binding: digest64,
    observation: digest64,
    terminal_revision: 7,
    provenance: { digest: digest64, entry_count: 3, copy_policy_version: 1 },
    policy_version: PIPELINE_V2_APPLY_POLICY_VERSION,
    steps: [],
    payloads: [],
    noops: [],
    total_payload_byte_count: 0,
  };
}

function publishPlan(): Record<string, unknown> {
  const plan = basePlan() as Record<string, unknown>;
  plan.steps = [
    {
      step_index: 1,
      change_index: 1,
      path: "a.txt",
      kind: "publish_regular_file",
      expected_before: fileNode("OLD"),
      desired_after: fileNode("NEW"),
      payload_index: 1,
    },
  ];
  plan.payloads = [{ payload_index: 1, change_index: 1, byte_count: 3, sha256: sha("NEW"), artifact: "1.payload" }];
  plan.total_payload_byte_count = 3;
  return plan;
}

describe("apply plan schema", () => {
  test("the empty plan validates and round-trips through its canonical form", () => {
    const plan = validateApplyPlan(basePlan());
    expect(plan.schema_version).toBe(1);
    expect(plan.kind).toBe("pipeline_v2_project_apply_plan");
    expect(plan.steps).toHaveLength(0);
    expect(plan.payloads).toHaveLength(0);
    expect(plan.noops).toHaveLength(0);
    expect(plan.total_payload_byte_count).toBe(0);
    const reparsed = parseApplyPlan(applyPlanCanonicalJson(plan));
    expect(applyPlanCanonicalJson(reparsed)).toBe(applyPlanCanonicalJson(plan));
  });

  test("the full step vocabulary is closed and exactly the six documented kinds", () => {
    expect([...PIPELINE_V2_APPLY_STEP_KINDS]).toEqual([
      "remove_leaf",
      "remove_directory",
      "create_directory",
      "publish_regular_file",
      "publish_symlink",
      "set_mode",
    ]);
  });

  test("every step kind validates its exact field coherence", () => {
    const removal = validateApplyPlan({
      ...basePlan(),
      steps: [{ step_index: 1, change_index: 1, path: "a.txt", kind: "remove_leaf", expected_before: fileNode("OLD"), desired_after: null }],
    });
    expect(removal.steps[0]!.desired_after).toBeNull();
    const directoryRemoval = validateApplyPlan({
      ...basePlan(),
      steps: [{ step_index: 1, change_index: 1, path: "d", kind: "remove_directory", expected_before: dirNode(0o750), desired_after: null }],
    });
    expect(directoryRemoval.steps[0]!.kind).toBe("remove_directory");
    const creation = validateApplyPlan({
      ...basePlan(),
      steps: [{ step_index: 1, change_index: 1, path: "d", kind: "create_directory", expected_before: null, desired_after: dirNode(0o700) }],
    });
    expect(creation.steps[0]!.desired_after).toEqual(dirNode(0o700));
    const retarget = validateApplyPlan({
      ...basePlan(),
      steps: [{ step_index: 1, change_index: 1, path: "l", kind: "publish_symlink", expected_before: symlinkNode("old"), desired_after: symlinkNode("new") }],
    });
    expect(retarget.steps[0]!.desired_after).toEqual(symlinkNode("new"));
    const mode = validateApplyPlan({
      ...basePlan(),
      steps: [{ step_index: 1, change_index: 1, path: "a.txt", kind: "set_mode", expected_before: fileNode("OLD"), desired_after: { kind: "regular_file", mode: 0o600, byte_count: 3, sha256: sha("OLD") } }],
    });
    expect(mode.steps[0]!.kind).toBe("set_mode");
  });

  test("dense step indexes are enforced by position", () => {
    const plan = basePlan() as Record<string, unknown>;
    plan.steps = [
      { step_index: 1, change_index: 1, path: "a", kind: "remove_leaf", expected_before: fileNode("A"), desired_after: null },
      { step_index: 3, change_index: 2, path: "b", kind: "create_directory", expected_before: null, desired_after: dirNode(0o700) },
    ];
    expect(() => validateApplyPlan(plan)).toThrow(/dense step index/);
  });

  test("the per-path step sequence allows only removal followed by creation", () => {
    const twoCreations = basePlan() as Record<string, unknown>;
    twoCreations.steps = [
      { step_index: 1, change_index: 1, path: "d", kind: "create_directory", expected_before: null, desired_after: dirNode(0o700) },
      { step_index: 2, change_index: 2, path: "d", kind: "publish_symlink", expected_before: null, desired_after: symlinkNode("t") },
    ];
    expect(() => validateApplyPlan(twoCreations)).toThrow(/unsupported step sequence/);
    const twoRemovals = basePlan() as Record<string, unknown>;
    twoRemovals.steps = [
      { step_index: 1, change_index: 1, path: "d", kind: "remove_leaf", expected_before: fileNode("A"), desired_after: null },
      { step_index: 2, change_index: 2, path: "d", kind: "remove_leaf", expected_before: fileNode("A"), desired_after: null },
    ];
    expect(() => validateApplyPlan(twoRemovals)).toThrow(/unsupported step sequence/);
    const threeSteps = basePlan() as Record<string, unknown>;
    threeSteps.steps = [
      { step_index: 1, change_index: 1, path: "d", kind: "remove_leaf", expected_before: fileNode("A"), desired_after: null },
      { step_index: 2, change_index: 2, path: "d", kind: "create_directory", expected_before: null, desired_after: dirNode(0o700) },
      { step_index: 3, change_index: 3, path: "d", kind: "set_mode", expected_before: dirNode(0o700), desired_after: dirNode(0o750) },
    ];
    expect(() => validateApplyPlan(threeSteps)).toThrow(/too many steps/);
  });

  test("per-kind coherence refusals are typed", () => {
    const badLeaf = basePlan() as Record<string, unknown>;
    badLeaf.steps = [{ step_index: 1, change_index: 1, path: "d", kind: "remove_leaf", expected_before: dirNode(0o750), desired_after: null }];
    expect(() => validateApplyPlan(badLeaf)).toThrow(/cannot remove a directory as a leaf/);
    const badDirRemoval = basePlan() as Record<string, unknown>;
    badDirRemoval.steps = [{ step_index: 1, change_index: 1, path: "a.txt", kind: "remove_directory", expected_before: fileNode("A"), desired_after: null }];
    expect(() => validateApplyPlan(badDirRemoval)).toThrow(/can only remove a directory/);
    const badCreate = basePlan() as Record<string, unknown>;
    badCreate.steps = [{ step_index: 1, change_index: 1, path: "d", kind: "create_directory", expected_before: fileNode("A"), desired_after: dirNode(0o700) }];
    expect(() => validateApplyPlan(badCreate)).toThrow(/creates into absence/);
    const fileWithoutPayload = basePlan() as Record<string, unknown>;
    fileWithoutPayload.steps = [{ step_index: 1, change_index: 1, path: "a.txt", kind: "publish_regular_file", expected_before: null, desired_after: fileNode("NEW") }];
    expect(() => validateApplyPlan(fileWithoutPayload)).toThrow(/must carry a payload index/);
    const payloadOnCreate = basePlan() as Record<string, unknown>;
    payloadOnCreate.steps = [{ step_index: 1, change_index: 1, path: "d", kind: "create_directory", expected_before: null, desired_after: dirNode(0o700), payload_index: 1 }];
    expect(() => validateApplyPlan(payloadOnCreate)).toThrow(/cannot carry a payload/);
    const modeChangeOfContent = basePlan() as Record<string, unknown>;
    modeChangeOfContent.steps = [{ step_index: 1, change_index: 1, path: "a.txt", kind: "set_mode", expected_before: fileNode("OLD"), desired_after: fileNode("NEW") }];
    expect(() => validateApplyPlan(modeChangeOfContent)).toThrow(/must not change file content/);
    const symlinkMode = basePlan() as Record<string, unknown>;
    symlinkMode.steps = [{ step_index: 1, change_index: 1, path: "l", kind: "set_mode", expected_before: symlinkNode("t"), desired_after: symlinkNode("t") }];
    expect(() => validateApplyPlan(symlinkMode)).toThrow(/must set the mode of a file or directory/);
  });

  test("payload bindings are dense, canonical and bound to publishing steps", () => {
    const unbound = basePlan() as Record<string, unknown>;
    unbound.payloads = [{ payload_index: 1, change_index: 1, byte_count: 1, sha256: digest64, artifact: "1.payload" }];
    unbound.total_payload_byte_count = 1;
    expect(() => validateApplyPlan(unbound)).toThrow(/not bound to a publishing step/);
    const dangling = basePlan() as Record<string, unknown>;
    dangling.steps = [{ step_index: 1, change_index: 1, path: "a.txt", kind: "publish_regular_file", expected_before: null, desired_after: fileNode("NEW"), payload_index: 2 }];
    expect(() => validateApplyPlan(dangling)).toThrow(/references an unknown payload/);
    const wrongName = publishPlan();
    (wrongName.payloads as Array<Record<string, unknown>>)[0]!["artifact"] = "renamed.payload";
    expect(() => validateApplyPlan(wrongName)).toThrow(/canonical payload name/);
    const wrongTotal = publishPlan();
    wrongTotal.total_payload_byte_count = 4;
    expect(() => validateApplyPlan(wrongTotal)).toThrow(/total payload byte count/);
    const foreignStepPayload = basePlan() as Record<string, unknown>;
    foreignStepPayload.steps = [{ step_index: 1, change_index: 1, path: "a.txt", kind: "set_mode", expected_before: fileNode("OLD"), desired_after: fileNode("OLD"), payload_index: 1 }];
    foreignStepPayload.payloads = [{ payload_index: 1, change_index: 1, byte_count: 3, sha256: sha("NEW"), artifact: "1.payload" }];
    foreignStepPayload.total_payload_byte_count = 3;
    expect(() => validateApplyPlan(foreignStepPayload)).toThrow(/cannot carry a payload/);
    const mismatchedBinding = publishPlan();
    ((mismatchedBinding.payloads as Array<Record<string, unknown>>)[0]!)["sha256"] = digest64;
    expect(() => validateApplyPlan(mismatchedBinding)).toThrow(/does not match its publishing step content contract/);
  });

  test("the plan covers every change index densely", () => {
    const gap = basePlan() as Record<string, unknown>;
    gap.steps = [
      { step_index: 1, change_index: 2, path: "a.txt", kind: "remove_leaf", expected_before: fileNode("A"), desired_after: null },
    ];
    expect(() => validateApplyPlan(gap)).toThrow(/change index 1/);
  });

  test("exact-field rejection at every level, without echoing unknown names", () => {
    const extraTop = { ...basePlan(), extra: 1 };
    expect(() => validateApplyPlan(extraTop)).toThrow(/unknown field/);
    const missingTop = basePlan() as Record<string, unknown>;
    delete missingTop.observation;
    expect(() => validateApplyPlan(missingTop)).toThrow(/missing the required field/);
    const extraStep = publishPlan();
    (extraStep.steps as Array<Record<string, unknown>>)[0]!["hostile"] = "x";
    expect(() => validateApplyPlan(extraStep)).toThrow(/unknown field/);
    const extraPayload = publishPlan();
    (extraPayload.payloads as Array<Record<string, unknown>>)[0]!["body"] = "secret";
    expect(() => validateApplyPlan(extraPayload)).toThrow(/unknown field/);
    const extraProvenance = basePlan() as Record<string, unknown>;
    extraProvenance.provenance = { ...(extraProvenance.provenance as Record<string, unknown>), path: "/tmp/x" };
    expect(() => validateApplyPlan(extraProvenance)).toThrow(/unknown field/);
  });

  test("scalars, paths and node shapes are validated fail-closed", () => {
    const unsafeId = basePlan() as Record<string, unknown>;
    unsafeId.apply_id = "../escape";
    expect(() => validateApplyPlan(unsafeId)).toThrow(/safe id/);
    const badBinding = basePlan() as Record<string, unknown>;
    badBinding.project_root_binding = "/abs/path";
    expect(() => validateApplyPlan(badBinding)).toThrow(/64-hex digest/);
    const absolutePath = publishPlan();
    (absoluteStep(absolutePath)).path = "/etc/passwd";
    function absoluteStep(plan: Record<string, unknown>): { path: string } {
      return (plan.steps as Array<{ path: string }>)[0]!;
    }
    expect(() => validateApplyPlan(absolutePath)).toThrow(/safe relative path/);
    const traversalPath = publishPlan();
    ((traversalPath.steps as Array<{ path: string }>)[0]!).path = "../escape";
    expect(() => validateApplyPlan(traversalPath)).toThrow(/safe relative path/);
    const badNode = publishPlan();
    ((badNode.steps as Array<Record<string, unknown>>)[0]!)["desired_after"] = { kind: "directory", mode: 0o777777 };
    expect(() => validateApplyPlan(badNode)).toThrow(/permission mode/);
    const unknownNodeKind = publishPlan();
    ((unknownNodeKind.steps as Array<Record<string, unknown>>)[0]!)["desired_after"] = { kind: "fifo" };
    expect(() => validateApplyPlan(unknownNodeKind)).toThrow(/unknown node kind/);
    const policyVersion = basePlan() as Record<string, unknown>;
    policyVersion.policy_version = 2;
    expect(() => validateApplyPlan(policyVersion)).toThrow(/policy version/);
    const schemaVersion = basePlan() as Record<string, unknown>;
    schemaVersion.schema_version = 2;
    expect(() => validateApplyPlan(schemaVersion)).toThrow(/schema version/);
  });

  test("malformed JSON keeps the stable content-free diagnostic", () => {
    try {
      parseApplyPlan("{not json");
      throw new Error("expected a refusal");
    } catch (cause) {
      expect(cause instanceof PipelineV2ApplyPlanError).toBe(true);
      expect((cause as PipelineV2ApplyPlanError).reason).toBe("invalid_json");
      expect((cause as Error).message).toBe("the stored plan is not valid JSON");
    }
  });
});

describe("apply plan digest and canonical form", () => {
  test("the digest is deterministic and domain separated", () => {
    const plan = validateApplyPlan(publishPlan());
    const first = applyPlanDigest(plan);
    const second = applyPlanDigest(validateApplyPlan(JSON.parse(applyPlanCanonicalJson(plan))));
    expect(first).toBe(second);
    expect(first).toMatch(/^[0-9a-f]{64}$/);
    const raw = createHash("sha256").update(applyPlanCanonicalJson(plan)).digest("hex");
    expect(first).not.toBe(raw);
    expect(PIPELINE_V2_APPLY_PLAN_DIGEST_DOMAIN).toBe("pipeline-v2-project-apply-plan\0");
    const withDomain = createHash("sha256")
      .update(PIPELINE_V2_APPLY_PLAN_DIGEST_DOMAIN)
      .update(applyPlanCanonicalJson(plan), "utf8")
      .digest("hex");
    expect(first).toBe(withDomain);
  });

  test("the digest is sensitive to every execution-relevant field class", () => {
    const reference = applyPlanDigest(validateApplyPlan(publishPlan()));
    const stepOrder = publishPlan();
    const stepOrderPlan = validateApplyPlan({
      ...stepOrder,
      noops: [{ change_index: 2, path: "b.txt", expected: fileNode("B") }],
      steps: [
        (stepOrder.steps as Array<Record<string, unknown>>)[0],
        { step_index: 2, change_index: 2, path: "b.txt", kind: "set_mode", expected_before: fileNode("B"), desired_after: { kind: "regular_file", mode: 0o600, byte_count: 1, sha256: sha("B") } },
      ],
    });
    expect(applyPlanDigest(stepOrderPlan)).not.toBe(reference);
    const payloadBytes = publishPlan();
    ((payloadBytes.payloads as Array<Record<string, unknown>>)[0]!)["sha256"] = digest64;
    expect(() => validateApplyPlan(payloadBytes)).toThrow(); // the step binding no longer matches
    const nodeMode = publishPlan();
    ((nodeMode.steps as Array<Record<string, unknown>>)[0]! as Record<string, unknown>)["desired_after"] = { kind: "regular_file", mode: 0o644, byte_count: 3, sha256: sha("NEW") };
    expect(applyPlanDigest(validateApplyPlan(nodeMode))).not.toBe(reference);
    const target = basePlan() as Record<string, unknown>;
    target.steps = [{ step_index: 1, change_index: 1, path: "l", kind: "publish_symlink", expected_before: symlinkNode("old"), desired_after: symlinkNode("changed") }];
    expect(applyPlanDigest(validateApplyPlan(target))).not.toBe(reference);
    const identity = basePlan() as Record<string, unknown>;
    identity.observation = sha("other");
    expect(applyPlanDigest(validateApplyPlan(identity))).not.toBe(reference);
    const binding = basePlan() as Record<string, unknown>;
    binding.project_root_binding = sha("other-root");
    expect(applyPlanDigest(validateApplyPlan(binding))).not.toBe(reference);
  });

  test("the canonical plan carries no host path, run root, content or timestamp", () => {
    const plan = validateApplyPlan(publishPlan());
    const canonical = applyPlanCanonicalJson(plan);
    expect(canonical).not.toContain("/");
    expect(canonical).not.toContain("T2");
    expect(canonical).not.toContain("NEW");
    expect(canonical).not.toContain("\\u0000");
    expect(canonical).not.toMatch(/"dev"|"ino"|"uid"|"gid"/);
  });
});
