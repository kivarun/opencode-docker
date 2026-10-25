import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  PIPELINE_V2_APPLY_POLICY_VERSION,
  PIPELINE_V2_APPLY_PLAN_KIND,
  PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION,
  validateApplyPlan,
  type PipelineV2ApplyNode,
  type PipelineV2ApplyPlan,
  type PipelineV2ApplyPlanNoop,
  type PipelineV2ApplyPlanPayload,
  type PipelineV2ApplyPlanStep,
  type PipelineV2ApplyStepKind,
} from "./pipeline_v2_apply_plan_internal.ts";
import type { PipelineV2ProjectChangeEntry } from "./pipeline_v2_project_changes_internal.ts";
import type { PreparedPipelineV2ProjectProvenance } from "./pipeline_v2_project_provenance.ts";

/**
 * The deterministic apply-all planner over the verified Project Changes
 * observation. It consumes the single production observation — the raw
 * operator tree scan plus the public change set (never a second scanner)
 * — and produces the byte-identical canonical plan for identical verified
 * input.
 *
 * Planner semantics (fail closed, zero-write):
 *   - every affected path (a reviewed change entry path) must currently
 *     match its baseline node or its result node; anything else is
 *     `diverged_project` and the whole preparation is refused before any
 *     artifact exists;
 *   - a path already at its result node becomes a durable no-op, recorded
 *     with its expected-after state (null = the path must be absent) so
 *     the package loader and the future writer can verify it;
 *   - the eligibility evaluation is projection-aware: a path is judged
 *     against its own current object, not against intermediate plan
 *     state, so an added child under a `file→directory` substitution is
 *     planned (its absence matches the baseline) even though the parent
 *     is still a file before the first step;
 *   - every planned removal is explicit; there is no recursive-delete
 *     step. Before a directory is removed or replaced, every operator
 *     descendant must be covered by the plan's own removal steps — any
 *     operator-only descendant is a `diverged_project` conflict, because
 *     a blind delete would silently destroy local data;
 *   - type substitutions produce separate durable steps (removal, then
 *     creation), so a crash between them has an unambiguous position;
 *   - hardlink topology in the reviewed nodes is refused for v1
 *     (`unsupported_metadata`); the metadata probe owner decides the
 *     remaining metadata policy over the live objects;
 *   - the projected state is simulated step by step; the final projected
 *     tree on every affected path must equal the reviewed result.
 *
 * Determinism: phases are fixed — removals deepest-first, directory
 * creations shallowest-first, regular-file publications, symlink
 * publications, mode-only steps last — with lexical path order inside a
 * phase, so identical verified input yields the byte-identical canonical
 * plan.
 */

export type PipelineV2ApplyPlannerRefusal =
  | "diverged_project"
  | "unsupported_metadata"
  | "unsupported_project_entry";

export class PipelineV2ApplyPlannerError extends Error {
  declare readonly refusal: PipelineV2ApplyPlannerRefusal;

  constructor(refusal: PipelineV2ApplyPlannerRefusal, message: string) {
    super(message);
    this.name = "PipelineV2ApplyPlannerError";
    Object.defineProperty(this, "refusal", {
      value: refusal,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

/** One observed operator tree node as the planner sees it. */
export interface ObservedApplyNode {
  readonly kind: "directory" | "regular_file" | "symlink";
  readonly mode: number;
  readonly byteCount: number;
  readonly sha256: string;
  readonly target: string;
  readonly nlink: number;
}

export interface PlannerInputTree {
  readonly operator: ReadonlyMap<string, ObservedApplyNode>;
  readonly changes: readonly PipelineV2ProjectChangeEntry[];
  readonly baseline: PreparedPipelineV2ProjectProvenance;
}

interface PlanNode {
  readonly kind: "directory" | "regular_file" | "symlink";
  readonly mode: number;
  readonly byte_count: number;
  readonly sha256: string;
  readonly target: string;
}

function observedToPlanNode(entry: ObservedApplyNode): PlanNode {
  return { kind: entry.kind, mode: entry.mode & 0o777, byte_count: entry.byteCount, sha256: entry.sha256, target: entry.target };
}

function entryNodeToPlanNode(node: PipelineV2ApplyNode | null): PlanNode | null {
  if (node === null) {
    return null;
  }
  if (node.kind === "directory") {
    return { kind: "directory", mode: node.mode, byte_count: 0, sha256: "", target: "" };
  }
  if (node.kind === "symlink") {
    return { kind: "symlink", mode: 0o777, byte_count: 0, sha256: "", target: node.target };
  }
  return { kind: "regular_file", mode: node.mode, byte_count: node.byte_count, sha256: node.sha256, target: "" };
}

function nodesEqual(a: PlanNode | undefined | null, b: PlanNode | null): boolean {
  const aAbsent = a === undefined || a === null;
  const bAbsent = b === null;
  if (aAbsent || bAbsent) {
    return aAbsent && bAbsent;
  }
  if (a!.kind !== b!.kind) {
    return false;
  }
  if (a!.kind === "directory") {
    return a!.mode === b!.mode;
  }
  if (a!.kind === "symlink") {
    return a!.target === b!.target;
  }
  return a!.mode === b!.mode && a!.byte_count === b!.byte_count && a!.sha256 === b!.sha256;
}

function expectDirectoryNode(node: PipelineV2ApplyNode, what: string): { readonly kind: "directory"; readonly mode: number } {
  if (node.kind !== "directory") {
    throw new Error(`the apply plan projection is inconsistent: ${what} is not a directory node`);
  }
  return node;
}

function expectFileNode(node: PipelineV2ApplyNode, what: string): { readonly kind: "regular_file"; readonly mode: number; readonly byte_count: number; readonly sha256: string } {
  if (node.kind !== "regular_file") {
    throw new Error(`the apply plan projection is inconsistent: ${what} is not a regular-file node`);
  }
  return node;
}

function expectSymlinkNode(node: PipelineV2ApplyNode, what: string): { readonly kind: "symlink"; readonly target: string } {
  if (node.kind !== "symlink") {
    throw new Error(`the apply plan projection is inconsistent: ${what} is not a symlink node`);
  }
  return node;
}

function nodeToApplyNode(node: PlanNode): PipelineV2ApplyNode {
  if (node.kind === "directory") {
    return { kind: "directory", mode: node.mode };
  }
  if (node.kind === "symlink") {
    return { kind: "symlink", target: node.target };
  }
  return { kind: "regular_file", mode: node.mode, byte_count: node.byte_count, sha256: node.sha256 };
}

const depthOf = (path: string): number => path.split("/").length;

interface RawStep {
  readonly changeIndex: number;
  readonly path: string;
  readonly kind: PipelineV2ApplyStepKind;
  readonly expectedBefore: PlanNode | null;
  readonly desiredAfter: PlanNode | null;
  readonly phase: number;
}

/**
 * Plans every reviewed change entry of one verified observation into the
 * ordered canonical steps and validates the result through the single
 * plan schema owner before returning it.
 */
export function planProjectApplySteps(
  tree: PlannerInputTree,
  applyId: string,
  runId: string,
  projectId: string,
  projectRootBinding: string,
  observation: string,
  terminalRevision: number,
): PipelineV2ApplyPlan {
  const operatorNodes = new Map<string, PlanNode>();
  for (const [path, entry] of tree.operator) {
    operatorNodes.set(path, observedToPlanNode(entry));
  }

  // 1. Eligibility against the current operator objects: every affected
  //    path must equal its baseline node or its result node.
  const requiresTransformation = new Set<string>();
  const noops: PipelineV2ApplyPlanNoop[] = [];
  const baselineByPath = new Map<string, PlanNode | null>();
  const resultByPath = new Map<string, PlanNode | null>();
  for (const change of tree.changes) {
    const baselineNode = entryNodeToPlanNode(change.baseline);
    const resultNode = entryNodeToPlanNode(change.result);
    baselineByPath.set(change.path, baselineNode);
    resultByPath.set(change.path, resultNode);
    const opNode = operatorNodes.get(change.path) ?? null;
    const matchesBaseline = nodesEqual(opNode, baselineNode);
    const matchesResult = nodesEqual(opNode, resultNode);
    if (!matchesBaseline && !matchesResult) {
      throw new PipelineV2ApplyPlannerError("diverged_project", "an affected path no longer matches the reviewed observation");
    }
    if (matchesResult) {
      noops.push(deepFreezeValue({
        change_index: change.index,
        path: change.path,
        expected: change.result === null ? null : nodeToApplyNode(entryNodeToPlanNode(change.result)!),
      }));
      continue;
    }
    requiresTransformation.add(change.path);
  }

  // 2. Hardlink policy over the reviewed nodes (v1 refuses hardlinks).
  for (const change of tree.changes) {
    if (change.result !== null && change.result.kind === "regular_file" && change.result.multiple_links) {
      throw new PipelineV2ApplyPlannerError("unsupported_metadata", "a reviewed result carries a hardlink group");
    }
    if (change.baseline !== null && change.baseline.kind === "regular_file" && change.baseline.multiple_links && requiresTransformation.has(change.path)) {
      throw new PipelineV2ApplyPlannerError("unsupported_metadata", "a touched baseline path carries a hardlink group");
    }
  }

  // 3. Directory containment: every operator descendant of a removed or
  //    replaced directory must be covered by the plan's own removals.
  const removalDirs = new Set<string>();
  for (const path of requiresTransformation) {
    const opNode = operatorNodes.get(path);
    if (opNode === undefined || opNode.kind !== "directory") {
      continue;
    }
    const resultNode = resultByPath.get(path) ?? null;
    if (resultNode === null || resultNode.kind !== "directory") {
      removalDirs.add(path);
    }
  }
  for (const dirPath of removalDirs) {
    const prefix = `${dirPath}/`;
    for (const candidate of operatorNodes.keys()) {
      if (candidate.startsWith(prefix) && !requiresTransformation.has(candidate)) {
        throw new PipelineV2ApplyPlannerError(
          "diverged_project",
          "an operator-only path lives inside a directory the reviewed plan removes",
        );
      }
    }
  }

  // 4. Build the raw steps per transformed path.
  const rawSteps: RawStep[] = [];
  let payloadCount = 0;
  const payloadByPath = new Map<string, PipelineV2ApplyPlanPayload>();
  const pushFilePublication = (changeIndex: number, path: string, result: PlanNode, phase: number, expectedBefore: PlanNode | null): void => {
    payloadCount += 1;
    const payload: PipelineV2ApplyPlanPayload = deepFreezeValue({
      payload_index: payloadCount,
      change_index: changeIndex,
      byte_count: result.byte_count,
      sha256: result.sha256,
      artifact: `${payloadCount}.payload`,
    });
    payloadByPath.set(path, payload);
    rawSteps.push({ changeIndex, path, kind: "publish_regular_file", expectedBefore, desiredAfter: result, phase });
  };
  const changeByPath = new Map<string, PipelineV2ProjectChangeEntry>();
  for (const change of tree.changes) {
    changeByPath.set(change.path, change);
  }
  for (const path of requiresTransformation) {
    const change = changeByPath.get(path)!;
    const opNode = operatorNodes.get(path) ?? null;
    const resultNode = resultByPath.get(path)!;
    const opKind = opNode?.kind ?? null;
    const resultKind = resultNode?.kind ?? null;

    if (opKind === null) {
      if (resultKind === "directory") {
        rawSteps.push({ changeIndex: change.index, path, kind: "create_directory", expectedBefore: null, desiredAfter: resultNode, phase: 30 });
      } else if (resultKind === "regular_file") {
        pushFilePublication(change.index, path, resultNode!, 40, null);
      } else if (resultKind === "symlink") {
        rawSteps.push({ changeIndex: change.index, path, kind: "publish_symlink", expectedBefore: null, desiredAfter: resultNode, phase: 50 });
      }
      continue;
    }

    if (resultKind === opKind) {
      if (opKind === "regular_file") {
        if (resultNode!.sha256 === opNode!.sha256 && resultNode!.byte_count === opNode!.byte_count) {
          if (resultNode!.mode !== opNode!.mode) {
            rawSteps.push({ changeIndex: change.index, path, kind: "set_mode", expectedBefore: opNode, desiredAfter: resultNode, phase: 60 });
          }
        } else {
          pushFilePublication(change.index, path, resultNode!, 40, opNode);
        }
        continue;
      }
      if (opKind === "directory") {
        if (resultNode!.mode !== opNode!.mode) {
          rawSteps.push({ changeIndex: change.index, path, kind: "set_mode", expectedBefore: opNode, desiredAfter: resultNode, phase: 60 });
        }
        continue;
      }
      // symlink → symlink: one atomic replacement over the old link object.
      rawSteps.push({ changeIndex: change.index, path, kind: "publish_symlink", expectedBefore: opNode, desiredAfter: resultNode, phase: 50 });
      continue;
    }

    if (opKind === "directory" && resultKind === "directory") {
      if (resultNode!.mode !== opNode!.mode) {
        rawSteps.push({ changeIndex: change.index, path, kind: "set_mode", expectedBefore: opNode, desiredAfter: resultNode, phase: 60 });
      }
      continue;
    }

    // Type substitution: explicit removal, then explicit creation. A
    // replaced directory relies on the child removals the deleted
    // descendant entries produce (the containment check above proved the
    // operator subtree is fully covered).
    rawSteps.push({
      changeIndex: change.index,
      path,
      kind: opKind === "directory" ? "remove_directory" : "remove_leaf",
      expectedBefore: opNode,
      desiredAfter: null,
      phase: 10,
    });
    if (resultKind === "directory") {
      rawSteps.push({ changeIndex: change.index, path, kind: "create_directory", expectedBefore: null, desiredAfter: resultNode, phase: 30 });
    } else if (resultKind === "regular_file") {
      pushFilePublication(change.index, path, resultNode!, 40, null);
    } else if (resultKind === "symlink") {
      rawSteps.push({ changeIndex: change.index, path, kind: "publish_symlink", expectedBefore: null, desiredAfter: resultNode, phase: 50 });
    }
  }

  // 5. Deterministic ordering.
  const ordered = [...rawSteps].sort((a, b) => {
    if (a.phase !== b.phase) {
      return a.phase - b.phase;
    }
    const direction = a.phase === 10 ? -1 : 1;
    const depthDelta = depthOf(a.path) - depthOf(b.path);
    if (depthDelta !== 0) {
      return direction * depthDelta;
    }
    return a.path < b.path ? -1 : a.path > b.path ? 1 : 0;
  });
  const steps: PipelineV2ApplyPlanStep[] = ordered.map((raw, position) => {
    const payload = raw.kind === "publish_regular_file" ? payloadByPath.get(raw.path) : undefined;
    return deepFreezeValue({
      step_index: position + 1,
      change_index: raw.changeIndex,
      path: raw.path,
      kind: raw.kind,
      expected_before: raw.expectedBefore === null ? null : nodeToApplyNode(raw.expectedBefore),
      desired_after: raw.desiredAfter === null ? null : nodeToApplyNode(raw.desiredAfter),
      ...(payload === undefined ? {} : { payload_index: payload.payload_index }),
    });
  });
  const payloads = [...payloadByPath.values()].sort((a, b) => a.payload_index - b.payload_index);
  const totalPayloadByteCount = payloads.reduce((acc, payload) => acc + payload.byte_count, 0);

  // 6. Projected-state verification: simulate the steps over the operator
  //    tree; every affected path must end exactly at its reviewed result.
  const projected = new Map<string, PlanNode>();
  for (const [path, node] of operatorNodes) {
    projected.set(path, node);
  }
  const descendantsOf = (path: string): string[] => {
    const prefix = `${path}/`;
    const found: string[] = [];
    for (const candidate of projected.keys()) {
      if (candidate.startsWith(prefix)) {
        found.push(candidate);
      }
    }
    return found;
  };
  const inconsistent = (what: string): Error => new Error(`the apply plan projection is inconsistent: ${what}`);
  for (const step of steps) {
    const current = projected.get(step.path);
    switch (step.kind) {
      case "remove_leaf": {
        if (current === undefined || current.kind === "directory") {
          throw inconsistent("the projected state does not carry the leaf to remove");
        }
        projected.delete(step.path);
        break;
      }
      case "remove_directory": {
        if (current === undefined || current.kind !== "directory") {
          throw inconsistent("the projected state does not carry the directory to remove");
        }
        if (descendantsOf(step.path).length > 0) {
          throw new PipelineV2ApplyPlannerError("diverged_project", "a directory cannot be removed while its projected children remain");
        }
        projected.delete(step.path);
        break;
      }
      case "create_directory": {
        if (current !== undefined) {
          throw inconsistent("the projected state already carries the directory to create");
        }
        const segments = step.path.split("/");
        for (let index = 1; index < segments.length; index += 1) {
          const ancestor = segments.slice(0, index).join("/");
          const ancestorNode = projected.get(ancestor);
          if (ancestorNode === undefined || ancestorNode.kind !== "directory") {
            throw inconsistent("a directory creation lacks its projected parent");
          }
        }
        projected.set(step.path, { kind: "directory", mode: expectDirectoryNode(step.desired_after!, "the directory creation").mode, byte_count: 0, sha256: "", target: "" });
        break;
      }
      case "publish_regular_file": {
        if (current !== undefined && current.kind === "directory") {
          throw inconsistent("a file publication cannot replace a projected directory");
        }
        const fileAfter = expectFileNode(step.desired_after!, "the file publication");
        projected.set(step.path, { kind: "regular_file", mode: fileAfter.mode, byte_count: fileAfter.byte_count, sha256: fileAfter.sha256, target: "" });
        break;
      }
      case "publish_symlink": {
        if (current !== undefined && current.kind === "directory") {
          throw inconsistent("a symlink publication cannot replace a projected directory");
        }
        projected.set(step.path, { kind: "symlink", mode: 0o777, byte_count: 0, sha256: "", target: expectSymlinkNode(step.desired_after!, "the symlink publication").target });
        break;
      }
      case "set_mode": {
        const modeAfter = step.desired_after!;
        if (current === undefined || current.kind !== modeAfter.kind || modeAfter.kind === "symlink") {
          throw inconsistent("the projected state does not carry the object to re-mode");
        }
        projected.set(step.path, { ...current, mode: modeAfter.mode });
        break;
      }
    }
  }
  for (const change of tree.changes) {
    const projectedNode = projected.get(change.path) ?? null;
    const resultNode = resultByPath.get(change.path) ?? null;
    if (!nodesEqual(projectedNode, resultNode)) {
      throw new PipelineV2ApplyPlannerError("diverged_project", "the projected final state does not match the reviewed result");
    }
  }

  const plan = deepFreezeValue({
    schema_version: PIPELINE_V2_APPLY_PLAN_SCHEMA_VERSION,
    kind: PIPELINE_V2_APPLY_PLAN_KIND,
    apply_id: applyId,
    run_id: runId,
    project_id: projectId,
    project_root_binding: projectRootBinding,
    observation,
    terminal_revision: terminalRevision,
    provenance: deepFreezeValue({
      digest: tree.baseline.sha256,
      entry_count: tree.baseline.manifest.entries.length,
      copy_policy_version: tree.baseline.manifest.copy_policy_version,
    }),
    policy_version: PIPELINE_V2_APPLY_POLICY_VERSION,
    steps: deepFreezeValue(steps),
    payloads: deepFreezeValue(payloads),
    noops: deepFreezeValue(noops),
    total_payload_byte_count: totalPayloadByteCount,
  });
  return validateApplyPlan(plan);
}
