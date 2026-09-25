import type { PipelineV2RunPlanStoreCandidate } from "../src/pipeline_v2_run_plan_store.ts";

/**
 * Type-level contract of the run-plan store's durability-unknown
 * candidate: the exact discriminated union. This module is checked by the
 * shared `tsc --noEmit -p orchestrator` run and deliberately exposes no
 * runtime value — `bun test` does not collect it.
 */

const hex = (char: string): string => char.repeat(64);

const taskCandidate: PipelineV2RunPlanStoreCandidate = {
  kind: "task",
  run_id: "run-1",
  task_id: "task-1",
  revision: 1,
  sha256: hex("a"),
  final_path: "/x",
};

const planCandidate: PipelineV2RunPlanStoreCandidate = {
  kind: "plan",
  run_id: "run-1",
  revision: 1,
  sha256: hex("b"),
  final_path: "/x",
};

const waitIntentCandidate: PipelineV2RunPlanStoreCandidate = {
  kind: "wait_intent",
  run_id: "run-1",
  wait_index: 3,
  sha256: hex("c"),
  final_path: "/x",
};

// @ts-expect-error a task candidate without its task id
const taskWithoutTaskId: PipelineV2RunPlanStoreCandidate = {
  kind: "task",
  run_id: "run-1",
  revision: 1,
  sha256: hex("a"),
  final_path: "/x",
};

// @ts-expect-error a task candidate without its revision
const taskWithoutRevision: PipelineV2RunPlanStoreCandidate = {
  kind: "task",
  run_id: "run-1",
  task_id: "task-1",
  sha256: hex("a"),
  final_path: "/x",
};

const taskWithWaitIndex: PipelineV2RunPlanStoreCandidate = {
  kind: "task",
  run_id: "run-1",
  task_id: "task-1",
  revision: 1,
  // @ts-expect-error a task candidate with a wait index
  wait_index: 3,
  sha256: hex("a"),
  final_path: "/x",
};

const planWithTaskId: PipelineV2RunPlanStoreCandidate = {
  kind: "plan",
  run_id: "run-1",
  revision: 1,
  // @ts-expect-error a plan candidate with a task id
  task_id: "task-1",
  sha256: hex("b"),
  final_path: "/x",
};

const planWithWaitIndex: PipelineV2RunPlanStoreCandidate = {
  kind: "plan",
  run_id: "run-1",
  revision: 1,
  // @ts-expect-error a plan candidate with a wait index
  wait_index: 3,
  sha256: hex("b"),
  final_path: "/x",
};

// @ts-expect-error a wait-intent candidate without its wait index
const intentWithoutWaitIndex: PipelineV2RunPlanStoreCandidate = {
  kind: "wait_intent",
  run_id: "run-1",
  sha256: hex("c"),
  final_path: "/x",
};

const intentWithRevision: PipelineV2RunPlanStoreCandidate = {
  kind: "wait_intent",
  run_id: "run-1",
  wait_index: 3,
  // @ts-expect-error a wait-intent candidate with a revision
  revision: 1,
  sha256: hex("c"),
  final_path: "/x",
};

const intentWithTaskId: PipelineV2RunPlanStoreCandidate = {
  kind: "wait_intent",
  run_id: "run-1",
  wait_index: 3,
  // @ts-expect-error a wait-intent candidate with a task id
  task_id: "task-1",
  sha256: hex("c"),
  final_path: "/x",
};

/**
 * The narrowing proof: after the kind check the branch's mandatory fields
 * are present without undefined, and the other branches' fields are not
 * accessible — the checks below are the compiled proof.
 */
function candidateIdentity(candidate: PipelineV2RunPlanStoreCandidate): string {
  if (candidate.kind === "task") {
    const taskId: string = candidate.task_id;
    const revision: number = candidate.revision;
    // @ts-expect-error the wait index is not a task field
    const waitIndex = candidate.wait_index;
    return `${taskId}/${revision}/${String(waitIndex)}`;
  }
  if (candidate.kind === "plan") {
    const revision: number = candidate.revision;
    // @ts-expect-error the task id is not a plan field
    const taskId = candidate.task_id;
    // @ts-expect-error the wait index is not a plan field
    const waitIndex = candidate.wait_index;
    return `plan/${revision}/${String(taskId)}${String(waitIndex)}`;
  }
  const waitIndex: number = candidate.wait_index;
  // @ts-expect-error the task id is not a wait-intent field
  const taskId = candidate.task_id;
  // @ts-expect-error the revision is not a wait-intent field
  const revision = candidate.revision;
  return `intent/${waitIndex}/${String(taskId)}${String(revision)}`;
}

void taskCandidate;
void planCandidate;
void waitIntentCandidate;
void taskWithoutTaskId;
void taskWithoutRevision;
void taskWithWaitIndex;
void planWithTaskId;
void planWithWaitIndex;
void intentWithoutWaitIndex;
void intentWithRevision;
void intentWithTaskId;
void candidateIdentity;
