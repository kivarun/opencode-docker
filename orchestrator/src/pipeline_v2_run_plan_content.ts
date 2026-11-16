/**
 * The versioned read-only task/plan content projection of one pipeline
 * v2 run (P-1): the byte-exact chunk stream of one accepted task
 * revision's body and the paged deterministic semantic record projection
 * of one accepted plan revision.
 *
 * The single public facade for both surfaces is this module. It reuses —
 * never duplicates — the established production owners: the shared
 * read-only durable-run load chain (`loadDurablePipelineV2Run`), the
 * single prepared-manifest substrate (`prepare*`/`parse*` own all
 * validation and digests), the single run-plan store loaders (the only
 * artifact read path) and `stageTaskBodyDigest` as the body-digest
 * owner. There is no second parser, no second state validator, no
 * parallel content reconstruction from events or identity records, and
 * no write of any kind on any path (no mutation lock, no temporary file,
 * no materialization, no project access, no Session, no helper call).
 *
 * Authority: the caller names run identity and accepted revision
 * identity only (safe ids and safe integers) — never a filesystem path.
 * The ledger is re-read from the validated durable state on every
 * request; an identity without exactly one accepted ledger record fails
 * closed (`unknown_revision`) before any artifact is read. The loaded
 * prepared manifest must then match the ledger record exactly; a missing
 * artifact is `artifact_missing`, any canonical/layout/kind/mode/symlink
 * or digest divergence is `artifact_modified`, and unexpected I/O is
 * `internal_error` — all with zero content bytes.
 *
 * Every success document is path-free (no run root, no bundle root, no
 * artifact path) and content-free in its envelope members; the task
 * body bytes travel only inside `dataBase64` and the body digest is
 * always the dedicated `pipeline-v2-stage-task-body\0` digest of the
 * full body, never compared across digest domains.
 */
import { loadDurablePipelineV2Run } from "./pipeline_v2_inspect.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import { isPipelineV2SafeId } from "./pipeline_v2_scalar.ts";
import { stageTaskBodyDigest } from "./pipeline_v2_stage_task.ts";
import {
  loadPipelineV2PlanRevision,
  loadPipelineV2TaskRevision,
  PipelineV2RunPlanStoreError,
} from "./pipeline_v2_run_plan_store.ts";
import type {
  PipelineV2RunPlanRevisionManifest,
  PreparedPipelineV2RunPlanRevision,
  PreparedPipelineV2RunTaskRevision,
} from "./pipeline_v2_run_plan_manifests.ts";
import type { PipelineV2RunState, PipelineV2TaskRevisionState, PipelineV2PlanRevisionState } from "./pipeline_v2_state.ts";

/** The default and maximum raw-byte chunk size of the task body stream. */
export const PIPELINE_V2_RUN_PLAN_TASK_DEFAULT_CHUNK = 65_536;
export const PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK = 193_536;

/** The default and maximum record-page size of the plan projection. */
export const PIPELINE_V2_RUN_PLAN_PLAN_DEFAULT_LIMIT = 100;
export const PIPELINE_V2_RUN_PLAN_PLAN_MAX_LIMIT = 200;

/**
 * The byte cap of the complete serialized CLI document and of the
 * complete serialized HTTP document (each surface independently): the
 * page/chunk builders keep every produced page below this value minus
 * the envelope reserve, and the CLI/HTTP envelopes are pinned below the
 * reserve by permanent boundary tests.
 */
export const PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP = 262_144;

/**
 * The serialized envelope budget excluded from the page budget: the
 * worst-case CLI/HTTP envelope around one page (identity fields at
 * maximum safe-id lengths, digests, cursor fields and JSON syntax) is
 * pinned below this value by boundary tests on both surfaces.
 */
export const PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE = 1_024;

/** The closed plan projection record vocabulary. */
export const PIPELINE_V2_RUN_PLAN_RECORD_KINDS = ["stage", "task", "task_dependency"] as const;

/** The task origin classes as carried by the accepted task manifest. */
export const PIPELINE_V2_RUN_PLAN_TASK_ORIGINS = ["planning_proposal", "user_response"] as const;

export type PipelineV2RunPlanContentReason =
  | "run_not_found"
  | "invalid_options"
  | "run_state_invalid"
  | "pipeline_mismatch"
  | "unknown_revision"
  | "artifact_missing"
  | "artifact_modified"
  | "invalid_cursor"
  | "internal_error";

const REASONS: ReadonlySet<PipelineV2RunPlanContentReason> = new Set<PipelineV2RunPlanContentReason>([
  "run_not_found",
  "invalid_options",
  "run_state_invalid",
  "pipeline_mismatch",
  "unknown_revision",
  "artifact_missing",
  "artifact_modified",
  "invalid_cursor",
  "internal_error",
]);

export class PipelineV2RunPlanContentError extends Error {
  declare readonly reason: PipelineV2RunPlanContentReason;

  constructor(reason: PipelineV2RunPlanContentReason, message: string) {
    if (!REASONS.has(reason)) {
      throw new TypeError(`unknown pipeline v2 run plan content error reason ${JSON.stringify(reason)}`);
    }
    super(message);
    this.name = "PipelineV2RunPlanContentError";
    Object.defineProperty(this, "reason", {
      value: reason,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }
}

/** The options of one read-only `orchestrator task-revision` invocation. */
export interface PipelineV2TaskContentOptions {
  readonly runId: string;
  readonly stateRoot: string;
  readonly taskId: string;
  readonly revision: number;
  readonly offset?: number;
  readonly limit?: number;
}

/** The options of one read-only `orchestrator plan-revision` invocation. */
export interface PipelineV2PlanContentOptions {
  readonly runId: string;
  readonly stateRoot: string;
  readonly revision: number;
  readonly after?: number;
  readonly limit?: number;
}

/** The shared path-free failure document (exact top-level keys). */
export interface PipelineV2RunPlanContentFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly reason: PipelineV2RunPlanContentReason;
}

/** One task-body chunk of the byte-exact content stream. */
export interface PipelineV2TaskContentReportBody {
  readonly byteCount: number;
  readonly sha256: string;
  readonly offset: number;
  readonly limit: number;
  readonly returnedByteCount: number;
  readonly nextOffset: number | null;
  readonly dataBase64: string;
}

/** The versioned success document of one task-body chunk. */
export interface PipelineV2TaskContentReport {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly taskId: string;
  readonly revision: number;
  readonly manifestSha256: string;
  readonly previousSha256: string | null;
  readonly origin: "planning_proposal" | "user_response";
  readonly body: PipelineV2TaskContentReportBody;
}

export type PipelineV2TaskContentOutcome = PipelineV2TaskContentReport | PipelineV2RunPlanContentFailure;

/** One projected stage record. */
export interface PipelineV2PlanContentStageRecord {
  readonly record: "stage";
  readonly stage_position: number;
  readonly id: string;
  readonly template: string;
}

/** One projected task record. */
export interface PipelineV2PlanContentTaskRecord {
  readonly record: "task";
  readonly stage_position: number;
  readonly task_position: number;
  readonly id: string;
  readonly revision: number;
  readonly sha256: string;
}

/** One projected task-dependency record. */
export interface PipelineV2PlanContentTaskDependencyRecord {
  readonly record: "task_dependency";
  readonly stage_position: number;
  readonly task_position: number;
  readonly dependency_position: number;
  readonly task_id: string;
}

export type PipelineV2PlanContentRecord =
  | PipelineV2PlanContentStageRecord
  | PipelineV2PlanContentTaskRecord
  | PipelineV2PlanContentTaskDependencyRecord;

/** The versioned success document of one plan projection page. */
export interface PipelineV2PlanContentReport {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly revision: number;
  readonly manifestSha256: string;
  readonly previousSha256: string | null;
  readonly originExecution: number;
  readonly after: number;
  readonly records: readonly PipelineV2PlanContentRecord[];
  readonly nextAfter: number | null;
}

export type PipelineV2PlanContentOutcome = PipelineV2PlanContentReport | PipelineV2RunPlanContentFailure;

function failure(runId: string, reason: PipelineV2RunPlanContentReason): PipelineV2RunPlanContentFailure {
  return deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, reason });
}

/**
 * The record-page builder: flattens the verified plan manifest into the
 * fixed semantic order and applies the stop-before rule against the
 * envelope cap. Records are projected lazily so the page budget bounds
 * the amount of work per page, never the artifact load itself.
 */
export function projectPlanRecordsPage(
  total: number,
  recordAt: (index: number) => PipelineV2PlanContentRecord,
  after: number,
  limit: number,
): { records: readonly PipelineV2PlanContentRecord[]; nextAfter: number | null } {
  const pageBudget = PIPELINE_V2_RUN_PLAN_ENVELOPE_BYTE_CAP - PIPELINE_V2_RUN_PLAN_PAGE_ENVELOPE_RESERVE;
  const records: PipelineV2PlanContentRecord[] = [];
  let used = 0;
  let index = after;
  while (index < total && records.length < limit) {
    const record = recordAt(index);
    const serialized = JSON.stringify(record);
    // All projected record members are ASCII by grammar (safe ids,
    // lowercase hex digests, decimal integers), so one JSON code unit is
    // one UTF-8 byte; the exact per-record byte length is the serialized
    // length.
    const byteLength = serialized.length;
    const delimiter = records.length > 0 ? 1 : 0;
    if (used + byteLength + delimiter > pageBudget) {
      break;
    }
    records.push(record);
    used += byteLength + delimiter;
    index += 1;
  }
  if (index < total && records.length === 0) {
    // Unproducible by the record grammar (pinned by the unproducibility
    // test); a producer invariant violation is never a partial success.
    throw new PipelineV2RunPlanContentError("internal_error", "the projection record does not fit the page budget");
  }
  const nextAfter = index < total ? index : null;
  return { records, nextAfter };
}

/** The exact semantic order of the verified plan manifest. */
export function planRecordCount(manifest: PipelineV2RunPlanRevisionManifest): number {
  let count = 0;
  for (const stage of manifest.stages) {
    count += 1;
    for (const task of stage.tasks) {
      count += 1;
      count += task.depends_on.length;
    }
  }
  return count;
}

function planRecordAt(manifest: PipelineV2RunPlanRevisionManifest): (index: number) => PipelineV2PlanContentRecord {
  // The fixed flattening order is deterministic; the index walk below is
  // a direct enumeration of that order.
  const entries: PipelineV2PlanContentRecord[] = [];
  for (let stageIndex = 0; stageIndex < manifest.stages.length; stageIndex += 1) {
    const stage = manifest.stages[stageIndex]!;
    const stagePosition = stageIndex + 1;
    entries.push({ record: "stage", stage_position: stagePosition, id: stage.id, template: stage.template });
    for (let taskIndex = 0; taskIndex < stage.tasks.length; taskIndex += 1) {
      const task = stage.tasks[taskIndex]!;
      const taskPosition = taskIndex + 1;
      entries.push({
        record: "task",
        stage_position: stagePosition,
        task_position: taskPosition,
        id: task.id,
        revision: task.revision,
        sha256: task.sha256,
      });
      for (let dependencyIndex = 0; dependencyIndex < task.depends_on.length; dependencyIndex += 1) {
        entries.push({
          record: "task_dependency",
          stage_position: stagePosition,
          task_position: taskPosition,
          dependency_position: dependencyIndex + 1,
          task_id: task.depends_on[dependencyIndex]!,
        });
      }
    }
  }
  return (index: number) => entries[index]!;
}

/**
 * The ledger lookup of one accepted task revision: exactly one record
 * with the exact pair, or the typed `unknown_revision` refusal before
 * any artifact is read.
 */
export function findAcceptedTaskRecord(state: PipelineV2RunState, taskId: string, revision: number): PipelineV2TaskRevisionState | null {
  for (const record of state.task_revisions) {
    if (record.task_id === taskId && record.revision === revision) {
      return record;
    }
  }
  return null;
}

/** The ledger lookup of one accepted plan revision (revision === record index). */
export function findAcceptedPlanRecord(state: PipelineV2RunState, revision: number): PipelineV2PlanRevisionState | null {
  for (const record of state.plan_revisions) {
    if (record.revision === revision) {
      return record;
    }
  }
  return null;
}

interface VerifiedRead {
  readonly runId: string;
  readonly runRoot: string;
  readonly state: PipelineV2RunState;
}

/**
 * The typed store-surface mapping of the P-1 contract: canonical/layout/
 * kind/mode/symlink and digest divergence are `artifact_modified`,
 * unexpected I/O and every non-integrity cause is `internal_error`,
 * absence stays the loader's `null`. Mapping is by typed cause only,
 * never by message text.
 */
export function storeCauseToReason(cause: unknown): PipelineV2RunPlanContentReason {
  if (cause instanceof PipelineV2RunPlanStoreError) {
    if (cause.reason === "io_failure") {
      return "internal_error";
    }
    if (cause.reason === "invalid_layout" || cause.reason === "conflict") {
      return "artifact_modified";
    }
  }
  return "internal_error";
}

async function verifiedRun(options: { runId: unknown; stateRoot: string }): Promise<VerifiedRead | PipelineV2RunPlanContentFailure> {
  const runId = options.runId;
  if (typeof runId !== "string" || !isPipelineV2SafeId(runId)) {
    return failure("", "invalid_options");
  }
  const read = await loadDurablePipelineV2Run({ runId, stateRoot: options.stateRoot });
  if (!read.ok) {
    const reason: PipelineV2RunPlanContentReason = REASONS.has(read.reason as PipelineV2RunPlanContentReason)
      ? (read.reason as PipelineV2RunPlanContentReason)
      : "internal_error";
    return failure(read.runId, reason);
  }
  return { runId: read.runId, runRoot: read.runRoot, state: read.state };
}

/**
 * Reads one bounded, byte-exact chunk of the body of one accepted task
 * revision, or returns exactly one typed failure with zero content
 * bytes. Read-only end to end: the ledger is re-read, the artifact is
 * loaded only through the production store and verified against the
 * ledger record before any chunk is sliced.
 */
export async function taskRevisionContentPipelineV2Run(
  options: PipelineV2TaskContentOptions,
): Promise<PipelineV2TaskContentOutcome> {
  const runId = options.runId;
  const stateRoot = options.stateRoot;
  const taskId = options.taskId;
  const revision = options.revision;
  const offset = options.offset ?? 0;
  const limit = options.limit ?? PIPELINE_V2_RUN_PLAN_TASK_DEFAULT_CHUNK;
  if (typeof stateRoot !== "string" || stateRoot === "") {
    return failure("", "invalid_options");
  }
  if (typeof taskId !== "string" || !isPipelineV2SafeId(taskId)) {
    return failure("", "invalid_options");
  }
  if (typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1) {
    return failure("", "invalid_options");
  }
  if (typeof offset !== "number" || !Number.isSafeInteger(offset) || offset < 0) {
    return failure("", "invalid_options");
  }
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > PIPELINE_V2_RUN_PLAN_TASK_MAX_CHUNK) {
    return failure("", "invalid_options");
  }
  try {
    const read = await verifiedRun({ runId, stateRoot });
    if ("ok" in read && read.ok === false) {
      return read;
    }
    const verified = read as VerifiedRead;
    const record = findAcceptedTaskRecord(verified.state, taskId, revision);
    if (record === null) {
      return failure(verified.runId, "unknown_revision");
    }
    let stored: Awaited<ReturnType<typeof loadPipelineV2TaskRevision>>;
    try {
      stored = await loadPipelineV2TaskRevision(verified.runRoot, taskId, revision);
    } catch (cause) {
      return failure(verified.runId, storeCauseToReason(cause));
    }
    if (stored === null) {
      return failure(verified.runId, "artifact_missing");
    }
    const prepared: PreparedPipelineV2RunTaskRevision = stored.task;
    if (prepared.sha256 !== record.sha256 || prepared.manifest.previous_sha256 !== record.previous_sha256) {
      return failure(verified.runId, "artifact_modified");
    }
    const bodyBytes = Buffer.from(prepared.manifest.body, "utf8");
    const byteCount = bodyBytes.byteLength;
    if (offset > byteCount) {
      return failure(verified.runId, "invalid_cursor");
    }
    const end = Math.min(offset + limit, byteCount);
    const returnedByteCount = end - offset;
    const nextOffset = end < byteCount ? end : null;
    const bodyDigest = stageTaskBodyDigest(bodyBytes);
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runId: verified.runId,
      taskId,
      revision,
      manifestSha256: prepared.sha256,
      previousSha256: record.previous_sha256,
      origin: prepared.manifest.origin,
      body: {
        byteCount,
        sha256: bodyDigest,
        offset,
        limit,
        returnedByteCount,
        nextOffset,
        dataBase64: returnedByteCount === 0 ? "" : bodyBytes.subarray(offset, end).toString("base64"),
      },
    }) satisfies PipelineV2TaskContentReport as PipelineV2TaskContentReport;
  } catch (cause) {
    if (cause instanceof PipelineV2RunPlanContentError) {
      return failure(typeof runId === "string" && isPipelineV2SafeId(runId) ? runId : "", cause.reason);
    }
    return failure(typeof runId === "string" && isPipelineV2SafeId(runId) ? runId : "", "internal_error");
  }
}

/**
 * Reads one bounded page of the deterministic semantic record projection
 * of one accepted plan revision, or returns exactly one typed failure
 * with zero content bytes. Read-only end to end; the full-artifact load
  * and ledger verification always precede the page computation.
 */
export async function planRevisionContentPipelineV2Run(
  options: PipelineV2PlanContentOptions,
): Promise<PipelineV2PlanContentOutcome> {
  const runId = options.runId;
  const stateRoot = options.stateRoot;
  const revision = options.revision;
  const after = options.after ?? 0;
  const limit = options.limit ?? PIPELINE_V2_RUN_PLAN_PLAN_DEFAULT_LIMIT;
  if (typeof stateRoot !== "string" || stateRoot === "") {
    return failure("", "invalid_options");
  }
  if (typeof revision !== "number" || !Number.isSafeInteger(revision) || revision < 1) {
    return failure("", "invalid_options");
  }
  if (typeof after !== "number" || !Number.isSafeInteger(after) || after < 0) {
    return failure("", "invalid_options");
  }
  if (typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > PIPELINE_V2_RUN_PLAN_PLAN_MAX_LIMIT) {
    return failure("", "invalid_options");
  }
  try {
    const read = await verifiedRun({ runId, stateRoot });
    if ("ok" in read && read.ok === false) {
      return read;
    }
    const verified = read as VerifiedRead;
    const record = findAcceptedPlanRecord(verified.state, revision);
    if (record === null) {
      return failure(verified.runId, "unknown_revision");
    }
    let stored: Awaited<ReturnType<typeof loadPipelineV2PlanRevision>>;
    try {
      stored = await loadPipelineV2PlanRevision(verified.runRoot, revision);
    } catch (cause) {
      return failure(verified.runId, storeCauseToReason(cause));
    }
    if (stored === null) {
      return failure(verified.runId, "artifact_missing");
    }
    const prepared: PreparedPipelineV2RunPlanRevision = stored.plan;
    const manifest = prepared.manifest;
    if (
      prepared.sha256 !== record.sha256 ||
      manifest.previous_sha256 !== record.previous_sha256 ||
      manifest.origin_execution !== record.origin_execution
    ) {
      return failure(verified.runId, "artifact_modified");
    }
    const total = planRecordCount(manifest);
    if (after > total) {
      return failure(verified.runId, "invalid_cursor");
    }
    const page = projectPlanRecordsPage(total, planRecordAt(manifest), after, limit);
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      schemaVersion: 1 as const,
      runId: verified.runId,
      revision,
      manifestSha256: prepared.sha256,
      previousSha256: record.previous_sha256,
      originExecution: record.origin_execution,
      after,
      records: page.records,
      nextAfter: page.nextAfter,
    }) satisfies PipelineV2PlanContentReport as PipelineV2PlanContentReport;
  } catch (cause) {
    if (cause instanceof PipelineV2RunPlanContentError) {
      return failure(typeof runId === "string" && isPipelineV2SafeId(runId) ? runId : "", cause.reason);
    }
    return failure(typeof runId === "string" && isPipelineV2SafeId(runId) ? runId : "", "internal_error");
  }
}
