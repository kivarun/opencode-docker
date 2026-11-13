import { canonicalJson } from "./canonical_json.ts";
import { deepFreezeValue } from "./pipeline_v2_freeze_internal.ts";
import {
  loadDurablePipelineV2Run,
  type PipelineV2InspectErrorReason,
} from "./pipeline_v2_inspect.ts";
import type {
  PipelineDecisionStateRecord,
  PipelineV2AgentExecutionState,
  PipelineV2DecisionExecutionState,
  PipelineV2RunState,
  PipelineV2StageGenerationRecord,
  PipelineV2StageIterationRecord,
} from "./pipeline_v2_state.ts";

/**
 * The versioned read-only durable-identity projection of one durable
 * pipeline v2 run (P-3): the stable external projection of the run's
 * durable identity, lifecycle bindings and ledgers.
 *
 * The single projection owner is this module. It reads only the
 * loader-validated schema-v12 state through the ONE shared read-only load
 * chain (`loadDurablePipelineV2Run` — there is no second loader), never
 * reconstructs order from events (events carry only `*_index` references
 * and are not a provenance source), reads no artifact (no run-plan
 * manifest, no activation output, no worker log), starts no worker, takes
 * no mutation lock and writes nothing. The projected record members are
 * exactly the durable data-plane values; every value is content-free
 * (safe ids, digests, closed-vocabulary enums, bounded integers) and no
 * path, credential, env value, prompt body, output body, log content or
 * effective configuration is ever carried — the record shapes exclude the
 * surfaced inspect/events fields (status mapping, boundary cards, run
 * outputs, transitions, commit history).
 *
 * Pagination is revision-pinned: the first page establishes the binding,
 * every continuation re-proves it against the revision of the single
 * atomic snapshot the request loaded, and a mismatch is the typed
 * `stale_snapshot` failure with zero partial response. The exclusive
 * cursor (`after`), the page limits and the deterministic stop rules are
 * fixed below; the projected stream order is the durable append-only
 * journal order, and a repeated read over an unchanged run is
 * byte-identical across restarts.
 */

/** The identity projection schema version. */
export const PIPELINE_V2_IDENTITY_SCHEMA_VERSION = 1;

/** The default page record limit. */
export const PIPELINE_V2_IDENTITY_DEFAULT_LIMIT = 100;

/** The maximum page record limit. */
export const PIPELINE_V2_IDENTITY_MAX_LIMIT = 200;

/**
 * The byte cap of ONE page of the identity projection: the complete
 * canonical serialized page document (envelope, records, cursor members
 * and every JSON delimiter) of each surface must fit within this limit.
 * Every stream record's members are bounded by the state validator's own
 * value grammar (safe ids up to 128 characters, 64-hex digests, closed
 * enums, safe integers), so each record serializes far below this cap and
 * a page never fails for a single oversized record; the cap is a
 * deterministic stop rule, not a failure reason.
 */
export const PIPELINE_V2_IDENTITY_PAGE_BYTE_LIMIT = 262144;

/** The closed identity record-kind vocabulary (twelve kinds). */
export const PIPELINE_V2_IDENTITY_RECORD_KINDS = [
  "plan_revision",
  "task_revision",
  "generation",
  "iteration",
  "grant",
  "wait",
  "wait_action",
  "execution_agent",
  "execution_decision",
  "agent_output",
  "decision_active_constraint",
  "decision_violated_relation",
] as const;

export type PipelineV2IdentityRecordKind = (typeof PIPELINE_V2_IDENTITY_RECORD_KINDS)[number];

/** One projected plan revision (the durable ledger record, verbatim fields). */
export type PipelineV2IdentityPlanRevisionRecord = {
  readonly record: "plan_revision";
  readonly index: number;
  readonly revision: number;
  readonly sha256: string;
  readonly previous_sha256: null | string;
  readonly origin_execution: number;
};

/** One projected task revision. */
export type PipelineV2IdentityTaskRevisionRecord = {
  readonly record: "task_revision";
  readonly index: number;
  readonly task_id: string;
  readonly revision: number;
  readonly sha256: string;
  readonly previous_sha256: null | string;
  /** NULL exactly when the revision carries no durable wait link. */
  readonly wait_index: null | number;
  readonly intent_sha256: null | string;
};

/** One projected stage generation. */
export type PipelineV2IdentityGenerationRecord = {
  readonly record: "generation";
  readonly index: number;
  readonly stage_id: string;
  readonly stage_position: number;
  readonly template_id: string;
  readonly plan_sha256: string;
  readonly initial_budget: number;
  readonly opened_transition_count: number;
  readonly iteration_count: number;
  /** NULL exactly when the generation has no open iteration. */
  readonly open_iteration: null | { readonly index: number; readonly opened_transition_count: number };
  readonly closed: null | { readonly by: string; readonly closed_transition_count: number };
};

/** One projected stage iteration (flattened out of its generation). */
export type PipelineV2IdentityIterationRecord = {
  readonly record: "iteration";
  readonly generation_index: number;
  readonly index: number;
  readonly opened_transition_count: number;
  /** `wait_index` is NULL exactly when the closure is not wait-bound. */
  readonly closed:
    | null
    | {
        readonly by: string;
        readonly wait_index: null | number;
        readonly closed_transition_count: number;
      };
};

/** One projected iteration grant. */
export type PipelineV2IdentityGrantRecord = {
  readonly record: "grant";
  readonly index: number;
  readonly generation_index: number;
  readonly wait_index: number;
  readonly intent_sha256: string;
  readonly additional_iterations: number;
};

/** One projected wait record. */
export type PipelineV2IdentityWaitRecord = {
  readonly record: "wait";
  readonly index: number;
  readonly transition_count: number;
  readonly state_id: string;
  readonly reason: string;
  readonly request_sha256: string;
  readonly intent_sha256: null | string;
  readonly response: null | { readonly action_id: string; readonly response_sha256: string };
};

/** One projected wait action (flattened out of its wait). */
export type PipelineV2IdentityWaitActionRecord = {
  readonly record: "wait_action";
  readonly wait_index: number;
  readonly id: string;
  readonly to: string;
};

/** One projected agent execution (scalar identity fields only). */
export type PipelineV2IdentityAgentExecutionRecord = {
  readonly record: "execution_agent";
  readonly index: number;
  readonly type: "agent";
  readonly state_id: string;
  readonly execution_role: string;
  /** NULL exactly for planning/control executions. */
  readonly iteration_index: null | number;
  readonly phase: string;
  /** The mandatory task binding; NULL exactly when durably absent. */
  readonly stage_task:
    | null
    | {
        readonly task_id: string;
        readonly task_revision: number;
        readonly task_manifest_sha256: string;
        readonly body_sha256: string;
      };
};

/** One projected decision execution (scalar fields; the id lists are flattened). */
export type PipelineV2IdentityDecisionExecutionRecord = {
  readonly record: "execution_decision";
  readonly index: number;
  readonly type: "decision";
  readonly state_id: string;
  readonly execution_role: string;
  readonly iteration_index: null | number;
  readonly phase: string;
  readonly input_digest: string;
  readonly result:
    | null
    | {
        readonly status: "selected";
        readonly outcome: string;
        readonly decision: string;
        readonly rule_id: string;
      }
    | { readonly status: "uncovered"; readonly outcome: "uncovered" }
    | { readonly status: "inconsistent_facts"; readonly outcome: "inconsistent_facts" }
    | {
        readonly status: "invalid_facts";
        readonly outcome: "invalid_facts";
        readonly reason: string;
        readonly fact_id: null | string;
        readonly actual_type: null | string;
      };
};

/** One flattened decision active-constraint id (durable order via `position`). */
export type PipelineV2IdentityDecisionActiveConstraintRecord = {
  readonly record: "decision_active_constraint";
  readonly execution_index: number;
  readonly position: number;
  readonly id: string;
};

/** One flattened decision violated-relation id (durable order via `position`). */
export type PipelineV2IdentityDecisionViolatedRelationRecord = {
  readonly record: "decision_violated_relation";
  readonly execution_index: number;
  readonly position: number;
  readonly id: string;
};

/** One flattened accepted agent output (metadata only; bytes never carried). */
export type PipelineV2IdentityAgentOutputRecord = {
  readonly record: "agent_output";
  readonly execution_index: number;
  readonly id: string;
  readonly digest: string;
};

/** The exact projection stream record union (all twelve kinds). */
export type PipelineV2IdentityRecord =
  | PipelineV2IdentityPlanRevisionRecord
  | PipelineV2IdentityTaskRevisionRecord
  | PipelineV2IdentityGenerationRecord
  | PipelineV2IdentityIterationRecord
  | PipelineV2IdentityGrantRecord
  | PipelineV2IdentityWaitRecord
  | PipelineV2IdentityWaitActionRecord
  | PipelineV2IdentityAgentExecutionRecord
  | PipelineV2IdentityDecisionExecutionRecord
  | PipelineV2IdentityAgentOutputRecord
  | PipelineV2IdentityDecisionActiveConstraintRecord
  | PipelineV2IdentityDecisionViolatedRelationRecord;

/**
 * The closed identity failure vocabulary. The load-chain reasons come
 * verbatim from the one shared read-only loader (`invalid_options` is
 * possible only before the run root is resolved); `invalid_cursor` and
 * `stale_snapshot` are the cursor/revision-contract reasons owned by this
 * module's page projector.
 */
export type PipelineV2IdentityErrorReason =
  | PipelineV2InspectErrorReason
  | "invalid_cursor"
  | "stale_snapshot";

/** The cursor/revision-contract error of the identity projection. */
export class PipelineV2IdentityError extends Error {
  readonly reason!: "invalid_cursor" | "stale_snapshot";

  constructor(reason: "invalid_cursor" | "stale_snapshot", message: string) {
    super(message);
    this.name = "PipelineV2IdentityError";
    Object.defineProperty(this, "reason", {
      value: reason,
      enumerable: true,
      writable: false,
      configurable: false,
    });
  }
}

const normalized = <T>(value: T | undefined): T | null => (value === undefined ? null : value);

const iterationClosed = (closed: PipelineV2StageIterationRecord["closed"]) =>
  closed === undefined ? null : deepFreezeValue({
    by: closed.by,
    wait_index: normalized(closed.wait_index),
    closed_transition_count: closed.closed_transition_count,
  });

const generationFrom = (generation: PipelineV2StageGenerationRecord): PipelineV2IdentityGenerationRecord =>
  deepFreezeValue({
    record: "generation" as const,
    index: generation.index,
    stage_id: generation.stage_id,
    stage_position: generation.stage_position,
    template_id: generation.template_id,
    plan_sha256: generation.plan_sha256,
    initial_budget: generation.initial_budget,
    opened_transition_count: generation.opened_transition_count,
    iteration_count: generation.iteration_count,
    open_iteration: normalized(generation.open_iteration),
    closed: normalized(generation.closed),
  });

const iterationsFrom = (generation: PipelineV2StageGenerationRecord): PipelineV2IdentityIterationRecord[] =>
  generation.iterations.map((iteration) =>
    deepFreezeValue({
      record: "iteration" as const,
      generation_index: generation.index,
      index: iteration.index,
      opened_transition_count: iteration.opened_transition_count,
      closed: iterationClosed(iteration.closed),
    }),
  );

const waitFrom = (
  wait: PipelineV2RunState["waits"][number],
): PipelineV2IdentityWaitRecord =>
  deepFreezeValue({
    record: "wait" as const,
    index: wait.index,
    transition_count: wait.transition_count,
    state_id: wait.state_id,
    reason: wait.reason,
    request_sha256: wait.request_sha256,
    intent_sha256: normalized(wait.intent?.intent_sha256),
    response: normalized(
      wait.response === undefined
        ? undefined
        : deepFreezeValue({
            action_id: wait.response.action_id,
            response_sha256: wait.response.response_sha256,
          }),
    ),
  });

type DecisionResultProjection = NonNullable<PipelineV2IdentityDecisionExecutionRecord["result"]>;

/** Projects the durable decision record's scalar members; the id lists are flattened by the stream builder. */
const decisionResultFrom = (result: PipelineDecisionStateRecord): DecisionResultProjection => {
  if (result.status === "selected") {
    return {
      status: "selected",
      outcome: result.outcome,
      decision: result.decision,
      rule_id: result.rule_id,
    };
  }
  if (result.status === "uncovered") {
    return { status: "uncovered", outcome: "uncovered" };
  }
  if (result.status === "inconsistent_facts") {
    return { status: "inconsistent_facts", outcome: "inconsistent_facts" };
  }
  return {
    status: "invalid_facts",
    outcome: "invalid_facts",
    reason: result.reason,
    fact_id: normalized(result.fact_id ?? null),
    actual_type: normalized(result.actual_type ?? null),
  };
};

/**
 * Builds the full identity record stream of one loader-validated state in
 * the fixed deterministic order: plan revisions, task revisions,
 * generations (each generation followed by its flattened iterations),
 * grants, waits (each wait followed by its flattened actions), and
 * executions (each execution followed by its flattened decision id lists
 * and, for agents, its flattened accepted outputs). Every array is the
 * durable append-only order, verbatim; nothing is sorted.
 */
export function buildPipelineV2IdentityRecords(state: PipelineV2RunState): readonly PipelineV2IdentityRecord[] {
  const records: PipelineV2IdentityRecord[] = [];
  for (const plan of state.plan_revisions) {
    records.push(
      deepFreezeValue({
        record: "plan_revision" as const,
        index: plan.index,
        revision: plan.revision,
        sha256: plan.sha256,
        previous_sha256: normalized(plan.previous_sha256),
        origin_execution: plan.origin_execution,
      }),
    );
  }
  for (const task of state.task_revisions) {
    records.push(
      deepFreezeValue({
        record: "task_revision" as const,
        index: task.index,
        task_id: task.task_id,
        revision: task.revision,
        sha256: task.sha256,
        previous_sha256: normalized(task.previous_sha256),
        wait_index: normalized(task.wait_index),
        intent_sha256: normalized(task.intent_sha256),
      }),
    );
  }
  for (const generation of state.generations) {
    records.push(generationFrom(generation));
    records.push(...iterationsFrom(generation));
  }
  for (const grant of state.grants) {
    records.push(
      deepFreezeValue({
        record: "grant" as const,
        index: grant.index,
        generation_index: grant.generation_index,
        wait_index: grant.wait_index,
        intent_sha256: grant.intent_sha256,
        additional_iterations: grant.additional_iterations,
      }),
    );
  }
  for (const wait of state.waits) {
    records.push(waitFrom(wait));
    for (const action of wait.actions) {
      records.push(deepFreezeValue({ record: "wait_action" as const, wait_index: wait.index, id: action.id, to: action.to }));
    }
  }
  for (const execution of state.executions) {
    if (execution.type === "agent") {
      const agent = execution as PipelineV2AgentExecutionState;
      records.push(
        deepFreezeValue({
          record: "execution_agent" as const,
          index: agent.index,
          type: "agent" as const,
          state_id: agent.state_id,
          execution_role: agent.execution_role,
          iteration_index: normalized(agent.iteration_index),
          phase: agent.phase,
          stage_task: normalized(
            agent.stage_task === undefined
              ? undefined
              : deepFreezeValue({
                  task_id: agent.stage_task.task_id,
                  task_revision: agent.stage_task.task_revision,
                  task_manifest_sha256: agent.stage_task.task_manifest_sha256,
                  body_sha256: agent.stage_task.body_sha256,
                }),
          ),
        }),
      );
      for (const output of agent.outputs ?? []) {
        records.push(
          deepFreezeValue({
            record: "agent_output" as const,
            execution_index: agent.index,
            id: output.id,
            digest: output.digest,
          }),
        );
      }
      continue;
    }
    const decision = execution as PipelineV2DecisionExecutionState;
    records.push(
      deepFreezeValue({
        record: "execution_decision" as const,
        index: decision.index,
        type: "decision" as const,
        state_id: decision.state_id,
        execution_role: decision.execution_role,
        iteration_index: normalized(decision.iteration_index),
        phase: decision.phase,
        input_digest: decision.input_digest,
        result: normalized(decision.result === undefined ? undefined : decisionResultFrom(decision.result)),
      }),
    );
    if (decision.result === undefined) {
      continue;
    }
    if (decision.result.status === "invalid_facts") {
      continue;
    }
    const idList: { readonly record: "decision_active_constraint" | "decision_violated_relation"; readonly ids: readonly string[] } =
      decision.result.status === "inconsistent_facts"
        ? { record: "decision_violated_relation", ids: decision.result.violated_relation_ids }
        : { record: "decision_active_constraint", ids: decision.result.active_constraint_ids };
    let position = 0;
    for (const id of idList.ids) {
      records.push(
        deepFreezeValue({
          record: idList.record,
          execution_index: decision.index,
          position,
          id,
        }),
      );
      position += 1;
    }
  }
  return deepFreezeValue(records);
}

/** The request of one identity page (singular values; the surfaces own the grammar). */
export interface PipelineV2IdentityPageRequest {
  /** The exclusive cursor (default 0). */
  readonly after?: number;
  /** The revision pin; REQUIRED for every continuation. Optional at `after = 0`. */
  readonly revision?: number;
  /** The page record limit (default 100, maximum 200). */
  readonly limit?: number;
}

/** The exact deep-frozen page: the snapshot revision, the records, the cursor. */
export interface PipelineV2IdentityPage {
  /** The revision of the single loader-validated snapshot the page was read from. */
  readonly revision: number;
  /** The exclusive cursor this page starts at. */
  readonly after: number;
  readonly records: readonly PipelineV2IdentityRecord[];
  /** `after + records.length` exactly when a tail exists; `null` otherwise. */
  readonly nextAfter: number | null;
}

const isPageRequestInteger = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value);

/**
 * Projects one identity page from a loader-validated state with the exact
 * cursor/revision contract: the grammar is validated fail-closed, the
 * revision pin is checked BEFORE the cursor-range checks (any provided
 * revision — including on the first page — must equal the loaded
 * snapshot's revision; a mismatch or a continuation without its revision
 * binding is a typed refusal with zero partial response), `after` beyond
 * the stream is the typed `invalid_cursor` refusal, and the page honors
 * both stop rules (record cap, full-document byte cap on the larger CLI
 * envelope) with the deterministic stop-before partition.
 */
export function projectPipelineV2IdentityPage(
  state: PipelineV2RunState,
  request: PipelineV2IdentityPageRequest,
): PipelineV2IdentityPage {
  if (request.after !== undefined && (typeof request.after !== "number" || !Number.isSafeInteger(request.after) || request.after < 0)) {
    throw new PipelineV2IdentityError("invalid_cursor", "the identity cursor must be a non-negative safe integer");
  }
  if (
    request.limit !== undefined &&
    (typeof request.limit !== "number" || !Number.isSafeInteger(request.limit) || request.limit < 1 || request.limit > PIPELINE_V2_IDENTITY_MAX_LIMIT)
  ) {
    throw new PipelineV2IdentityError("invalid_cursor", `the identity limit must be a positive safe integer up to ${PIPELINE_V2_IDENTITY_MAX_LIMIT}`);
  }
  if (request.revision !== undefined && request.revision !== null) {
    if (typeof request.revision !== "number" || !Number.isSafeInteger(request.revision) || request.revision < 1) {
      throw new PipelineV2IdentityError("invalid_cursor", "the identity revision pin must be a positive safe integer");
    }
    if (request.revision !== state.revision) {
      throw new PipelineV2IdentityError(
        "stale_snapshot",
        "the identity revision pin does not match the loaded durable snapshot; restart the stream from the first page",
      );
    }
  }
  const after = request.after ?? 0;
  const limit = request.limit ?? PIPELINE_V2_IDENTITY_DEFAULT_LIMIT;
  const records = buildPipelineV2IdentityRecords(state);
  if (after > records.length) {
    throw new PipelineV2IdentityError("invalid_cursor", "the identity cursor is beyond the projected identity stream");
  }
  if (after === records.length) {
    return deepFreezeValue({ revision: state.revision, after, records: [], nextAfter: null });
  }
  const page: PipelineV2IdentityRecord[] = [];
  for (const record of records.slice(after)) {
    // The partition is computed against the larger CLI envelope (which is
    // always at least as long as the HTTP envelope for the same records —
    // permanently proven by the transport tests): the probe envelope uses
    // the maximum-bound run id, so the candidate size is an upper bound of
    // both real surfaces at every step. The stop-before rule closes the
    // page BEFORE the record that would push the fully serialized document
    // over the cap; the bounded record grammar makes an oversized first
    // record unproducible, so a page always makes progress.
    if (page.length >= 1) {
      const candidate = `${identityCliEnvelopeJson(state.revision, after, [...page, record], after + page.length + 1)}\n`;
      if (Buffer.byteLength(candidate, "utf8") > PIPELINE_V2_IDENTITY_PAGE_BYTE_LIMIT) {
        break;
      }
    }
    page.push(record);
    if (page.length >= limit) {
      break;
    }
  }
  const nextAfter = after + page.length;
  return deepFreezeValue({
    revision: state.revision,
    after,
    records: page,
    nextAfter: nextAfter >= records.length ? null : nextAfter,
  });
}

/** The canonical CLI envelope JSON for one candidate page (the partition probe). */
function identityCliEnvelopeJson(
  revision: number,
  after: number,
  records: readonly PipelineV2IdentityRecord[],
  nextAfter: number | null,
): string {
  return canonicalJson({
    ok: true,
    exitCode: 0,
    schemaVersion: PIPELINE_V2_IDENTITY_SCHEMA_VERSION,
    runId: "x".repeat(128),
    revision,
    after,
    records,
    nextAfter,
  });
}

/** The identity success document (exact own keys; path-free — no runRoot). */
export interface PipelineV2IdentitySuccess {
  readonly ok: true;
  readonly exitCode: 0;
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly revision: number;
  readonly after: number;
  readonly records: readonly PipelineV2IdentityRecord[];
  readonly nextAfter: number | null;
}

/** The identity failure document (exact own keys; exitCode always 1; path-free). */
export interface PipelineV2IdentityFailure {
  readonly ok: false;
  readonly exitCode: 1;
  readonly runId: string;
  readonly reason: PipelineV2IdentityErrorReason;
}

/** The identity result union: one page document or exactly one typed failure. */
export type PipelineV2IdentityOutcome = PipelineV2IdentitySuccess | PipelineV2IdentityFailure;

export interface PipelineV2IdentityOptions {
  /** The run id (the shared safe-id grammar; never echoed when invalid). */
  readonly runId: string;
  /** The local orchestrator state root (absolute; read-only access). */
  readonly stateRoot: string;
  /** The exclusive momentary cursor (default 0; REQUIRED with it: the revision pin). */
  readonly after?: number;
  /** The revision pin (REQUIRED for every continuation `after > 0`). */
  readonly revision?: number;
  /** The page limit (default 100, maximum 200). */
  readonly limit?: number;
}

const identityFailure = (
  runId: string,
  reason: PipelineV2IdentityErrorReason,
): PipelineV2IdentityFailure => deepFreezeValue({ ok: false as const, exitCode: 1 as const, runId, reason });

/**
 * Reads one page of the durable identity projection of one pipeline v2
 * run through the one shared read-only load chain. There is no mutation
 * lock, no Docker Helper involvement, no durable change and no filesystem
 * write on any path; no worker, session, helper or Launcher call is ever
 * started; the run tree is byte-identical before and after the call and a
 * repeated read over an unchanged run is byte-identical.
 */
export async function identityPipelineV2Run(
  options: PipelineV2IdentityOptions,
): Promise<PipelineV2IdentityOutcome> {
  const runId = options.runId;
  if (
    afterWithoutRevision(options.after, options.revision) !== undefined
  ) {
    return identityFailure(typeof runId === "string" ? runId : "", "invalid_cursor");
  }
  const read = await loadDurablePipelineV2Run(options as { runId: unknown; stateRoot: string });
  if (!read.ok) {
    return identityFailure(read.runId, read.reason);
  }
  const { runId: safeRunId, state } = read;
  try {
    const page = projectPipelineV2IdentityPage(state, {
      after: options.after,
      revision: options.revision,
      limit: options.limit,
    });
    return deepFreezeValue({
      ok: true as const,
      exitCode: 0 as const,
      // The constant is declared as the literal 1, so this stays the exact
      // literal type the versioned document contract requires.
      schemaVersion: 1 as const,
      runId: safeRunId,
      revision: page.revision,
      after: page.after,
      records: page.records,
      nextAfter: page.nextAfter,
    });
  } catch (cause) {
    if (cause instanceof PipelineV2IdentityError) {
      return identityFailure(safeRunId, cause.reason);
    }
    return identityFailure(safeRunId, "internal_error");
  }
}

/**
 * The cross-field grammar: every continuation (`after > 0`) must carry its
 * revision pin. A violation is the typed cursor refusal, regardless of the
 * loaded snapshot (no filesystem effect happens before this check).
 */
function afterWithoutRevision(
  after: number | undefined,
  revision: number | undefined,
): "invalid_cursor" | undefined {
  if ((after ?? 0) > 0 && revision === undefined) {
    return "invalid_cursor";
  }
  return undefined;
}
