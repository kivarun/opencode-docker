import type { AgentSmokeOptions } from "./agent_smoke.ts";
import type { RunInputBinding } from "./pipeline_v2_runtime.ts";
import { expectSafeId } from "./pipeline_v2_state.ts";
import { isCleanAbsolutePath } from "./clean_path.ts";
import { DEFAULT_WORKER_IMAGE } from "./worker.ts";

export const DEFAULT_PIPELINE_ROOT = "/opt/orchestrator/pipelines/default";

/** The bundled pipeline v2 default selected by `orchestrator run` when no
 * `--pipeline-root` is given. The v1 `DEFAULT_PIPELINE_ROOT` stays the
 * `agent-smoke` default; the two defaults never cross. */
export const DEFAULT_PIPELINE_V2_ROOT = "/opt/orchestrator/pipelines/default-v2";

export interface ParsedSmokeArgs {
  kind: "smoke";
  workspace: string;
  workerImage: string;
  launcherId?: string;
}

export interface ParsedAgentSmokeArgs {
  kind: "agent-smoke";
  workspace: string;
  configRoot: string;
  pipelineRoot: string;
  launcherId?: string;
}

export interface ParsedPipelineRunArgs {
  readonly kind: "run";
  readonly pipelineRoot: string;
  readonly configRoot: string;
  readonly projectSourcePath: string;
  readonly inputBindings: readonly RunInputBinding[];
  readonly runId?: string;
  readonly launcherId?: string;
  readonly json: boolean;
}

export interface ParsedPipelineResumeArgs {
  readonly kind: "resume";
  readonly runId: string;
  readonly configRoot: string;
  readonly launcherId?: string;
  readonly json: boolean;
}

export interface ParsedPipelineRespondArgs {
  readonly kind: "respond";
  readonly runId: string;
  readonly waitIndex: number;
  readonly actionId: string;
  readonly json: boolean;
}

export interface ParsedPipelineContinueStageArgs {
  readonly kind: "continue-stage";
  readonly runId: string;
  readonly waitIndex: number;
  readonly additionalIterations: number;
  readonly configRoot: string;
  readonly launcherId?: string;
  readonly json: boolean;
}

export interface ParsedPipelineReviseTaskArgs {
  readonly kind: "revise-task";
  readonly runId: string;
  readonly waitIndex: number;
  readonly taskId: string;
  readonly taskFile: string;
  readonly configRoot: string;
  readonly launcherId?: string;
  readonly json: boolean;
}

export interface ParsedPipelineResumePlanArgs {
  readonly kind: "resume-plan";
  readonly runId: string;
  readonly stageId: string;
  readonly initialBudget: number;
  readonly configRoot: string;
  readonly launcherId?: string;
  readonly json: boolean;
}

export interface ParsedPipelineInspectArgs {
  readonly kind: "inspect";
  readonly runId: string;
  readonly configRoot: string;
  readonly json: boolean;
}

export interface ParsedPipelineLogsArgs {
  readonly kind: "logs";
  readonly runId: string;
  readonly configRoot: string;
  readonly executionIndex: number | null;
  readonly offset: number;
  readonly limit: number;
  readonly json: boolean;
}

export interface ParsedPipelineEventsArgs {
  readonly kind: "events";
  readonly runId: string;
  readonly configRoot: string;
  readonly after: number;
  readonly limit: number;
  readonly json: boolean;
}

export interface ParsedPipelineProjectChangesArgs {
  readonly kind: "project-changes";
  readonly runId: string;
  readonly configRoot: string;
  readonly project: string;
  readonly after: number;
  readonly limit: number;
  readonly observation: string | null;
  readonly json: boolean;
}

export interface ParsedPipelineProjectChangeContentArgs {
  readonly kind: "project-change-content";
  readonly runId: string;
  readonly configRoot: string;
  readonly project: string;
  readonly changeIndex: number;
  readonly side: "baseline" | "result";
  readonly observation: string;
  readonly offset: number;
  readonly limit: number;
  readonly json: boolean;
}

export type ParsedCommand =
  | ParsedSmokeArgs
  | ParsedAgentSmokeArgs
  | ParsedPipelineRunArgs
  | ParsedPipelineResumeArgs
  | ParsedPipelineRespondArgs
  | ParsedPipelineContinueStageArgs
  | ParsedPipelineReviseTaskArgs
  | ParsedPipelineResumePlanArgs
  | ParsedPipelineInspectArgs
  | ParsedPipelineEventsArgs
  | ParsedPipelineLogsArgs
  | ParsedPipelineProjectChangesArgs
  | ParsedPipelineProjectChangeContentArgs
  | ParsedPipelineProjectApplyArgs
  | ParsedPipelineProjectApplyInspectArgs;

export function usage(): string {
  return [
    "usage: orchestrator <command> [flags]",
    "",
    "commands:",
    "  smoke        one-shot launcher-delegation smoke test:",
    "               launcher credential -> child session -> worker container ->",
    "               workspace artifact -> verified cleanup",
    "  agent-smoke  OpenCode agent smoke test driven by a declarative pipeline:",
    "               pipeline -> multi-state plan -> profile and input resolution ->",
    "               launcher credential -> child session per agent-state activation ->",
    "               OpenCode agent -> work product -> validated result.json ->",
    "               unchanged protected input -> outcome transition -> verified cleanup",
    "  run          production pipeline v2 execution:",
    "               pipeline bundle -> profiles -> launcher credential -> run root ->",
    "               durable state -> agent/decision activations -> decision routing ->",
    "               terminal -> published run outputs",
    "  resume       production pipeline v2 continuation and recovery of an already",
    "               durable run: existing run root -> durable state -> restored",
    "               runtime context -> agent/decision activations -> terminal ->",
    "               published run outputs (mutating; also completes the durable",
    "               post-commit recovery suffixes of a crashed run)",
    "  inspect      read-only versioned projection of one durable run: no worker,",
    "               no Launcher credential, no Docker Helper call and no durable",
    "               change - prints the run status, its boundary (the one safe",
    "               next mutating command) and the stable run-output projection",
    "  events       read-only versioned lifecycle events of one durable run: the",
    "               schema-v8 commit history projected as public event schema 1",
    "               with an exclusive revision cursor - no worker, no Launcher",
    "               credential, no mutation lock and no durable change",
    "  logs         read-only versioned worker logs of one durable run: the",
    "               schema-v9 bindings of the agent executions projected as",
    "               public logs schema 1 with bounded base64 content chunks -",
    "               no worker, no Launcher credential, no mutation lock and",
    "               no durable change",
    "  project-changes read-only versioned run workspace changes of one finalized",
    "               durable run relative to its durable project provenance",
    "               baseline and the observed current operator project: an",
    "               observation with an ephemeral read snapshot identity -",
    "               no worker, no Launcher credential, no mutation lock, no",
    "               durable change and no write to the operator project",
  "  project-change-content read-only bounded raw-byte content of one side",
  "               of one verified change entry: bound to the recomputed",
  "               observation identity, served through one single-pass",
  "               verified stream - no worker, no Launcher credential, no",
  "               mutation lock, no durable change and no write anywhere",
  "  project-apply explicit Project Apply of one finalized run's reviewed",
  "               result onto the registered operator project: caller-owned",
  "               apply id, exact observation binding, durable per-path",
  "               progress - never automatic, starts no worker and no",
  "               Docker Helper call",
  "  project-apply-inspect read-only projection of one durable Apply",
  "               operation (status, revision, step counts): never runs",
  "               recovery and never changes any status",
    "  respond      production pipeline v2 wait response: record the user's answer to",
    "               one durably open wait (request manifest -> response manifest ->",
    "               durable wait_response_recorded); the pipeline is NOT continued -",
    "               continuation is the separate 'orchestrator resume' command",
  "  continue-stage production pipeline v2 stage intervention: grant extra stage",
  "               iterations and resume the run in one operation (the exact",
  "               'continue_stage' intervention: intent -> grant -> iteration",
  "               closure -> response -> reopened iteration -> immediate resume)",
  "  revise-task  production pipeline v2 task revision intervention: replace one",
  "               stage task with a new body and resume the run in one operation",
  "               (the exact 'revise_task' intervention: intent -> task revision",
  "               -> replanned iteration closure -> response -> planning",
  "               execution -> new plan revision)",
  "  resume-plan  production pipeline v2 planning continuation: accept the settled",
  "               planning output as the next plan revision, hand off onto the",
  "               explicitly selected stage and resume the run in one operation",
  "               (serves the initial plan-ready boundary of a fresh run and",
  "               the replanned boundary after a revise cycle; stage id and",
  "               initial budget are explicit operator policy; there is no",
  "               automatic planning loop)",
  "",
    "common flags:",
    "  --workspace PATH        workspace passed to 'docker-helper session create'; must exist",
    "                          and be visible to the orchestrator at the same absolute path",
    "                          (default: ${SMOKE_WORKSPACE:-/workspace})",
    "  --launcher-id DHL_ID    fail unless the installed credential belongs to this launcher",
    "",
    "smoke flags:",
    `  --image WORKER_IMAGE    worker container image (default: ${DEFAULT_WORKER_IMAGE})`,
    "",
    "run flags (production pipeline v2):",
    "  [--pipeline-root ABS]   optional pipeline bundle root with schema_version 2; an",
    "                          explicitly given value must be an absolute path. Without",
    `                          the flag 'run' uses the bundled pipeline v2 default`,
    `                          (${DEFAULT_PIPELINE_V2_ROOT}): the planner writes the`,
    "                          operator-visible run plan proposal (stage-1, template",
    "                          development, new task task-1 from the protected task",
    "                          input). The bundled default carries the trusted",
    "                          'plan_ready' auto policy, so one 'run' command can",
    "                          continue through plan acceptance, the first stage",
    "                          generation/iteration and the successor execution",
    "                          automatically; the default stage publishes the",
    "                          V2-native result at <run-root>/outputs/result. A bundle",
    "                          without that policy stops with the planReady outcome",
    "                          and its next manual step is 'orchestrator resume-plan",
    "                          --stage-id stage-1 --initial-budget 1' (the manual",
    "                          planning path).",
    "  [--run-id SAFE_ID]      optional caller-chosen run identifier (letters, digits, '_',",
    "                          '.', '-', at most 128 characters). The caller is responsible",
    "                          for durably storing it BEFORE the call: when the result is",
    "                          lost, 'orchestrator inspect --run-id R' reports the exact",
    "                          boundary and the existing resume/intervention commands take",
    "                          over. A repeated 'run' with the same id is not a retry or a",
    "                          resume: the run root is claimed exclusively and an already",
    "                          existing object fails the call (use 'inspect', never a second",
    "                          create). Without the flag the generated run id is unchanged.",
    "  --config-root PATH      operator-controlled configuration root holding",
    "                          profiles/<name>.yaml and the OpenCode configurations; required;",
    "                          absolute path",
    "  --project PATH          project source directory; required; absolute path. The runner",
    "                          copies it once into the run-owned directory",
    "                          <state-root>/pipeline-runs/<run-id>/project; every activation",
    "                          mounts that copy read-write at /workspace and worker changes",
    "                          persist there across activations. The source directory is",
    "                          never modified, and its path never reaches the worker.",
    "  --input ID=PATH         bind one declared pipeline run input; repeatable, declaration",
    "                          order is preserved. ID is a safe identifier (letters, digits,",
    "                          '_', '.', '-', at most 128 characters) and PATH an absolute",
    "                          host path; the pair is split at the first '=', so '=' inside",
    "                          PATH is allowed. Omitting --input entirely is valid when the",
    "                          pipeline declares no run inputs; the trusted pipeline loader",
    "                          and the data plane verify that every declared input is bound",
    "                          exactly once and reject missing, duplicate or unknown ids.",
    "  --json                  print exactly one JSON PipelineV2RunOutcome document on stdout",
    "                          (no progress lines); worker and image-pull output is forwarded",
    "                          to stderr; the exit code is the outcome's exit code.",
    "",
    "respond flags (production pipeline v2 wait response):",
    "  --run-id SAFE_ID        the run id of the durably waiting run; required; a safe",
    "                          identifier (letters, digits, '_', '.', '-', at most 128",
    "                          characters)",
    "  --wait-index N          the wait journal index of the open wait; required; a",
    "                          positive decimal integer without sign, leading zeros,",
    "                          fraction, exponent or whitespace",
    "  --action SAFE_ID        one action id declared by that wait's request; required.",
    "                          The routing target is never caller-supplied: it is",
    "                          always the request's own declaration for that action.",
    "  --json                  print exactly one JSON PipelineV2WaitResponseOutcome",
    "                          document on stdout; the exit code is the outcome's",
    "                          exit code.",
    "",
  "  respond records the answer durably (state waiting -> active at the declared",
  "  target) and never continues the pipeline: run 'orchestrator resume' to",
  "  continue. respond needs no config-root, no Launcher credential and no Docker",
  "  Helper configuration; it accepts no fresh-run or resume flags, no target",
  "  state, no digests and no JSON body. Repeating the same answer is an",
  "  idempotent success; repeating with a different action is a conflict.",
  "  The special intervention action ids ('continue_stage', 'revise_task') are",
  "  handled by dedicated commands and the generic respond fail-closes on them",
  "  (exit 1, reason invalid_state) before any response publication: the",
  "  'continue_stage' intervention runs via 'orchestrator continue-stage' and",
  "  the 'revise_task' intervention via 'orchestrator revise-task'. The durable",
  "  state stays untouched by the fail-closed rejection.",
    "",
    "resume flags (production pipeline v2 continuation):",
    "  --run-id SAFE_ID        the run id of the already durable run; required; a safe",
    "                          identifier (letters, digits, '_', '.', '-', at most 128",
    "                          characters). The pipeline, the project copy, the input",
    "                          snapshot and the accepted outputs come only from the",
    "                          durable state and the run-owned layout.",
    "  --config-root PATH      operator-controlled configuration root holding",
    "                          profiles/<name>.yaml and the OpenCode configurations;",
    "                          required; absolute path. Profiles are trusted operator",
    "                          configuration and are loaded again at resume; the pipeline",
    "                          execution identity is verified by the durable digest.",
    "  --launcher-id DHL_ID    fail unless the installed credential belongs to this launcher",
    "  --json                  print exactly one JSON PipelineV2RunOutcome document on stdout",
    "                          (no progress lines); worker and image-pull output is forwarded",
    "                          to stderr; the exit code is the outcome's exit code.",
    "",
    "  resume continues only a clean active run (status active, phase running, every",
    "  execution settled and bound to its committed transition, the last wait already",
    "  answered). Waiting, in-flight, publishing, terminal and final states are refused",
    "  without any durable write, Session or callback; the run root and state.json are",
    "  never modified by a refusal. resume accepts no --pipeline-root, --project,",
    "  --input, --workspace, --image, --profile or state-root flags: the pipeline comes",
    "  only from the durable state and the data only from the run-owned layout.",
    "",
    "continue-stage flags (production pipeline v2 stage intervention):",
    "  --run-id SAFE_ID        the run id of the durably waiting run; required; a safe",
    "                          identifier (letters, digits, '_', '.', '-', at most 128",
    "                          characters)",
    "  --wait-index N          the wait journal index of the open 'continue_stage' wait;",
    "                          required; a positive decimal integer without sign,",
    "                          leading zeros, fraction, exponent or whitespace",
    "  --additional-iterations N the number of additional stage iterations the",
    "                          intervention grants; required; no default; the same",
    "                          positive decimal integer grammar as --wait-index",
    "  --config-root PATH      operator-controlled configuration root holding",
    "                          profiles/<name>.yaml and the OpenCode configurations;",
    "                          required; absolute path. Profiles are trusted operator",
    "                          configuration and are loaded again by the intervention;",
    "                          the pipeline execution identity is verified by the",
    "                          durable digest.",
    "  --launcher-id DHL_ID    fail unless the installed credential belongs to this launcher",
    "  --json                  print exactly one JSON PipelineV2RunOutcome document on stdout",
    "                          (no progress lines); worker and image-pull output is forwarded",
    "                          to stderr; the exit code is the outcome's exit code.",
    "",
  "  continue-stage answers one open 'continue_stage' wait and immediately resumes",
  "  the run as one operation: it accepts the user's intent, grants the requested",
  "  additional stage iterations, closes the interrupted iteration, records the",
  "  response, reopens the next iteration and continues with the successor stage",
  "  execution. The action is fixed as 'continue_stage' (the command accepts no",
  "  --action flag) and never accepts a target state, a stage id, a plan digest,",
  "  the initial budget, a prepared intent or a compiled plan - every internal",
  "  intervention parameter is derived from the authoritative durable state. It",
  "  accepts no fresh-run flags and no state-root flags.",
  "",
  "revise-task flags (production pipeline v2 task revision intervention):",
  "  --run-id SAFE_ID        the run id of the durably waiting run; required; a safe",
  "                          identifier (letters, digits, '_', '.', '-', at most 128",
  "                          characters)",
  "  --wait-index N          the wait journal index of the open 'revise_task' wait;",
  "                          required; a positive decimal integer without sign,",
  "                          leading zeros, fraction, exponent or whitespace",
  "  --task-id SAFE_ID       the task id to revise; required; a safe identifier.",
  "                          The current task revision is derived from the durable",
  "                          plan, never from the ledger tail.",
  "  --task-file PATH        a file whose UTF-8 content is the revised task body;",
  "                          required; absolute path. The body is never passed",
  "                          inline or through argv (there is no --task-body flag):",
  "                          the command reads the file exactly once as UTF-8 text",
  "                          without trimming, normalizing line endings or adding a",
  "                          trailing newline; an empty file is a CLI contract error",
  "                          and the runner is never invoked. The file path and the",
  "                          body never appear in the outcome, the durable state or",
  "                          any diagnostic.",
  "  --config-root PATH      operator-controlled configuration root holding",
  "                          profiles/<name>.yaml and the OpenCode configurations;",
  "                          required; absolute path. Profiles are trusted operator",
  "                          configuration and are loaded again by the intervention;",
  "                          the pipeline execution identity is verified by the",
  "                          durable digest.",
  "  --launcher-id DHL_ID    fail unless the installed credential belongs to this launcher",
  "  --json                  print exactly one JSON PipelineV2RunOutcome document on stdout",
  "                          (no progress lines); worker and image-pull output is forwarded",
  "                          to stderr; the exit code is the outcome's exit code.",
  "",
  "  revise-task answers one open 'revise_task' wait and resumes the run as one",
  "  operation: it accepts the user's intent, accepts the revised task body as a",
  "  new durable task revision, closes the interrupted iteration as replanned,",
  "  records the response and continues with the architect planning execution and",
  "  the next plan revision. The action is fixed as 'revise_task' (the command",
  "  accepts no --action flag) and never accepts a target state, a stage id, a",
  "  plan digest, the initial budget, a prepared intent, a candidate task",
  "  revision or a compiled plan - every internal intervention parameter is",
  "  derived from the authoritative durable state. It accepts no fresh-run flags",
  "  and no state-root flags.",
  "",
  "resume-plan flags (production pipeline v2 planning continuation):",
  "  --run-id SAFE_ID        the run id of the durable run sitting at its settled",
  "                          planning boundary; required; a safe identifier",
  "                          (letters, digits, '_', '.', '-', at most 128 characters)",
  "  --stage-id SAFE_ID      the plan stage to hand the run onto; required; explicit",
  "                          operator policy of this command (never derived from the",
  "                          durable state); a safe identifier",
  "  --initial-budget N      the initial iteration budget of the opened stage",
  "                          generation; required; explicit operator policy of this",
  "                          command (never derived from the durable state); a",
  "                          positive decimal integer without sign, leading zeros,",
  "                          fraction, exponent or whitespace; no default",
  "  --config-root PATH      operator-controlled configuration root holding",
  "                          profiles/<name>.yaml and the OpenCode configurations;",
  "                          required; absolute path. Profiles are trusted operator",
  "                          configuration and are loaded again by the continuation;",
  "                          the pipeline execution identity is verified by the",
  "                          durable digest.",
  "  --launcher-id DHL_ID    fail unless the installed credential belongs to this launcher",
  "  --json                  print exactly one JSON PipelineV2RunOutcome document on stdout",
  "                          (no progress lines); worker and image-pull output is forwarded",
  "                          to stderr; the exit code is the outcome's exit code.",
  "",
  "  resume-plan accepts the settled planning output as the next durable plan",
  "  revision and hands the run onto the explicitly selected stage in one",
  "  operation. It serves both planning boundaries: the initial plan-ready",
  "  boundary of a fresh run (plan revision -> first generation and iteration",
  "  1 opened -> planning transition committed) and the replanned boundary",
  "  after a revise cycle (task revisions -> plan revision -> old generation",
  "  closed (replanned) -> new generation and iteration 1 opened -> planning",
  "  transition committed); both end in the successor stage execution. The",
  "  command is not a respond action and accepts no --action flag; it accepts",
  "  no --wait-index, no --additional-iterations, no --task-id/--task-file/",
  "  --task-body, no fresh-run flag and no state-root flag; every internal",
  "  handoff parameter (the plan acceptance, the stage opening, the committed",
  "  planning transition and the resumed execution) is derived from the",
  "  authoritative durable state by the composition controller. There is no",
  "  automatic planning loop and no automatic stage/budget selection: both",
  "  policy scalars are explicit operator decisions of each invocation.",
  "",
  "inspect flags (production pipeline v2, read-only):",
  "  --run-id SAFE_ID        the run id of the durable run to inspect; required; a",
  "                          safe identifier (letters, digits, '_', '.', '-', at most",
  "                          128 characters)",
  "  --config-root PATH      non-empty absolute configuration root; required by the",
  "                          shared command grammar but not used by inspect (no",
  "                          profile is loaded, no Launcher credential is read)",
  "  --json                  print exactly one JSON versioned inspect document on",
  "                          stdout (no progress lines); errors and diagnostics go",
  "                          to stderr; the exit code is 1 only when the inspect",
  "                          itself could not be performed, never because of the",
  "                          run's own state.",
  "",
  "  inspect reads the durable state through the read-only snapshot store and",
  "  never mutates anything: a missing run is not created, a damaged run is not",
  "  repaired, and a repeated inspect over an unchanged run returns byte-identical",
  "  JSON. The document is the versioned external projection (schemaVersion 1) -",
  "  status, boundary and run outputs - not the internal durable state document,",
  "  which is not an external integration contract. The boundary names the one",
  "  safe next mutating command: 'plan_ready' (continuation 'resume' when the",
  "  trusted compiled planning role carries the plan_ready auto policy,",
  "  'resume_plan' for the manual 'resume-plan' contract), 'waiting' (with the",
  "  open wait's declared actions in their verbatim order), 'resumable' (the",
  "  ordinary runtime boundary and every durable post-commit recovery window -",
  "  always continued by 'orchestrator resume'), 'in_flight' (a genuinely",
  "  unfinished execution; not recoverable yet), and the final 'succeeded'/'failed'",
  "  boundaries. A finished failed run inspects successfully (exit 0, status",
  "  'failed'). Until the terminal publication the outputs list is empty; the",
  "  projection carries id, type, required, present and - only for present",
  "  outputs - the digest, in declaration order, never output content or",
  "  filesystem paths. Concurrent mutating commands on one run are serialized",
  "  per process by a kernel-owned run mutation lock: a refused caller fails",
  "  fast with reason 'run_busy' and zero durable effects. list-runs,",
  "  event/log streams and idempotency keys are not part of this command.",
  "",
  "events flags (production pipeline v2, read-only):",
  "  --run-id SAFE_ID        the run id of the durable run to read; required; a",
  "                          safe identifier (letters, digits, '_', '.', '-', at most",
  "                          128 characters)",
  "  --config-root PATH      non-empty absolute configuration root; required by the",
  "                          shared command grammar but not used by events (no",
  "                          profile is loaded, no Launcher credential is read)",
  "  --after N               exclusive durable revision cursor (default 0): an event",
  "                          is returned only when its commit revision is greater",
  "                          than N; a cursor beyond the observed revision fails with",
  "                          reason 'invalid_cursor'",
  "  --limit N               page limit (default 100, maximum 200), applied after",
  "                          the exclusive filter; next-after carries the last",
  "                          returned revision only when further events exist",
  "  --json                  print exactly one JSON versioned events document on",
  "                          stdout (no progress lines); errors and diagnostics go",
  "                          to stderr; the exit code is 1 only when the events read",
  "                          itself could not be performed, never because of the",
  "                          run's own state.",
  "",
  "  events project the durable schema-v8 commit history - the reducer-owned",
  "  ordered provenance of every applied command - as the versioned public",
  "  event schema 1: one entry per revision with its closed kind and the",
  "  minimal content-free references. The cursor is an exclusive durable",
  "  revision cursor, never an array offset and never process-local state;",
  "  events reads one atomic state snapshot, takes no mutation lock, never",
  "  writes, and a repeated read over an unchanged run is byte-identical.",
  "  Worker stdout/stderr and durable log capture are not part of this",
  "  command.",
  "",
  "logs flags (production pipeline v2, read-only):",
  "  --run-id SAFE_ID        the run id of the durable run to read; required; a",
  "                          safe identifier (letters, digits, '_', '.', '-', at most",
  "                          128 characters)",
  "  --config-root PATH      non-empty absolute configuration root; required by the",
  "                          shared command grammar but not used by logs (no",
  "                          profile is loaded, no Launcher credential is read)",
  "  [--execution-index N]   read one agent execution's worker log instead of the",
  "                          metadata list; a positive decimal integer without sign,",
  "                          leading zeros, fraction, exponent or whitespace",
  "  [--offset N]            raw payload byte offset (default 0); requires",
  "                          --execution-index; offsets are raw bytes, never UTF-8",
  "                          code points and never base64 characters",
  "  [--limit N]             maximum raw-byte chunk size (default 65536, maximum",
  "                          262144); requires --execution-index",
  "  --json                  print exactly one JSON versioned logs document on",
  "                          stdout (no progress lines); errors and diagnostics go",
  "                          to stderr; the exit code is 1 only when the logs read",
  "                          itself could not be performed, never because of the",
  "                          run's own state.",
  "",
  "  logs project the durable schema-v9 worker-log bindings of the agent",
  "  executions as the versioned public logs surface (schemaVersion 1): the",
  "  metadata list carries every agent execution in execution-index order",
  "  (decision executions carry no log and never appear), a null binding",
  "  before the terminal worker result reads as 'pending', an unavailable",
  "  capture reads as 'unavailable', and a captured or truncated binding is",
  "  confirmed against its immutable artifact before any byte is served.",
  "  Content is served as bounded base64 chunks of the verified raw payload;",
  "  offset is a raw-byte cursor ('invalid_cursor' beyond the payload), the",
  "  captured digest is over the 'pipeline-v2-worker-log' domain, and",
  "  'captured' never means upstream completeness. The logs are",
  "  secret-bearing and unredacted by design; they are served only through",
  "  this read-only chain (no mutation lock, no Docker Helper involvement,",
  "  no Launcher credential and no durable change), and no log content",
  "  reaches headers, errors, state, events, inspect or discovery.",
  "",
  "project-changes flags (production pipeline v2, read-only):",
  "  --run-id SAFE_ID        the run id of the durable run to read; required",
  "  --config-root PATH      non-empty absolute configuration root; required by the",
  "                          shared command grammar but not used by project-changes",
  "                          (no profile is loaded, no Launcher credential is read)",
  "  --project PATH          clean absolute path of the current operator project;",
  "                          required; observed read-only and never written by",
  "                          this command",
  "  [--after N]             exclusive entry cursor (default 0); a cursor beyond",
  "                          the total change set is 'invalid_cursor'",
  "  [--limit N]             maximum entries per page (default 100, maximum 200)",
  "  [--observation SHA256]  the 64-hex observation identity of a previous page;",
  "                           required with --after > 0; a recomputed digest that",
  "                          differs is 'stale_observation' (no partial page)",
  "  --json                  print exactly one JSON versioned changes document on",
  "                          stdout; errors and diagnostics go to stderr; the exit",
  "                          code is 1 only when the changes read itself could not",
  "                          be performed, never because of the run's own state.",
  "",
  "  project-changes compares the observed current content of the final run",
  "  workspace (<run-root>/project) against the durable schema-v10 project",
  "  provenance baseline and the observed current operator project. The",
  "  result is 'run workspace changes relative to the durable project",
  "  baseline': an ordered change set (added / deleted / content_modified /",
  "  mode_modified / content_and_mode_modified / symlink_target_modified /",
  "  type_changed) with a source relation per entry (matches_baseline /",
  "  matches_result / diverged). The observation digest is an ephemeral",
  "  read snapshot identity, never durable state and never permission to",
  "  apply anything: this command writes nothing anywhere, the operator",
  "  project stays read-only, and run success does not mean project",
  "  delivery. Only finalized terminal runs carry a comparable workspace;",
  "  every other boundary refuses as 'changes_not_ready'.",
  "",
  "project-change-content flags (production pipeline v2, read-only):",
  "  --run-id SAFE_ID        the run id of the durable run to read; required",
  "  --config-root PATH      non-empty absolute configuration root; required by the",
  "                          shared command grammar but not used by",
  "                          project-change-content (no profile is loaded, no",
  "                          Launcher credential is read)",
  "  --project PATH          clean absolute path of the current operator project;",
  "                          required; observed read-only and never written by",
  "                          this command",
  "  --change-index N        the 1-based index of one verified change entry of",
  "                          the recomputed observation; required",
  "  --side baseline|result  which side of the change to read; required",
  "  --observation SHA256    the 64-hex observation identity the content is",
  "                          bound to; required; a recomputed digest that",
  "                          differs is 'stale_observation' (no bytes)",
  "  [--offset N]            raw-byte cursor (default 0); beyond the verified",
  "                          byte count is 'invalid_cursor'",
  "  [--limit N]             maximum raw bytes per response (default 65536,",
  "                          maximum 262144); the cursor counts raw bytes",
  "  --json                  print exactly one JSON versioned content document",
  "                          on stdout; errors and diagnostics go to stderr.",
  "",
  "  The content of one side is available only when it is a regular file",
  "  whose bytes are provable: the baseline side through the current",
  "  operator project when it still matches the durable baseline, the",
  "  result side through the run workspace. The four success statuses are",
  "  'available' (bounded raw-byte slice, verified by one single-pass",
  "  stream whose digest covers the same bytes the slice came from),",
  "  'implicit_empty' (the logical empty side of an added baseline or a",
  "  deleted result - never an error, never a fabricated digest),",
  "  'metadata_only' (directory or symlink side; a symlink is never",
  "  dereferenced) and 'unavailable' (the only reason is",
  "  'source_no_longer_matches_baseline'). The content is secret-bearing",
  "  and unredacted by design; the bytes travel only in 'dataBase64' and",
  "  never in diagnostics, headers or URLs. This command writes nothing",
  "  anywhere (atime may move under the kernel's read policy and is not",
  "  part of any zero-write claim), and no Apply exists.",
  "",
  "project-apply flags (production pipeline v2, explicit Project Apply):",
  "  --run-id SAFE_ID        the terminal run whose reviewed result is applied",
  "  --apply-id SAFE_ID      caller-owned idempotency identity of the operation;",
  "                          the same identity continues crash recovery and",
  "                          never creates a second package",
  "  --project-id SAFE_ID    the registry project id the operation binds to",
  "  --observation SHA256    the 64-hex Project Changes observation the",
  "                          operation is bound to; a recomputed digest that",
  "                          differs is 'stale_observation' (zero writes)",
  "  --config-root PATH      non-empty absolute configuration root; required by",
  "                          the shared command grammar but not used here",
  "                          (no profile is loaded, no Launcher credential)",
  "  --project PATH          clean absolute path of the registered operator",
  "                          project; required; written only under the",
  "                          cooperative project lock",
  "  --apply-store-root PATH clean absolute mode-0700 directory owning the",
  "                          durable operation packages; required",
  "  --json                  print exactly one JSON versioned operation document",
  "                          on stdout; errors and diagnostics go to stderr.",
  "",
  "  Apply is always explicit and never automatic: the operator reviews the",
  "  Project Changes observation first. The operation is durable and",
  "  retryable by apply id; progress is per-path crash-safe (not",
  "  whole-tree atomic), and the cooperative project lock does not protect",
  "  against editors or VCS tools that ignore flock. This command starts",
  "  no worker, no Docker Helper call and no session.",
  "",
  "project-apply-inspect flags (production pipeline v2, read-only):",
  "  --run-id SAFE_ID --apply-id SAFE_ID --project-id SAFE_ID",
  "                          the operation to project; an operation bound to a",
  "                          different run or project is reported as absent",
  "  --config-root PATH      required by the shared command grammar; unused",
  "  --project PATH          clean absolute path of the registered operator",
  "                          project; required; never written",
  "  --apply-store-root PATH the operation store to read; required",
  "  --json                  print exactly one JSON versioned operation document",
  "                          on stdout. This command is strictly read-only: it",
  "                          never runs recovery and never changes any status.",
  "",
    "run locations and runtime configuration:",
    "  The durable run state is written to",
    "    <state-root>/pipeline-runs/<run-id>/state.json",
    "  and the published run outputs stay at the fixed location",
    "    <state-root>/pipeline-runs/<run-id>/outputs",
    "  (run outputs are never copied to a user-chosen path). The state root is",
    "  runtime configuration, not a per-run flag:",
    "    ORCHESTRATOR_STATE_ROOT          local state root; otherwise",
    "                                     ${XDG_STATE_HOME:-$HOME/.local/state}/orchestrator",
    "    ORCHESTRATOR_DAEMON_STATE_ROOT   the same directory by the path the Docker Helper",
    "                                     daemon sees; defaults to the local root (host mode)",
    "  Both variables must be non-empty absolute clean paths; nothing is created by",
    "  the resolver, and the runner itself verifies kind, canonical form, identity",
    "  and mode 0700. The same resolution applies to 'resume' and 'continue-stage',",
    "  which additionally require the existing run root",
    "  <state-root>/pipeline-runs/<run-id> to exist unchanged (it is never created",
    "  or chmodded by them). The same resolution applies to 'respond' and to the",
    "  read-only 'inspect'.",
    "",
    "run profiles and worker configuration:",
    "  The pipeline's agent states select the profiles; the worker image, the",
    "  OpenCode configuration and the exact environment bindings belong to those",
    "  profiles (see the profile schema under agent-smoke). There are no --image,",
    "  --profile, --task, --workspace or output-path flags for 'run': the user never",
    "  sets mounts, container paths, Session credentials, helper transport, or the",
    "  state-root projection per run. Prompt and input bodies travel only inside the",
    "  orchestrator-owned execution document and never appear in command lines,",
    "  environment, state, or diagnostics. A failure before the run root is created",
    "  leaves nothing behind; afterwards the run root is the diagnostic directory",
    "  and is never removed automatically.",
    "",
    "agent-smoke flags:",
    "  --config-root PATH      operator-controlled configuration root; must exist and be",
    "                          visible to the orchestrator at this absolute path",
    "  --pipeline-root PATH    pipeline bundle root; must exist and be visible to the",
    "                          orchestrator at this absolute path",
    `                          (default: ${DEFAULT_PIPELINE_ROOT}, the bundled default pipeline)`,
    "",
    "The pipeline is the production input for agent-smoke: the declarative pipeline",
    "selects the execution profile, the protected workspace input, the agent prompt,",
    "the result contract, and the timeout. The supported v1 execution shape covers",
    "sequential states, branching and cycles bounded by max_transitions: any number",
    "of agent and terminal states, an agent or terminal entry state, one transition",
    'per agent state with outcome "completed", max_attempts 1, and the standard',
    "agent result contract. Any other structurally valid pipeline is rejected before",
    "Launcher authentication and before any child Session is created.",
    "",
  "agent-smoke is the v1 diagnostic command; the production pipeline v2 path is",
  "'orchestrator run', 'orchestrator resume' continues an already durable run",
  "from its clean active boundary and completes the durable post-commit recovery",
  "suffixes of a crashed run (the run id and the configuration root are the",
  "only user inputs of a resume), 'orchestrator inspect' reads one durable run",
  "without changing anything (the run id and the configuration root are the",
  "only user inputs; no Launcher credential and no Docker Helper call),",
  "'orchestrator respond' records the user's",
  "answer to one durably open wait (the run id, the wait index and the action id",
  "are the only user inputs of a response), 'orchestrator continue-stage'",
  "performs the continue_stage stage intervention and resumes the run in one",
  "operation (the run id, the wait index, the additional iteration count and the",
  "configuration root are the only user inputs), 'orchestrator revise-task'",
  "performs the revise_task task revision intervention and resumes the run in",
  "one operation (the run id, the wait index, the task id, the task-body file",
  "and the configuration root are the only user inputs), and 'orchestrator",
  "resume-plan' is the manual planning path for a plan-ready boundary without",
  "the trusted plan_ready auto policy: it accepts the settled planning output",
  "as the next plan revision",
  "and hands the run onto the explicitly selected stage in one operation (the",
  "run id, the stage id, the initial budget and the configuration root are the",
  "only user inputs; the stage id and the initial budget are explicit operator",
  "policy and there is no automatic planning loop). The bundled default-v2",
  "carries the plan_ready auto policy, so a fresh run of it can reach its",
  "terminal in one 'run' command without a separate resume-plan step.",
  "",
  "There is no --profile, --task, or --image flag for agent-smoke: the profile and",
    "input path come only from the pipeline's agent state, the worker image comes",
    "only from the selected profile.",
    "",
    "The execution profile owns the trusted worker configuration: the worker image,",
    "the OpenCode configuration file and the exact environment bindings.",
    "",
    "Profile schema (schema_version 1, YAML, under <config-root>/profiles/<name>.yaml):",
    "  schema_version: 1",
    "  image: <worker image>",
    "  opencode_config: <path relative to the configuration root>",
    "  env:",
    "    DEST:",
    "      from_env: SOURCE_VAR",
    "      required: true|false",
    "",
    "Profile rules:",
    "  - unknown and missing fields are rejected; schema_version must be 1",
    "  - the profile name is a single safe path component; the profile file and the OpenCode",
    "    configuration must be regular files that resolve inside the configuration root",
    "  - env bindings are exact: only declared variables reach the worker, there is no",
    "    ambient inheritance; a missing required source variable fails before any child",
    "    session is created; a missing optional source variable is not forwarded",
    "  - orchestrator-owned control variables (DOCKER_HELPER_*, AGENT_SMOKE_*,",
    "    ORCHESTRATOR_*, OPENCODE_CONFIG_CONTENT) and operator path variables (HOME,",
    "    XDG_CONFIG_HOME, XDG_STATE_HOME, XDG_RUNTIME_DIR) can be neither destinations",
    "    nor sources",
    "  - profile files may reference secret environment-variable names but must never",
    "    contain secret values; resolved values are passed to the worker as",
    "    `--env KEY=VALUE` arguments of the docker-helper run CLI call; until",
    "    docker-helper issue #3 is implemented they appear in that process's argv;",
    "    they never appear in state files or the orchestrator's own diagnostics",
    "",
    "The OpenCode configuration is read by the orchestrator from the path given by",
    "opencode_config and forwarded to the worker as OPENCODE_CONFIG_CONTENT.",
    "",
    "The agent prompt comes from the pipeline bundle and is materialized, together",
    "with the run identity and the exact result format, into an orchestrator-owned",
    "execution document inside the run directory of the workspace. The worker command",
    "receives only that workspace path; prompt and input bodies never appear in",
    "command lines, environment, state, or the orchestrator's own diagnostics.",
    "",
    "The pipeline's timeout_seconds bounds the worker docker-helper run: after the",
    "deadline the runner sends the CLI process SIGTERM and the run fails normally.",
    "User SIGINT/SIGTERM keeps its own semantics and exit codes 130/143.",
    "",
    "The launcher credential is read from",
    "  ${XDG_CONFIG_HOME:-$HOME/.config}/docker-helper/credential.token",
    "install it with: XDG_CONFIG_HOME=<private-dir> docker-helper credential install",
  ].join("\n");
}

function parseValue(argv: string[], i: number, flag: string): { value: string; next: number } {
  const arg = argv[i] ?? "";
  if (arg === flag) {
    const value = argv[i + 1];
    if (value === undefined || value === "") {
      throw new Error(`${flag} requires a value`);
    }
    return { value, next: i + 1 };
  }
  if (arg.startsWith(`${flag}=`)) {
    const value = arg.slice(`${flag}=`.length);
    if (value === "") {
      throw new Error(`${flag} requires a value`);
    }
    return { value, next: i };
  }
  throw new Error(`unknown argument: ${arg}`);
}

/**
 * Parses the read-only pipeline v2 inspect command. The user-facing
 * contract: `--run-id` (safe-id validated) and `--config-root`
 * (non-empty absolute path) are required singletons; `--json` is an
 * optional singleton. Inspect performs no Launcher authentication, no
 * Docker Helper transport and no durable change: it accepts no
 * `--launcher-id`, no fresh-run flag, no intervention flag, no
 * state-root flag and no internal index/digest flag — every unknown flag
 * or positional argument is rejected. The pipeline, the boundary
 * classification and the output projection come only from the durable
 * state, the run-owned layout and the verified pipeline.
 */
function parseInspectArgs(argv: string[]): ParsedPipelineInspectArgs {
  let runId: string | null = null;
  let configRoot: string | null = null;
  let json = false;

  const requireSingleton = (current: string | null, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      requireSingleton(runId, "--run-id");
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      throw new Error(
        "inspect does not accept --launcher-id; it performs no Launcher authentication",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "inspect does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error("inspect does not accept --project; the run-owned project copy already exists");
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error("inspect does not accept --input; the run-owned input snapshot already exists");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error("inspect does not accept --workspace; no worker is launched by inspect");
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("inspect does not accept --image; no worker is launched by inspect");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error("inspect does not accept --profile; no profile is loaded by inspect");
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("inspect does not accept --task; there is no task input on an inspect");
    } else if (arg === "--stage-id" || arg.startsWith("--stage-id=")) {
      throw new Error(
        "inspect does not accept --stage-id; stage selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--initial-budget" || arg.startsWith("--initial-budget=")) {
      throw new Error(
        "inspect does not accept --initial-budget; budget selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      throw new Error(
        "inspect does not accept --wait-index; wait selection belongs to the response and intervention commands",
      );
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "inspect does not accept --action; it changes nothing and records no response",
      );
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      throw new Error(
        "inspect does not accept --additional-iterations; grants belong to 'continue-stage'",
      );
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      throw new Error("inspect does not accept --task-id; task revisions belong to 'revise-task'");
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      throw new Error("inspect does not accept --task-file; task revisions belong to 'revise-task'");
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      throw new Error("inspect does not accept --task-body; task revisions belong to 'revise-task'");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "inspect does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "inspect does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for inspect");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABS is required for inspect");
  }
  return {
    kind: "inspect",
    runId,
    configRoot,
    json,
  };
}

/**
 * Parses the read-only pipeline v2 events command. The user-facing
 * contract: `--run-id` (safe-id validated) and `--config-root`
 * (non-empty absolute path) are required singletons; `--after`
 * (non-negative decimal safe integer, default 0 — the exclusive durable
 * revision cursor) and `--limit` (positive decimal safe integer, default
 * 100, maximum 200) are optional singletons; `--json` is an optional
 * singleton. Events performs no Launcher authentication, no Docker
 * Helper transport, no mutation lock and no durable change: it accepts
 * no `--launcher-id`, no fresh-run flag, no intervention flag, no
 * state-root flag and no internal index/digest flag — every unknown flag
 * or positional argument is rejected. The events projection comes only
 * from the durable schema-v8 commit history.
 */
function parseEventsArgs(argv: string[]): ParsedPipelineEventsArgs {
  let runId: string | null = null;
  let configRoot: string | null = null;
  let after: number | null = null;
  let limit: number | null = null;
  let json = false;

  const requireSingleton = (current: unknown, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      requireSingleton(runId, "--run-id");
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--after" || arg.startsWith("--after=")) {
      const { value, next } = parseValue(argv, i, "--after");
      requireSingleton(after, "--after");
      after = parseNonNegativeDecimalSafeInteger(value, "--after");
      i = next;
    } else if (arg === "--limit" || arg.startsWith("--limit=")) {
      const { value, next } = parseValue(argv, i, "--limit");
      requireSingleton(limit, "--limit");
      const parsed = parsePositiveDecimalSafeInteger(value, "--limit");
      if (parsed > 200) {
        throw new Error("--limit must be a positive safe integer up to 200");
      }
      limit = parsed;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      throw new Error(
        "events does not accept --launcher-id; it performs no Launcher authentication",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "events does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error("events does not accept --project; the run-owned project copy already exists");
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error("events does not accept --input; the run-owned input snapshot already exists");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error("events does not accept --workspace; no worker is launched by events");
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("events does not accept --image; no worker is launched by events");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error("events does not accept --profile; no profile is loaded by events");
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("events does not accept --task; there is no task input on an events read");
    } else if (arg === "--stage-id" || arg.startsWith("--stage-id=")) {
      throw new Error(
        "events does not accept --stage-id; stage selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--initial-budget" || arg.startsWith("--initial-budget=")) {
      throw new Error(
        "events does not accept --initial-budget; budget selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      throw new Error(
        "events does not accept --wait-index; wait selection belongs to the response and intervention commands",
      );
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "events does not accept --action; it changes nothing and records no response",
      );
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      throw new Error(
        "events does not accept --additional-iterations; grants belong to 'continue-stage'",
      );
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      throw new Error("events does not accept --task-id; task revisions belong to 'revise-task'");
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      throw new Error("events does not accept --task-file; task revisions belong to 'revise-task'");
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      throw new Error("events does not accept --task-body; task revisions belong to 'revise-task'");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "events does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "events does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for events");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABS is required for events");
  }
  return {
    kind: "events",
    runId,
    configRoot,
    after: after ?? 0,
    limit: limit ?? 100,
    json,
  };
}

function parseLogsArgs(argv: string[]): ParsedPipelineLogsArgs {
  let runId: string | null = null;
  let configRoot: string | null = null;
  let executionIndex: number | null = null;
  let offset: number | null = null;
  let limit: number | null = null;
  let json = false;

  const requireSingleton = (current: unknown, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      requireSingleton(runId, "--run-id");
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--execution-index" || arg.startsWith("--execution-index=")) {
      const { value, next } = parseValue(argv, i, "--execution-index");
      requireSingleton(executionIndex, "--execution-index");
      executionIndex = parsePositiveDecimalSafeInteger(value, "--execution-index");
      i = next;
    } else if (arg === "--offset" || arg.startsWith("--offset=")) {
      const { value, next } = parseValue(argv, i, "--offset");
      requireSingleton(offset, "--offset");
      offset = parseNonNegativeDecimalSafeInteger(value, "--offset");
      i = next;
    } else if (arg === "--limit" || arg.startsWith("--limit=")) {
      const { value, next } = parseValue(argv, i, "--limit");
      requireSingleton(limit, "--limit");
      const parsed = parsePositiveDecimalSafeInteger(value, "--limit");
      if (parsed > 262144) {
        throw new Error("--limit must be a positive safe integer up to 262144");
      }
      limit = parsed;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      throw new Error(
        "logs does not accept --launcher-id; it performs no Launcher authentication",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "logs does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error("logs does not accept --project; the run-owned project copy already exists");
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error("logs does not accept --input; the run-owned input snapshot already exists");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error("logs does not accept --workspace; no worker is launched by logs");
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("logs does not accept --image; no worker is launched by logs");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error("logs does not accept --profile; no profile is loaded by logs");
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("logs does not accept --task; there is no task input on a logs read");
    } else if (arg === "--stage-id" || arg.startsWith("--stage-id=")) {
      throw new Error(
        "logs does not accept --stage-id; stage selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--initial-budget" || arg.startsWith("--initial-budget=")) {
      throw new Error(
        "logs does not accept --initial-budget; budget selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      throw new Error(
        "logs does not accept --wait-index; wait selection belongs to the response and intervention commands",
      );
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "logs does not accept --action; it changes nothing and records no response",
      );
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      throw new Error(
        "logs does not accept --additional-iterations; grants belong to 'continue-stage'",
      );
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      throw new Error("logs does not accept --task-id; task revisions belong to 'revise-task'");
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      throw new Error("logs does not accept --task-file; task revisions belong to 'revise-task'");
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      throw new Error("logs does not accept --task-body; task revisions belong to 'revise-task'");
    } else if (arg === "--after" || arg.startsWith("--after=")) {
      throw new Error("logs does not accept --after; the events cursor belongs to 'events'");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "logs does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "logs does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for logs");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABS is required for logs");
  }
  if (offset !== null && executionIndex === null) {
    throw new Error("--offset requires --execution-index");
  }
  if (limit !== null && executionIndex === null) {
    throw new Error("--limit requires --execution-index");
  }
  return {
    kind: "logs",
    runId,
    configRoot,
    executionIndex,
    offset: offset ?? 0,
    limit: limit ?? 65536,
    json,
  };
}

function parseProjectChangesArgs(argv: string[]): ParsedPipelineProjectChangesArgs {
  let runId: string | null = null;
  let configRoot: string | null = null;
  let project: string | null = null;
  let after: number | null = null;
  let limit: number | null = null;
  let observation: string | null = null;
  let json = false;

  const requireSingleton = (current: unknown, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      requireSingleton(runId, "--run-id");
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      const { value, next } = parseValue(argv, i, "--project");
      requireSingleton(project, "--project");
      project = requireAbsolute(value, "--project");
      if (!isCleanAbsolutePath(project)) {
        throw new Error("--project must be a clean absolute path");
      }
      i = next;
    } else if (arg === "--after" || arg.startsWith("--after=")) {
      const { value, next } = parseValue(argv, i, "--after");
      requireSingleton(after, "--after");
      after = parseNonNegativeDecimalSafeInteger(value, "--after");
      i = next;
    } else if (arg === "--limit" || arg.startsWith("--limit=")) {
      const { value, next } = parseValue(argv, i, "--limit");
      requireSingleton(limit, "--limit");
      const parsed = parsePositiveDecimalSafeInteger(value, "--limit");
      if (parsed > 200) {
        throw new Error("--limit must be a positive safe integer up to 200");
      }
      limit = parsed;
      i = next;
    } else if (arg === "--observation" || arg.startsWith("--observation=")) {
      const { value, next } = parseValue(argv, i, "--observation");
      requireSingleton(observation, "--observation");
      if (!/^[0-9a-f]{64}$/.test(value)) {
        throw new Error("--observation must be a 64-character lowercase hexadecimal digest");
      }
      observation = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      throw new Error(
        "project-changes does not accept --launcher-id; it performs no Launcher authentication",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "project-changes does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error("project-changes does not accept --input; the run-owned input snapshot already exists");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error("project-changes does not accept --workspace; no worker is launched by project-changes");
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("project-changes does not accept --image; no worker is launched by project-changes");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error("project-changes does not accept --profile; no profile is loaded by project-changes");
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("project-changes does not accept --task; there is no task input on a changes read");
    } else if (arg === "--stage-id" || arg.startsWith("--stage-id=")) {
      throw new Error(
        "project-changes does not accept --stage-id; stage selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--initial-budget" || arg.startsWith("--initial-budget=")) {
      throw new Error(
        "project-changes does not accept --initial-budget; budget selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      throw new Error(
        "project-changes does not accept --wait-index; wait selection belongs to the response and intervention commands",
      );
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "project-changes does not accept --action; it changes nothing and records no response",
      );
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      throw new Error(
        "project-changes does not accept --additional-iterations; grants belong to 'continue-stage'",
      );
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      throw new Error("project-changes does not accept --task-id; task revisions belong to 'revise-task'");
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      throw new Error("project-changes does not accept --task-file; task revisions belong to 'revise-task'");
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      throw new Error("project-changes does not accept --task-body; task revisions belong to 'revise-task'");
    } else if (arg === "--execution-index" || arg.startsWith("--execution-index=")) {
      throw new Error("project-changes does not accept --execution-index; the log cursor belongs to 'logs'");
    } else if (arg === "--offset" || arg.startsWith("--offset=")) {
      throw new Error("project-changes does not accept --offset; the log cursor belongs to 'logs'");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "project-changes does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "project-changes does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for project-changes");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABS is required for project-changes");
  }
  if (project === null) {
    throw new Error("--project ABS is required for project-changes");
  }
  return {
    kind: "project-changes",
    runId,
    configRoot,
    project,
    after: after ?? 0,
    limit: limit ?? 100,
    observation,
    json,
  };
}

function parseProjectChangeContentArgs(argv: string[]): ParsedPipelineProjectChangeContentArgs {
  let runId: string | null = null;
  let configRoot: string | null = null;
  let project: string | null = null;
  let changeIndex: number | null = null;
  let side: string | null = null;
  let observation: string | null = null;
  let offset: number | null = null;
  let limit: number | null = null;
  let json = false;

  const requireSingleton = (current: unknown, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      requireSingleton(runId, "--run-id");
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      const { value, next } = parseValue(argv, i, "--project");
      requireSingleton(project, "--project");
      project = requireAbsolute(value, "--project");
      if (!isCleanAbsolutePath(project)) {
        throw new Error("--project must be a clean absolute path");
      }
      i = next;
    } else if (arg === "--change-index" || arg.startsWith("--change-index=")) {
      const { value, next } = parseValue(argv, i, "--change-index");
      requireSingleton(changeIndex, "--change-index");
      changeIndex = parsePositiveDecimalSafeInteger(value, "--change-index");
      i = next;
    } else if (arg === "--side" || arg.startsWith("--side=")) {
      const { value, next } = parseValue(argv, i, "--side");
      requireSingleton(side, "--side");
      if (value !== "baseline" && value !== "result") {
        throw new Error("--side must be 'baseline' or 'result'");
      }
      side = value;
      i = next;
    } else if (arg === "--observation" || arg.startsWith("--observation=")) {
      const { value, next } = parseValue(argv, i, "--observation");
      requireSingleton(observation, "--observation");
      if (!/^[0-9a-f]{64}$/.test(value)) {
        throw new Error("--observation must be a 64-character lowercase hexadecimal digest");
      }
      observation = value;
      i = next;
    } else if (arg === "--offset" || arg.startsWith("--offset=")) {
      const { value, next } = parseValue(argv, i, "--offset");
      requireSingleton(offset, "--offset");
      offset = parseNonNegativeDecimalSafeInteger(value, "--offset");
      i = next;
    } else if (arg === "--limit" || arg.startsWith("--limit=")) {
      const { value, next } = parseValue(argv, i, "--limit");
      requireSingleton(limit, "--limit");
      const parsed = parsePositiveDecimalSafeInteger(value, "--limit");
      if (parsed > 262144) {
        throw new Error("--limit must be a positive safe integer up to 262144");
      }
      limit = parsed;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--after" || arg.startsWith("--after=")) {
      throw new Error("project-change-content does not accept --after; the entry cursor belongs to 'project-changes'");
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      throw new Error(
        "project-change-content does not accept --launcher-id; it performs no Launcher authentication",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "project-change-content does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error("project-change-content does not accept --input; the run-owned input snapshot already exists");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error("project-change-content does not accept --workspace; no worker is launched by project-change-content");
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("project-change-content does not accept --image; no worker is launched by project-change-content");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error("project-change-content does not accept --profile; no profile is loaded by project-change-content");
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("project-change-content does not accept --task; there is no task input on a content read");
    } else if (arg === "--stage-id" || arg.startsWith("--stage-id=")) {
      throw new Error(
        "project-change-content does not accept --stage-id; stage selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--initial-budget" || arg.startsWith("--initial-budget=")) {
      throw new Error(
        "project-change-content does not accept --initial-budget; budget selection is operator policy of 'resume-plan'",
      );
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      throw new Error(
        "project-change-content does not accept --wait-index; wait selection belongs to the response and intervention commands",
      );
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "project-change-content does not accept --action; it changes nothing and records no response",
      );
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      throw new Error(
        "project-change-content does not accept --additional-iterations; grants belong to 'continue-stage'",
      );
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      throw new Error("project-change-content does not accept --task-id; task revisions belong to 'revise-task'");
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      throw new Error("project-change-content does not accept --task-file; task revisions belong to 'revise-task'");
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      throw new Error("project-change-content does not accept --task-body; task revisions belong to 'revise-task'");
    } else if (arg === "--execution-index" || arg.startsWith("--execution-index=")) {
      throw new Error("project-change-content does not accept --execution-index; the log cursor belongs to 'logs'");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "project-change-content does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "project-change-content does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for project-change-content");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABS is required for project-change-content");
  }
  if (project === null) {
    throw new Error("--project ABS is required for project-change-content");
  }
  if (changeIndex === null) {
    throw new Error("--change-index N is required for project-change-content");
  }
  if (side === null) {
    throw new Error("--side baseline|result is required for project-change-content");
  }
  if (observation === null) {
    throw new Error("--observation SHA256 is required for project-change-content");
  }
  return {
    kind: "project-change-content",
    runId,
    configRoot,
    project,
    changeIndex,
    side: side as "baseline" | "result",
    observation,
    offset: offset ?? 0,
    limit: limit ?? 65536,
    json,
  };
}

export interface ParsedPipelineProjectApplyArgs {
  readonly kind: "project-apply";
  readonly runId: string;
  readonly applyId: string;
  readonly projectId: string;
  readonly observation: string;
  readonly configRoot: string;
  readonly project: string;
  readonly applyStoreRoot: string;
  readonly json: boolean;
}

export interface ParsedPipelineProjectApplyInspectArgs {
  readonly kind: "project-apply-inspect";
  readonly runId: string;
  readonly applyId: string;
  readonly projectId: string;
  readonly configRoot: string;
  readonly project: string;
  readonly applyStoreRoot: string;
  readonly json: boolean;
}

/** The shared flag battery of the two explicit Project Apply commands. */
function parseProjectApplyShared(
  argv: string[],
  commandName: "project-apply" | "project-apply-inspect",
): {
  runId: string;
  applyId: string;
  projectId: string;
  observation: string | null;
  configRoot: string;
  project: string;
  applyStoreRoot: string;
  json: boolean;
} {
  let runId: string | null = null;
  let applyId: string | null = null;
  let projectId: string | null = null;
  let observation: string | null = null;
  let configRoot: string | null = null;
  let project: string | null = null;
  let applyStoreRoot: string | null = null;
  let json = false;

  const requireSingleton = (current: unknown, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };
  const expectSafeIdFlag = (value: string, flag: string): string => {
    try {
      expectSafeId(value, flag);
    } catch (cause) {
      throw new Error(`${flag} must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`);
    }
    return value;
  };
  const foreign = (arg: string, why: string): never => {
    throw new Error(`${commandName} does not accept ${arg.split("=")[0] ?? arg}; ${why}`);
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      requireSingleton(runId, "--run-id");
      runId = expectSafeIdFlag(value, "--run-id");
      i = next;
    } else if (arg === "--apply-id" || arg.startsWith("--apply-id=")) {
      const { value, next } = parseValue(argv, i, "--apply-id");
      requireSingleton(applyId, "--apply-id");
      applyId = expectSafeIdFlag(value, "--apply-id");
      i = next;
    } else if (arg === "--project-id" || arg.startsWith("--project-id=")) {
      const { value, next } = parseValue(argv, i, "--project-id");
      requireSingleton(projectId, "--project-id");
      projectId = expectSafeIdFlag(value, "--project-id");
      i = next;
    } else if (arg === "--observation" || arg.startsWith("--observation=")) {
      if (commandName === "project-apply-inspect") {
        foreign(arg, "the bound observation comes from the durable operation");
      }
      const { value, next } = parseValue(argv, i, "--observation");
      requireSingleton(observation, "--observation");
      if (!/^[0-9a-f]{64}$/.test(value)) {
        throw new Error("--observation must be a 64-character lowercase hexadecimal digest");
      }
      observation = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      const { value, next } = parseValue(argv, i, "--project");
      requireSingleton(project, "--project");
      project = requireAbsolute(value, "--project");
      if (!isCleanAbsolutePath(project)) {
        throw new Error("--project must be a clean absolute path");
      }
      i = next;
    } else if (arg === "--apply-store-root" || arg.startsWith("--apply-store-root=")) {
      const { value, next } = parseValue(argv, i, "--apply-store-root");
      requireSingleton(applyStoreRoot, "--apply-store-root");
      applyStoreRoot = requireAbsolute(value, "--apply-store-root");
      if (!isCleanAbsolutePath(applyStoreRoot)) {
        throw new Error("--apply-store-root must be a clean absolute path");
      }
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      foreign(arg, "it performs no Launcher authentication");
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      foreign(arg, "the pipeline comes only from the durable state");
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      foreign(arg, "the run-owned input snapshot already exists");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      foreign(arg, "no worker is launched");
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      foreign(arg, "no worker is launched");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      foreign(arg, "no profile is loaded");
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      foreign(arg, "there is no task input");
    } else if (arg === "--stage-id" || arg.startsWith("--stage-id=")) {
      foreign(arg, "stage selection is operator policy of 'resume-plan'");
    } else if (arg === "--initial-budget" || arg.startsWith("--initial-budget=")) {
      foreign(arg, "budget selection is operator policy of 'resume-plan'");
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      foreign(arg, "wait selection belongs to the response and intervention commands");
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      foreign(arg, "it changes nothing and records no response");
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      foreign(arg, "grants belong to 'continue-stage'");
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      foreign(arg, "task revisions belong to 'revise-task'");
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      foreign(arg, "task revisions belong to 'revise-task'");
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      foreign(arg, "task revisions belong to 'revise-task'");
    } else if (arg === "--execution-index" || arg.startsWith("--execution-index=")) {
      foreign(arg, "the log cursor belongs to 'logs'");
    } else if (arg === "--offset" || arg.startsWith("--offset=")) {
      foreign(arg, "the content cursor belongs to 'project-change-content'");
    } else if (arg === "--limit" || arg.startsWith("--limit=")) {
      foreign(arg, "the content cursor belongs to 'project-change-content'");
    } else if (arg === "--change-index" || arg.startsWith("--change-index=")) {
      foreign(arg, "the change index belongs to 'project-change-content'");
    } else if (arg === "--side" || arg.startsWith("--side=")) {
      foreign(arg, "the content side belongs to 'project-change-content'");
    } else if (arg === "--after" || arg.startsWith("--after=")) {
      foreign(arg, "the entry cursor belongs to 'project-changes'");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      foreign(arg, "set the ORCHESTRATOR_STATE_ROOT environment variable");
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      foreign(arg, "set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable");
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error(`--run-id SAFE_ID is required for ${commandName}`);
  }
  if (applyId === null) {
    throw new Error(`--apply-id SAFE_ID is required for ${commandName}`);
  }
  if (projectId === null) {
    throw new Error(`--project-id SAFE_ID is required for ${commandName}`);
  }
  if (configRoot === null) {
    throw new Error(`--config-root ABS is required for ${commandName}`);
  }
  if (project === null) {
    throw new Error(`--project ABS is required for ${commandName}`);
  }
  if (applyStoreRoot === null) {
    throw new Error(`--apply-store-root ABS is required for ${commandName}`);
  }
  return {
    runId,
    applyId: applyId,
    projectId: projectId,
    observation,
    configRoot,
    project,
    applyStoreRoot,
    json,
  };
}

function parseProjectApplyArgs(argv: string[]): ParsedPipelineProjectApplyArgs {
  const shared = parseProjectApplyShared(argv, "project-apply");
  if (shared.observation === null) {
    throw new Error("--observation SHA256 is required for project-apply");
  }
  return {
    kind: "project-apply",
    runId: shared.runId,
    applyId: shared.applyId,
    projectId: shared.projectId,
    observation: shared.observation,
    configRoot: shared.configRoot,
    project: shared.project,
    applyStoreRoot: shared.applyStoreRoot,
    json: shared.json,
  };
}

function parseProjectApplyInspectArgs(argv: string[]): ParsedPipelineProjectApplyInspectArgs {
  const shared = parseProjectApplyShared(argv, "project-apply-inspect");
  return {
    kind: "project-apply-inspect",
    runId: shared.runId,
    applyId: shared.applyId,
    projectId: shared.projectId,
    configRoot: shared.configRoot,
    project: shared.project,
    applyStoreRoot: shared.applyStoreRoot,
    json: shared.json,
  };
}

export function parseCommand(
  kind: "smoke" | "agent-smoke" | "run" | "resume" | "respond" | "continue-stage" | "revise-task" | "resume-plan" | "inspect" | "events" | "logs" | "project-changes" | "project-change-content" | "project-apply" | "project-apply-inspect",
  argv: string[],
): ParsedCommand {
  if (kind === "run") {
    return parseRunArgs(argv);
  }
  if (kind === "resume") {
    return parseResumeArgs(argv);
  }
  if (kind === "respond") {
    return parseRespondArgs(argv);
  }
  if (kind === "continue-stage") {
    return parseContinueStageArgs(argv);
  }
  if (kind === "revise-task") {
    return parseReviseTaskArgs(argv);
  }
  if (kind === "resume-plan") {
    return parseResumePlanArgs(argv);
  }
  if (kind === "inspect") {
    return parseInspectArgs(argv);
  }
  if (kind === "events") {
    return parseEventsArgs(argv);
  }
  if (kind === "logs") {
    return parseLogsArgs(argv);
  }
  if (kind === "project-changes") {
    return parseProjectChangesArgs(argv);
  }
  if (kind === "project-change-content") {
    return parseProjectChangeContentArgs(argv);
  }
  if (kind === "project-apply") {
    return parseProjectApplyArgs(argv);
  }
  if (kind === "project-apply-inspect") {
    return parseProjectApplyInspectArgs(argv);
  }
  let workspace: string | null = null;
  let workerImage: string | null = null;
  let launcherId: string | undefined;
  let configRoot: string | null = null;
  let pipelineRoot: string | undefined;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      const { value, next } = parseValue(argv, i, "--workspace");
      if (!value.startsWith("/")) {
        throw new Error("--workspace must be an absolute path");
      }
      workspace = value;
      i = next;
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      if (kind === "agent-smoke") {
        throw new Error(
          "agent-smoke does not accept --image; the worker image comes only from the pipeline-selected execution profile",
        );
      }
      const { value, next } = parseValue(argv, i, "--image");
      workerImage = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      if (kind !== "agent-smoke") {
        throw new Error(`unknown argument: ${arg}`);
      }
      const { value, next } = parseValue(argv, i, "--config-root");
      if (!value.startsWith("/")) {
        throw new Error("--config-root must be an absolute path");
      }
      configRoot = value;
      i = next;
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      if (kind !== "agent-smoke") {
        throw new Error(`unknown argument: ${arg}`);
      }
      const { value, next } = parseValue(argv, i, "--pipeline-root");
      if (!value.startsWith("/")) {
        throw new Error("--pipeline-root must be an absolute path");
      }
      pipelineRoot = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      if (kind === "agent-smoke") {
        throw new Error(
          "agent-smoke no longer accepts --profile; the execution profile is selected by the pipeline's agent state",
        );
      }
      throw new Error(`unknown argument: ${arg}`);
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      if (kind === "agent-smoke") {
        throw new Error(
          "agent-smoke no longer accepts --task; the workspace input path is declared by the pipeline",
        );
      }
      throw new Error(`unknown argument: ${arg}`);
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (kind === "smoke") {
    return {
      kind,
      workspace: workspace ?? defaultWorkspace(),
      workerImage: workerImage ?? DEFAULT_WORKER_IMAGE,
      launcherId,
    };
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for agent-smoke");
  }
  return {
    kind,
    workspace: workspace ?? defaultWorkspace(),
    configRoot,
    pipelineRoot: pipelineRoot ?? DEFAULT_PIPELINE_ROOT,
    launcherId,
  };
}

/**
 * Parses the production pipeline v2 command. The user-facing contract:
 * `--pipeline-root`, `--config-root` and `--project` are required and must
 * be absolute; `--input` is repeatable in `SAFE_ID=ABSOLUTE_PATH` form
 * (split at the first `=`, so `=` inside the path is allowed); zero inputs
 * are valid. Singleton flags reject duplicates instead of silently taking
 * the last value; `--json` takes no value and cannot repeat; the v1 and
 * container flags, output-path flags and state-root flags are rejected for
 * `run`, as is every unknown flag.
 */
function parseRunArgs(argv: string[]): ParsedPipelineRunArgs {
  let pipelineRoot: string | null = null;
  let configRoot: string | null = null;
  let projectSourcePath: string | null = null;
  let runId: string | null = null;
  let launcherId: string | undefined;
  let json = false;
  const inputBindings: RunInputBinding[] = [];
  const seenInputIds = new Set<string>();

  const requireSingleton = (current: string | null, flag: string): void => {
    if (current !== null) {
      throw new Error(`${flag} may be given at most once`);
    }
  };
  const requireAbsolute = (value: string, flag: string): string => {
    if (!value.startsWith("/")) {
      throw new Error(`${flag} must be an absolute path`);
    }
    return value;
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      const { value, next } = parseValue(argv, i, "--pipeline-root");
      requireSingleton(pipelineRoot, "--pipeline-root");
      pipelineRoot = requireAbsolute(value, "--pipeline-root");
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      requireSingleton(configRoot, "--config-root");
      configRoot = requireAbsolute(value, "--config-root");
      i = next;
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      const { value, next } = parseValue(argv, i, "--project");
      requireSingleton(projectSourcePath, "--project");
      projectSourcePath = requireAbsolute(value, "--project");
      i = next;
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      const { value, next } = parseValue(argv, i, "--input");
      const equals = value.indexOf("=");
      if (equals <= 0) {
        throw new Error("--input requires SAFE_ID=ABSOLUTE_PATH");
      }
      const id = value.slice(0, equals);
      const path = value.slice(equals + 1);
      try {
        expectSafeId(id, "--input id");
      } catch (cause) {
        throw new Error(
          `--input id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      if (path === "") {
        throw new Error("--input requires SAFE_ID=ABSOLUTE_PATH (the path part is empty)");
      }
      if (!path.startsWith("/")) {
        throw new Error(`--input ${id} must be bound to an absolute path`);
      }
      if (seenInputIds.has(id)) {
        throw new Error(`--input ${id} is bound more than once`);
      }
      seenInputIds.add(id);
      inputBindings.push({ id, path });
      i = next;
    } else if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      if (runId !== null) {
        throw new Error("--run-id may be given at most once");
      }
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      if (launcherId !== undefined) {
        throw new Error("--launcher-id may be given at most once");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error(
        "run does not accept --workspace; the run-owned project copy is created from --project",
      );
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("run does not accept --image; the worker image comes only from the selected profile");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error(
        "run does not accept --profile; the execution profiles are selected by the pipeline's agent states",
      );
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error(
        "run does not accept --task; run inputs are declared by the pipeline and bound with --input",
      );
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "run does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "run does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for run");
  }
  if (projectSourcePath === null) {
    throw new Error("--project ABSOLUTE_PATH is required for run");
  }
  return {
    kind: "run",
    pipelineRoot: pipelineRoot ?? DEFAULT_PIPELINE_V2_ROOT,
    configRoot,
    projectSourcePath,
    inputBindings,
    runId: runId ?? undefined,
    launcherId,
    json,
  };
}

/**
 * Parses the production pipeline v2 resume command. The user-facing
 * contract: `--run-id` (safe-id validated) and `--config-root` (absolute)
 * are required; `--launcher-id` and `--json` are optional singletons;
 * `--json` takes no value. Every fresh-run flag — `--pipeline-root`,
 * `--project`, `--input`, `--workspace`, `--image`, `--profile`, `--task`
 * and the state-root flags — is rejected for `resume`, as is every unknown
 * flag or positional argument: the pipeline comes only from the durable
 * state, and the project and inputs only from the run-owned layout.
 */
function parseResumeArgs(argv: string[]): ParsedPipelineResumeArgs {
  let runId: string | null = null;
  let configRoot: string | null = null;
  let launcherId: string | undefined;
  let json = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      if (runId !== null) {
        throw new Error("--run-id may be given at most once");
      }
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      if (configRoot !== null) {
        throw new Error("--config-root may be given at most once");
      }
      if (!value.startsWith("/")) {
        throw new Error("--config-root must be an absolute path");
      }
      configRoot = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      if (launcherId !== undefined) {
        throw new Error("--launcher-id may be given at most once");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "resume does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error(
        "resume does not accept --project; the run-owned project copy already exists in the run root",
      );
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error(
        "resume does not accept --input; the run-owned input snapshot already exists in the run root",
      );
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error(
        "resume does not accept --workspace; the run-owned project copy is mounted from the run root",
      );
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error(
        "resume does not accept --image; the worker image comes only from the selected profile",
      );
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error(
        "resume does not accept --profile; the execution profiles are selected by the pipeline's agent states",
      );
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("resume does not accept --task; there is no task input on a resumed run");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "resume does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "resume does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for resume");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for resume");
  }
  return {
    kind: "resume",
    runId,
    configRoot,
    launcherId,
    json,
  };
}

/**
 * The canonical positive decimal safe-integer grammar for `--wait-index`:
 * one or more digits with no leading zero, no sign, no whitespace, no
 * fraction and no exponent, whose numeric value is a safe integer. The
 * parser rejects everything else before any filesystem access.
 */
function parsePositiveDecimalSafeInteger(value: string, flag: string): number {
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(
      `${flag} must be a positive decimal integer without sign, leading zeros, fraction, exponent or whitespace`,
    );
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${flag} exceeds the safe integer range`);
  }
  return parsed;
}

/**
 * Parses one non-negative decimal safe integer (zero allowed): no sign,
 * no leading zeros, no fraction, no exponent, whose numeric value is a
 * safe integer. The parser rejects everything else before any filesystem
 * access.
 */
function parseNonNegativeDecimalSafeInteger(value: string, flag: string): number {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error(
      `${flag} must be a non-negative decimal integer without sign, leading zeros, fraction, exponent or whitespace`,
    );
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${flag} exceeds the safe integer range`);
  }
  return parsed;
}

/**
 * Parses the production pipeline v2 wait-response command. The
 * user-facing contract: `--run-id` (safe-id validated), `--wait-index`
 * (canonical positive decimal safe integer) and `--action` (safe-id
 * validated) are required singletons; `--json` takes no value and cannot
 * repeat. The caller can never pass a target state, a request digest, a
 * response digest or a JSON body: every fresh-run and resume flag is
 * rejected for `respond`, as is every unknown flag or positional argument.
 */
function parseRespondArgs(argv: string[]): ParsedPipelineRespondArgs {
  let runId: string | null = null;
  let waitIndex: number | null = null;
  let actionId: string | null = null;
  let json = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      if (runId !== null) {
        throw new Error("--run-id may be given at most once");
      }
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      const { value, next } = parseValue(argv, i, "--wait-index");
      if (waitIndex !== null) {
        throw new Error("--wait-index may be given at most once");
      }
      waitIndex = parsePositiveDecimalSafeInteger(value, "--wait-index");
      i = next;
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      const { value, next } = parseValue(argv, i, "--action");
      if (actionId !== null) {
        throw new Error("--action may be given at most once");
      }
      try {
        expectSafeId(value, "--action");
      } catch (cause) {
        throw new Error(
          `--action must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      actionId = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      throw new Error(
        "respond does not accept --config-root; the durable wait request already binds the actions",
      );
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      throw new Error(
        "respond does not accept --launcher-id; the response command uses no Launcher credential",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "respond does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error("respond does not accept --project; the run-owned project copy already exists");
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error("respond does not accept --input; the run-owned input snapshot already exists");
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error("respond does not accept --workspace; no worker is launched by respond");
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error("respond does not accept --image; no worker is launched by respond");
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error("respond does not accept --profile; no profile is loaded by respond");
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("respond does not accept --task; there is no task input on a response");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "respond does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "respond does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for respond");
  }
  if (waitIndex === null) {
    throw new Error("--wait-index POSITIVE_INTEGER is required for respond");
  }
  if (actionId === null) {
    throw new Error("--action SAFE_ID is required for respond");
  }
  return {
    kind: "respond",
    runId,
    waitIndex,
    actionId,
    json,
  };
}

/**
 * Parses the production pipeline v2 continue-stage intervention command.
 * The user-facing contract: `--run-id` (safe-id validated), `--wait-index`
 * and `--additional-iterations` (both canonical positive decimal safe
 * integers, the latter with no default) and `--config-root` (absolute) are
 * required singletons; `--launcher-id` and `--json` are optional
 * singletons. The intervention action is fixed by the runner entrypoint as
 * `continue_stage`; the caller can never pass an action id, a target
 * state, a stage id, a plan digest, the initial budget, a prepared intent
 * or a compiled plan: every fresh-run, respond and state-root flag is
 * rejected for `continue-stage`, as is every unknown flag or positional
 * argument.
 */
function parseContinueStageArgs(argv: string[]): ParsedPipelineContinueStageArgs {
  let runId: string | null = null;
  let waitIndex: number | null = null;
  let additionalIterations: number | null = null;
  let configRoot: string | null = null;
  let launcherId: string | undefined;
  let json = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      if (runId !== null) {
        throw new Error("--run-id may be given at most once");
      }
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      const { value, next } = parseValue(argv, i, "--wait-index");
      if (waitIndex !== null) {
        throw new Error("--wait-index may be given at most once");
      }
      waitIndex = parsePositiveDecimalSafeInteger(value, "--wait-index");
      i = next;
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      const { value, next } = parseValue(argv, i, "--additional-iterations");
      if (additionalIterations !== null) {
        throw new Error("--additional-iterations may be given at most once");
      }
      additionalIterations = parsePositiveDecimalSafeInteger(value, "--additional-iterations");
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      if (configRoot !== null) {
        throw new Error("--config-root may be given at most once");
      }
      if (!value.startsWith("/")) {
        throw new Error("--config-root must be an absolute path");
      }
      configRoot = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      if (launcherId !== undefined) {
        throw new Error("--launcher-id may be given at most once");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "continue-stage does not accept --action; the intervention action is fixed as continue_stage by the runner entrypoint",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "continue-stage does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error(
        "continue-stage does not accept --project; the run-owned project copy already exists in the run root",
      );
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error(
        "continue-stage does not accept --input; the run-owned input snapshot already exists in the run root",
      );
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error(
        "continue-stage does not accept --workspace; the run-owned project copy is mounted from the run root",
      );
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error(
        "continue-stage does not accept --image; the worker image comes only from the selected profile",
      );
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error(
        "continue-stage does not accept --profile; the execution profiles are selected by the pipeline's agent states",
      );
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error("continue-stage does not accept --task; there is no task input on an intervention");
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "continue-stage does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "continue-stage does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for continue-stage");
  }
  if (waitIndex === null) {
    throw new Error("--wait-index POSITIVE_INTEGER is required for continue-stage");
  }
  if (additionalIterations === null) {
    throw new Error("--additional-iterations POSITIVE_INTEGER is required for continue-stage (no default)");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for continue-stage");
  }
  return {
    kind: "continue-stage",
    runId,
    waitIndex,
    additionalIterations,
    configRoot,
    launcherId,
    json,
  };
}

function defaultWorkspace(): string {
  return process.env.SMOKE_WORKSPACE?.trim() || "/workspace";
}

/**
 * Parses the production pipeline v2 revise-task intervention command. The
 * user-facing contract: `--run-id` and `--task-id` (both safe-id
 * validated), `--wait-index` (canonical positive decimal safe integer) and
 * `--task-file` and `--config-root` (both absolute) are required
 * singletons; `--launcher-id` and `--json` are optional singletons. The
 * intervention action is fixed by the runner entrypoint as `revise_task`
 * (no `--action` flag), and the revised task body is never passed inline
 * or through argv (no `--task-body` flag): the dispatcher reads the file
 * exactly once as UTF-8 text. The caller can never pass a target state, a
 * stage id, a plan digest, the initial budget, a prepared intent, a
 * candidate task revision or a compiled plan: every fresh-run, respond,
 * continue-stage and state-root flag is rejected for `revise-task`, as is
 * every unknown flag or positional argument.
 */
function parseReviseTaskArgs(argv: string[]): ParsedPipelineReviseTaskArgs {
  let runId: string | null = null;
  let waitIndex: number | null = null;
  let taskId: string | null = null;
  let taskFile: string | null = null;
  let configRoot: string | null = null;
  let launcherId: string | undefined;
  let json = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      if (runId !== null) {
        throw new Error("--run-id may be given at most once");
      }
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      const { value, next } = parseValue(argv, i, "--wait-index");
      if (waitIndex !== null) {
        throw new Error("--wait-index may be given at most once");
      }
      waitIndex = parsePositiveDecimalSafeInteger(value, "--wait-index");
      i = next;
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      const { value, next } = parseValue(argv, i, "--task-id");
      if (taskId !== null) {
        throw new Error("--task-id may be given at most once");
      }
      try {
        expectSafeId(value, "--task-id");
      } catch (cause) {
        throw new Error(
          `--task-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      taskId = value;
      i = next;
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      const { value, next } = parseValue(argv, i, "--task-file");
      if (taskFile !== null) {
        throw new Error("--task-file may be given at most once");
      }
      if (value === "") {
        throw new Error("--task-file must be an absolute path");
      }
      if (!value.startsWith("/")) {
        throw new Error("--task-file must be an absolute path");
      }
      taskFile = value;
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      if (configRoot !== null) {
        throw new Error("--config-root may be given at most once");
      }
      if (!value.startsWith("/")) {
        throw new Error("--config-root must be an absolute path");
      }
      configRoot = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      if (launcherId !== undefined) {
        throw new Error("--launcher-id may be given at most once");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "revise-task does not accept --action; the intervention action is fixed as revise_task by the runner entrypoint",
      );
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      throw new Error(
        "revise-task does not accept --task-body; the revised task body is never passed inline or through argv - use --task-file",
      );
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      throw new Error(
        "revise-task does not accept --additional-iterations; the task revision grants no stage iterations",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "revise-task does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error(
        "revise-task does not accept --project; the run-owned project copy already exists in the run root",
      );
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error(
        "revise-task does not accept --input; the run-owned input snapshot already exists in the run root",
      );
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error(
        "revise-task does not accept --workspace; the run-owned project copy is mounted from the run root",
      );
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error(
        "revise-task does not accept --image; the worker image comes only from the selected profile",
      );
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error(
        "revise-task does not accept --profile; the execution profiles are selected by the pipeline's agent states",
      );
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error(
        "revise-task does not accept --task; the revised task body is passed only through --task-file",
      );
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "revise-task does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "revise-task does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      // Every internal policy field (stage/plan/generation/iteration
      // identifiers, digests, intent, candidate, compiled plan, budget)
      // has no CLI flag: anything else is an unknown argument.
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for revise-task");
  }
  if (waitIndex === null) {
    throw new Error("--wait-index POSITIVE_INTEGER is required for revise-task");
  }
  if (taskId === null) {
    throw new Error("--task-id SAFE_ID is required for revise-task");
  }
  if (taskFile === null) {
    throw new Error("--task-file ABSOLUTE_PATH is required for revise-task");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for revise-task");
  }
  return {
    kind: "revise-task",
    runId,
    waitIndex,
    taskId,
    taskFile,
    configRoot,
    launcherId,
    json,
  };
}

/**
 * Parses the production pipeline v2 planning continuation command. The
 * user-facing contract: `--run-id` and `--stage-id` (both safe-id
 * validated), `--initial-budget` (canonical positive decimal safe
 * integer, no default) and `--config-root` (absolute) are required
 * singletons; `--launcher-id` and `--json` are optional singletons.
 * `stageId` and `initialBudget` stay explicit operator policy: the CLI
 * derives neither from the durable state, and no body or file is read.
 * The command is not a `respond` action (no `--action` flag) and accepts
 * no other intervention flag (`--wait-index`,
 * `--additional-iterations`, `--task-id`, `--task-file`, `--task-body`),
 * no fresh-run flag, no state-root flag and no internal proposal/plan/
 * digest/intent/generation/iteration/transition/compiled-stage flag: every
 * internal handoff parameter is derived by the composition controller from
 * the authoritative durable state, and every unknown flag or positional
 * argument is rejected.
 */
function parseResumePlanArgs(argv: string[]): ParsedPipelineResumePlanArgs {
  let runId: string | null = null;
  let stageId: string | null = null;
  let initialBudget: number | null = null;
  let configRoot: string | null = null;
  let launcherId: string | undefined;
  let json = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] ?? "";
    if (arg === "--run-id" || arg.startsWith("--run-id=")) {
      const { value, next } = parseValue(argv, i, "--run-id");
      if (runId !== null) {
        throw new Error("--run-id may be given at most once");
      }
      try {
        expectSafeId(value, "--run-id");
      } catch (cause) {
        throw new Error(
          `--run-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      runId = value;
      i = next;
    } else if (arg === "--stage-id" || arg.startsWith("--stage-id=")) {
      const { value, next } = parseValue(argv, i, "--stage-id");
      if (stageId !== null) {
        throw new Error("--stage-id may be given at most once");
      }
      try {
        expectSafeId(value, "--stage-id");
      } catch (cause) {
        throw new Error(
          `--stage-id must be a safe identifier: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
      }
      stageId = value;
      i = next;
    } else if (arg === "--initial-budget" || arg.startsWith("--initial-budget=")) {
      const { value, next } = parseValue(argv, i, "--initial-budget");
      if (initialBudget !== null) {
        throw new Error("--initial-budget may be given at most once");
      }
      initialBudget = parsePositiveDecimalSafeInteger(value, "--initial-budget");
      i = next;
    } else if (arg === "--config-root" || arg.startsWith("--config-root=")) {
      const { value, next } = parseValue(argv, i, "--config-root");
      if (configRoot !== null) {
        throw new Error("--config-root may be given at most once");
      }
      if (!value.startsWith("/")) {
        throw new Error("--config-root must be an absolute path");
      }
      configRoot = value;
      i = next;
    } else if (arg === "--launcher-id" || arg.startsWith("--launcher-id=")) {
      const { value, next } = parseValue(argv, i, "--launcher-id");
      if (!value.startsWith("dhl_")) {
        throw new Error("--launcher-id must be a launcher ID (dhl_...)");
      }
      if (launcherId !== undefined) {
        throw new Error("--launcher-id may be given at most once");
      }
      launcherId = value;
      i = next;
    } else if (arg === "--json") {
      if (json) {
        throw new Error("--json may be given at most once");
      }
      json = true;
    } else if (arg.startsWith("--json=")) {
      throw new Error("--json does not take a value");
    } else if (arg === "--action" || arg.startsWith("--action=")) {
      throw new Error(
        "resume-plan does not accept --action; the command is not a respond action and accepts no action id",
      );
    } else if (arg === "--wait-index" || arg.startsWith("--wait-index=")) {
      throw new Error(
        "resume-plan does not accept --wait-index; the planning continuation is not a wait response",
      );
    } else if (arg === "--additional-iterations" || arg.startsWith("--additional-iterations=")) {
      throw new Error(
        "resume-plan does not accept --additional-iterations; the planning continuation grants no stage iterations",
      );
    } else if (arg === "--task-id" || arg.startsWith("--task-id=")) {
      throw new Error(
        "resume-plan does not accept --task-id; the planning continuation revises no task",
      );
    } else if (arg === "--task-file" || arg.startsWith("--task-file=")) {
      throw new Error(
        "resume-plan does not accept --task-file; the planning continuation reads no file",
      );
    } else if (arg === "--task-body" || arg.startsWith("--task-body=")) {
      throw new Error(
        "resume-plan does not accept --task-body; the planning continuation accepts no body",
      );
    } else if (arg === "--pipeline-root" || arg.startsWith("--pipeline-root=")) {
      throw new Error(
        "resume-plan does not accept --pipeline-root; the pipeline comes only from the durable state",
      );
    } else if (arg === "--project" || arg.startsWith("--project=")) {
      throw new Error(
        "resume-plan does not accept --project; the run-owned project copy already exists in the run root",
      );
    } else if (arg === "--input" || arg.startsWith("--input=")) {
      throw new Error(
        "resume-plan does not accept --input; the run-owned input snapshot already exists in the run root",
      );
    } else if (arg === "--workspace" || arg.startsWith("--workspace=")) {
      throw new Error(
        "resume-plan does not accept --workspace; the run-owned project copy is mounted from the run root",
      );
    } else if (arg === "--image" || arg.startsWith("--image=")) {
      throw new Error(
        "resume-plan does not accept --image; the worker image comes only from the selected profile",
      );
    } else if (arg === "--profile" || arg.startsWith("--profile=")) {
      throw new Error(
        "resume-plan does not accept --profile; the execution profiles are selected by the pipeline's agent states",
      );
    } else if (arg === "--task" || arg.startsWith("--task=")) {
      throw new Error(
        "resume-plan does not accept --task; there is no task input on a planning continuation",
      );
    } else if (arg === "--state-root" || arg.startsWith("--state-root=")) {
      throw new Error(
        "resume-plan does not accept --state-root; set the ORCHESTRATOR_STATE_ROOT environment variable",
      );
    } else if (arg === "--daemon-state-root" || arg.startsWith("--daemon-state-root=")) {
      throw new Error(
        "resume-plan does not accept --daemon-state-root; set the ORCHESTRATOR_DAEMON_STATE_ROOT environment variable",
      );
    } else {
      // Every internal policy field (proposal/plan/digest/intent/
      // generation/iteration/transition/compiled-stage identifiers) has no
      // CLI flag: anything else is an unknown argument.
      throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (runId === null) {
    throw new Error("--run-id SAFE_ID is required for resume-plan");
  }
  if (stageId === null) {
    throw new Error("--stage-id SAFE_ID is required for resume-plan");
  }
  if (initialBudget === null) {
    throw new Error("--initial-budget POSITIVE_INTEGER is required for resume-plan (no default)");
  }
  if (configRoot === null) {
    throw new Error("--config-root ABSOLUTE_PATH is required for resume-plan");
  }
  return {
    kind: "resume-plan",
    runId,
    stageId,
    initialBudget,
    configRoot,
    launcherId,
    json,
  };
}
