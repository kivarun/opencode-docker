/**
 * Typed surface of the browser UI module (`orchestrator/ui/app.js`).
 *
 * The declaration mirrors exactly the exported runtime surface of the
 * dependency-free ES module; the model suite pins the runtime export
 * keys against this declaration.
 */

export declare class UiContractError extends Error {
  readonly reason: string;
}

export declare function isSafeId(value: unknown): value is string;

export interface UiProjectsModel {
  readonly kind: "projects";
  readonly projects: readonly {
    readonly id: string;
    readonly inputRootIds: readonly string[];
    /** The effective Project Apply capability of the bound project. */
    readonly projectApply: boolean;
  }[];
}

export interface UiRunsEntry {
  readonly runId: string;
  readonly projectId: string;
  readonly projectAvailable: boolean;
}

export interface UiRunsModel {
  readonly kind: "runs";
  readonly runs: readonly UiRunsEntry[];
  readonly nextAfter: string | null;
}

export interface UiRunOutput {
  readonly id: string;
  readonly type: "file" | "directory" | "json";
  readonly required: boolean;
  readonly present: boolean;
  readonly digest: string | null;
}

export interface UiRunModel {
  readonly kind: "run";
  readonly runId: string;
  readonly revision: number;
  readonly status: "running" | "waiting" | "succeeded" | "failed";
  readonly boundary: Record<string, unknown>;
  readonly outputs: readonly UiRunOutput[];
}

export interface UiCommandResultModel {
  readonly kind: "command_result";
  readonly runId: string;
  readonly outcome: "succeeded" | "waiting" | "plan_ready" | "responded" | "failed";
  readonly exitCode: 0 | 1 | 130 | 143;
  readonly reason: string | null;
}

export interface UiErrorModel {
  readonly kind: "error";
  readonly layer: "request" | "registry" | "binding" | "transport";
  readonly reason: string;
}

export type UiApiModel = UiProjectsModel | UiRunsModel | UiRunModel | UiCommandResultModel | UiErrorModel;

export declare function parseApiDocument(document: unknown): UiApiModel;

export interface UiRunsState {
  readonly runs: readonly UiRunsEntry[];
  readonly nextAfter: string | null;
}

export declare function createRunsState(): UiRunsState;
export declare function applyRunsPage(state: UiRunsState, page: UiRunsModel): UiRunsState;
export declare function runsPagePath(nextAfter: string | null): string;
export declare function freshRunPath(projectId: string): string;

export interface UiFreshRunInput {
  readonly id: string;
  readonly rootId: string;
  readonly ref: string;
}

export interface UiFreshRunBody {
  readonly run_id: string;
  readonly inputs: readonly { readonly id: string; readonly root_id: string; readonly ref: string }[];
}

export declare function buildFreshRunBody(projectId: string, runId: string, inputs: readonly UiFreshRunInput[]): UiFreshRunBody;

export interface UiLostResponseRecovery {
  readonly kind: "inspect_only";
  readonly runId: string;
  readonly postAgain: false;
  readonly inspectPath: string;
}

export declare function lostResponseRecovery(runId: string): UiLostResponseRecovery;

export declare function formatFailure(failure: unknown): string;

export interface UiApiFailure {
  readonly layer: string;
  readonly reason: string;
}

export type UiApiResult =
  | { readonly ok: true; readonly lost: false; readonly model: UiApiModel }
  | { readonly ok: false; readonly lost: boolean; readonly failure: UiApiFailure };

export declare function requestApiJson(
  fetchImpl: (path: string, init?: RequestInit) => Promise<Response>,
  path: string,
  init?: RequestInit,
): Promise<UiApiResult>;

/* ------------------------------------------------------------------ */
/* Manual lifecycle controls (v2).                                     */
/* ------------------------------------------------------------------ */

export interface UiReviseTaskControl {
  readonly kind: "revise_task";
  readonly runId: string;
  readonly waitIndex: number;
  readonly projectId: string;
  readonly inputRootIds: readonly string[];
}

export interface UiReviseTaskBlockedControl {
  readonly kind: "revise_task_blocked";
  readonly runId: string;
  readonly waitIndex: number;
  readonly cause: "projects_not_loaded" | "project_missing" | "project_unavailable" | "no_input_roots";
}

export type UiWaitingControl =
  | { readonly kind: "continue_stage"; readonly runId: string; readonly waitIndex: number }
  | UiReviseTaskControl
  | UiReviseTaskBlockedControl;

export type UiRunActionModel =
  | { readonly kind: "resume"; readonly runId: string }
  | { readonly kind: "resume_plan"; readonly runId: string }
  | { readonly kind: "terminal"; readonly runId: string; readonly status: "succeeded" | "failed" }
  | { readonly kind: "observe_only"; readonly runId: string }
  | { readonly kind: "waiting"; readonly runId: string; readonly controls: readonly UiWaitingControl[] }
  | { readonly kind: "unusable_boundary"; readonly runId: string; readonly reason: string };

export interface UiRunActionAuthority {
  readonly projectsModel: UiProjectsModel | null;
  readonly runEntry: UiRunsEntry | null;
}

export declare function buildRunActionModel(runModel: unknown, authority: UiRunActionAuthority | null): UiRunActionModel;

export type UiMutationEndpoint = "resume" | "resume_plan" | "continue_stage" | "revise_task";

export interface UiMutationRequest {
  readonly endpoint: UiMutationEndpoint;
  readonly path: string;
  readonly body: Record<string, unknown>;
}

export declare function buildMutationRequest(action: unknown, formValues: unknown): UiMutationRequest;

export interface UiEndpointResult {
  readonly ok: boolean;
  readonly model?: UiApiModel;
  readonly failure?: UiApiFailure;
}

export declare function expectRunOrError(result: UiApiResult | UiEndpointResult): UiEndpointResult;
export declare function expectDiscoveryModel(result: UiApiResult | UiEndpointResult, kind: "projects" | "runs"): UiEndpointResult;
export declare function expectMutationResult(result: UiApiResult | UiEndpointResult): UiEndpointResult;

export interface UiMutationGate {
  begin(): boolean;
  end(): void;
  isInFlight(): boolean;
}

export declare function createMutationGate(): UiMutationGate;

export interface UiGenerationGuard {
  next(): number;
  isCurrent(generation: number): boolean;
}

export declare function createGenerationGuard(): UiGenerationGuard;

export interface UiMutationFlow {
  readonly send: () => Promise<UiApiResult>;
  readonly inspect: (runId: string) => Promise<UiApiResult>;
  readonly runId: string;
  readonly handlers: {
    onCommandResult(model: UiCommandResultModel): void | Promise<void>;
    onLost(): void | Promise<void>;
    onRecovered(model: UiRunModel): void | Promise<void>;
    onRecoveryUnavailable(presented: UiApiFailure | UiErrorModel): void | Promise<void>;
    onError(presented: UiApiFailure | UiErrorModel): void | Promise<void>;
  };
}

export declare function runMutationFlow(flow: UiMutationFlow): Promise<void>;

/* ------------------------------------------------------------------ */
/* Inspect transport: the one single-flight admission owner of every   */
/* inspect GET.                                                        */
/* ------------------------------------------------------------------ */

export interface UiInspectTransportIo {
  /** The only inspect GET site; receives the request's abort signal. */
  readonly fetchInspect: (runId: string, signal: AbortSignal) => Promise<UiApiResult | undefined>;
  /** Fires exactly once per fetch start, before the request goes out. */
  readonly onInspectStarted: (runId: string) => void;
  /** Fires when a superseded request settles with no desired follow-up. */
  readonly onInspectDropped: (runId: string) => void;
}

export interface UiInspectDesire {
  readonly runId: string;
  /** Called only when the request settled and was never superseded. */
  readonly onApplied: (result: UiApiResult | undefined) => void;
}

export interface UiInspectTransport {
  /**
   * Admits one inspect desire: when the wire slot is free the desire
   * starts immediately, otherwise the active result is superseded and
   * the desire coalesces into the latest desired request. The returned
   * promise resolves with the raw API result after the settlement,
   * whatever the supersede state is.
   */
  request(desire: UiInspectDesire): Promise<UiApiResult | undefined>;
  /** Supersedes the active result and withdraws the pending desire. */
  invalidate(): void;
  /** Withdraws everything and refuses every later desire. */
  dispose(): void;
}

export declare function createInspectTransport(io: UiInspectTransportIo): UiInspectTransport;

/* ------------------------------------------------------------------ */
/* Live observation (v3): bounded read-only polling.                   */
/* ------------------------------------------------------------------ */

export declare const LIVE_OBSERVATION_POLL_DELAY_MS: 2000;

export type UiObservationEffect =
  | { readonly effect: "no_op" }
  | { readonly effect: "cancel_timer" }
  | { readonly effect: "start_inspect" }
  | { readonly effect: "render_state" }
  | { readonly effect: "schedule_timer"; readonly delayMs: number };

export interface UiObservationSnapshot {
  readonly status: string;
  readonly boundaryKind: string;
  readonly revision: number;
}

export interface UiLiveObservationScheduler {
  presentationState(): "off" | "live" | "paused" | "stopped";
  admitInspect(runId: unknown): boolean;
  takeOverInspect(runId: unknown): UiObservationEffect;
  runSelected(runId: unknown): UiObservationEffect;
  inspectStarted(runId: unknown): UiObservationEffect;
  inspectSucceeded(runId: unknown, applied: UiObservationSnapshot): UiObservationEffect;
  inspectFailed(runId: unknown): UiObservationEffect;
  inspectCancelled(runId: unknown): UiObservationEffect;
  timerFired(): UiObservationEffect;
  visibilityChanged(visible: unknown): UiObservationEffect;
  liveToggleChanged(enabled: unknown): UiObservationEffect;
  mutationStarted(): UiObservationEffect;
  mutationCompleted(): UiObservationEffect;
  disposed(): UiObservationEffect;
}

export declare function createLiveObservationScheduler(): UiLiveObservationScheduler;

export interface UiSchedulerTimers {
  readonly setTimeout: (callback: () => void, delayMs: number) => unknown;
  readonly clearTimeout: (handle: unknown) => void;
}

export interface UiSchedulerTimerAdapter {
  apply(effect: unknown): void;
  hasPendingTimer(): boolean;
}

export declare function createSchedulerTimerAdapter(timers: UiSchedulerTimers, onFire: () => void): UiSchedulerTimerAdapter;

export declare function liveObservationDocumentKey(model: unknown): string;
export declare function livePollAnnouncement(previousDocumentKey: unknown, model: unknown): string | null;
export declare function liveStateLabel(state: unknown): string;

/* ------------------------------------------------------------------ */
/* Lifecycle timeline (v4): the read-only events feed of one run.      */
/* ------------------------------------------------------------------ */

/** The fixed page limit of every timeline events request. */
export declare const EVENTS_PAGE_LIMIT: 100;

/** The closed public event-kind vocabulary of pipeline state schema v8. */
export declare const PIPELINE_V2_EVENT_KINDS: readonly string[];

/** The exact reference keys one event kind may carry; null when unknown. */
export declare function eventReferenceKeys(kind: unknown): readonly string[] | null;

export type UiEventEntry = { readonly revision: number; readonly kind: string } & { readonly [reference: string]: unknown };

export interface UiEventsModel {
  readonly kind: "events";
  readonly runId: string;
  readonly revision: number;
  readonly events: readonly UiEventEntry[];
  readonly nextAfter: number | null;
}

/**
 * The one parser of the versioned events document: schema 1, exact own
 * keys at every level, the 28 public kinds with their exact reference
 * shapes, positive-safe-integer revisions, and a run id equal to the
 * selected run. History order is preserved verbatim.
 */
export declare function parseEventsDocument(document: unknown, expectedRunId: string): UiEventsModel;

export type UiEventsEndpointModel = UiEventsModel | UiErrorModel | UiCommandResultModel;

export type UiEventsResult =
  | { readonly ok: true; readonly lost: false; readonly model: UiEventsEndpointModel }
  | { readonly ok: false; readonly lost: false; readonly failure: UiApiFailure };

/**
 * The events endpoint client: accepts only the events document of the
 * expected run (or the endpoint's documented failure envelopes) and
 * never satisfies an inspect, discovery or mutation request.
 */
export declare function requestEventsJson(
  fetchImpl: (path: string, init?: RequestInit) => Promise<Response>,
  path: string,
  init: RequestInit,
  expectedRunId: string,
): Promise<UiEventsResult>;

/** The one URL builder of every timeline events request. */
export declare function eventsRequestPath(runId: string, after: number): string;

export interface UiTimelineState {
  readonly runId: string;
  readonly cursor: number;
  readonly snapshotRevision: number;
  readonly events: readonly UiEventEntry[];
  readonly nextAfter: number | null;
  readonly stopped: boolean;
  readonly error: UiApiFailure | null;
}

export declare function createTimelineState(runId: string): UiTimelineState;

/**
 * The one dense merge: exclusive cursor, first entry `after + 1`,
 * strictly dense revisions, `nextAfter` bound to the last event, and
 * an empty page only when the cursor reached the document revision.
 */
export declare function applyEventsPage(state: UiTimelineState, page: UiEventsModel, requestedAfter: number): UiTimelineState;

export declare function stopTimeline(state: UiTimelineState, failure: UiApiFailure): UiTimelineState;

/** False for every stop reason except the broken cursor contract. */
export declare function timelineRetryAvailable(state: UiTimelineState): boolean;

export interface UiTimelineSummary {
  readonly detailRevision: number | null;
  readonly eventsRevision: number;
  readonly cursor: number;
  readonly caughtUp: boolean;
  readonly hasMore: boolean;
  readonly stopped: boolean;
}

export declare function timelineSummary(state: UiTimelineState, inspectRevision: number | null): UiTimelineSummary;

export declare function timelineAnnouncement(previous: UiTimelineState, next: UiTimelineState): string | null;

export interface UiEventsTransportIo {
  /** The only events GET site; receives the request's abort signal. */
  readonly fetchEvents: (runId: string, after: number, signal: AbortSignal) => Promise<UiEventsResult | undefined>;
  /** Fires exactly once per fetch start, before the request goes out. */
  readonly onStarted: (runId: string) => void;
  /** Fires exactly once per started request's settlement. */
  readonly onSettled: (runId: string) => void;
}

export interface UiEventsDesire {
  readonly runId: string;
  readonly after: number;
  /** Called only when the request settled and was never superseded. */
  readonly onApplied: (result: UiEventsResult | undefined) => void;
}

export interface UiEventsTransport {
  /**
   * Admits one events desire: an idle wire slot starts the fetch
   * immediately; a busy slot refuses the desire without a second
   * fetch. Returns the started flag and the settled promise.
   */
  request(desire: UiEventsDesire): { readonly started: boolean; readonly settled: Promise<UiEventsResult | undefined> };
  /**
   * Supersedes the active result and aborts it best effort; returns
   * the settled promise of the superseded request, or null.
   */
  invalidate(): Promise<UiEventsResult | undefined> | null;
  /** Refuses every later desire and supersedes the active result. */
  dispose(): void;
}

export declare function createEventsTransport(io: UiEventsTransportIo): UiEventsTransport;

/* ------------------------------------------------------------------ */
/* Worker logs (v5): the bounded read-only viewer of the durable       */
/* schema-v9 worker-log artifacts.                                     */
/* ------------------------------------------------------------------ */

export type UiWorkerLogStatus = "pending" | "unavailable" | "captured" | "truncated";

export interface UiWorkerLogMetadata {
  readonly executionIndex: number;
  readonly stateId: string;
  readonly status: UiWorkerLogStatus;
  readonly sha256?: string;
  readonly byteCount?: number;
  readonly truncatedDaemon?: boolean;
  readonly truncatedLocal?: boolean;
}

export interface UiWorkerLogsModel {
  readonly kind: "worker_logs";
  readonly runId: string;
  readonly revision: number;
  readonly logs: readonly UiWorkerLogMetadata[];
}

export interface UiWorkerLogContent {
  readonly executionIndex: number;
  readonly status: "captured" | "truncated";
  readonly sha256: string;
  readonly byteCount: number;
  readonly truncatedDaemon: boolean;
  readonly truncatedLocal: boolean;
  readonly offset: number;
  readonly nextOffset: number | null;
  /** The incrementally decoded text accumulated over every applied chunk. */
  readonly text: string;
}

export interface UiWorkerLogModel {
  readonly kind: "worker_log";
  readonly runId: string;
  readonly revision: number;
  readonly executionIndex: number;
  readonly stateId: string;
  readonly status: UiWorkerLogStatus;
  readonly sha256?: string;
  readonly byteCount?: number;
  readonly truncatedDaemon?: boolean;
  readonly truncatedLocal?: boolean;
  readonly offset?: number;
  readonly dataBase64?: string;
  readonly nextOffset?: number | null;
}

/** The worker-logs list document of one run (metadata union only). */
export declare function parseWorkerLogsDocument(document: unknown, expectedRunId: string): UiWorkerLogsModel;

/** One execution's status document or bounded content chunk document. */
export declare function parseWorkerLogDocument(document: unknown, expectedRunId: string): UiWorkerLogModel;

export type UiWorkerLogsEndpointModel = UiWorkerLogsModel | UiWorkerLogModel | UiErrorModel | UiCommandResultModel;

export type UiWorkerLogsResult =
  | { readonly ok: true; readonly lost: false; readonly model: UiWorkerLogsEndpointModel }
  | { readonly ok: false; readonly lost: false; readonly failure: UiApiFailure };

/**
 * The worker-logs endpoint client: accepts only the two logs documents
 * of the expected run (or the endpoint's documented failure envelopes)
 * and never satisfies an inspect, discovery or mutation request.
 */
export declare function requestWorkerLogsJson(
  fetchImpl: (path: string, init?: RequestInit) => Promise<Response>,
  path: string,
  init: RequestInit,
  expectedRunId: string,
): Promise<UiWorkerLogsResult>;

/** The one URL builder of every worker-logs request. */
export declare function workerLogsRequestPath(
  runId: string,
  executionIndex: number | null,
  offset: number,
  limit: number,
): string;

export interface UiWorkerLogsState {
  readonly runId: string;
  readonly snapshotRevision: number | null;
  readonly logs: readonly UiWorkerLogMetadata[];
  readonly selectedExecutionIndex: number | null;
  readonly content: UiWorkerLogContent | null;
  readonly requestState: "idle" | "loading" | "ready" | "failed";
  readonly error: { readonly layer: string; readonly reason: string } | null;
}

export interface UiLogsTransportIo {
  /** The only logs GET site; the run id comes from the desire. */
  readonly fetchLogs: (runId: string, path: string, signal: AbortSignal) => Promise<UiWorkerLogsResult | undefined>;
  /** Fires exactly once per fetch start, before the request goes out. */
  readonly onStarted: (runId: string) => void;
  /** Fires exactly once per started request's settlement. */
  readonly onSettled: (runId: string) => void;
}

export interface UiLogsDesire {
  readonly runId: string;
  readonly path: string;
  /**
   * The priority kind of the desire: a metadata desire never supersedes
   * an in-flight content request (it queues as the single coalesced
   * follow-up); a content desire supersedes an active metadata refresh.
   */
  readonly kind: "metadata" | "content";
  /** Called only when the request settled and was never superseded. */
  readonly onApplied: (result: UiWorkerLogsResult | undefined) => void;
}

export interface UiLogsTransport {
  /**
   * Admits one logs desire: an idle wire slot starts the fetch; a busy
   * slot supersedes the active request (its response is dropped) and
   * runs this desire as the single follow-up.
   */
  request(desire: UiLogsDesire): Promise<UiWorkerLogsResult | undefined>;
  /** Supersedes the active request and withdraws the queued desire. */
  invalidate(): void;
  /** Refuses every later desire and supersedes the active request. */
  dispose(): void;
}

export declare function createLogsTransport(io: UiLogsTransportIo): UiLogsTransport;

/** The empty logs state of one run selection. */
export declare function createWorkerLogsState(runId: string): UiWorkerLogsState;

export interface UiWorkerLogViewerIo {
  readonly transport: UiLogsTransport;
  /** Publishes the current state after every transition. */
  readonly onState: (state: UiWorkerLogsState | null) => void;
}

export interface UiWorkerLogViewer {
  /** Selection change / reset; discards state and the in-flight GET. */
  transitionToLogsView(runId: string): void;
  /** Leaves the logs view without a run binding. */
  dropView(): void;
  /** The explicit metadata load or refresh; never automatic on its own. */
  fetchLogsList(): void;
  /**
   * The automatic metadata refresh driven by the applied authoritative
   * inspect path: the first successful inspect of the selected run
   * issues exactly one list GET; every later applied inspect issues one
   * only when its revision is strictly greater than the last requested
   * one. Never loads payload bytes.
   */
  inspectApplied(model: unknown): void;
  /** One execution selection; content and the decoder start fresh. */
  selectExecution(index: number): void;
  /** The explicit `Load log` press; any boundary may read a durable log. */
  fetchChunk(): void;
  /** One retry click of the last failed chunk desire. */
  retryLastChunkRequest(): void;
  /** The read-only projection for the DOM adapter. */
  currentState(): UiWorkerLogsState | null;
}

export declare function createWorkerLogViewer(io: UiWorkerLogViewerIo): UiWorkerLogViewer;

/**
 * The one incremental UTF-8 decoder owner of the logs viewer: exactly
 * one non-fatal UTF-8 decoder per selected execution. Every non-final
 * chunk is decoded with `{stream: true}`; the terminal chunk is decoded
 * with `{stream: false}`, which flushes any multi-byte sequence split
 * across a chunk boundary.
 */
export declare interface UiLogChunkDecoder {
  decodeChunk(dataBase64: string, isFinal: boolean): string;
  reset(): void;
}

export declare function createLogChunkDecoder(): UiLogChunkDecoder;

/* ------------------------------------------------------------------ */
/* Project changes (v6): the read-only panel of the run workspace      */
/* changes relative to the durable project baseline.                   */
/* ------------------------------------------------------------------ */

export interface UiProjectChangesFileNode {
  readonly kind: "regular_file";
  readonly mode: number;
  readonly byte_count: number;
  readonly sha256: string;
  readonly multiple_links: boolean;
  readonly hardlink_group?: number;
}

export interface UiProjectChangesDirectoryNode {
  readonly kind: "directory";
  readonly mode: number;
}

export interface UiProjectChangesSymlinkNode {
  readonly kind: "symlink";
  readonly target: string;
}

export type UiProjectChangesNode = UiProjectChangesFileNode | UiProjectChangesDirectoryNode | UiProjectChangesSymlinkNode;

export type UiProjectChangeKind =
  | "added"
  | "deleted"
  | "content_modified"
  | "mode_modified"
  | "content_and_mode_modified"
  | "symlink_target_modified"
  | "type_changed";

export type UiProjectSourceRelation = "matches_baseline" | "matches_result" | "diverged";

export interface UiProjectChangeEntry {
  readonly index: number;
  readonly path: string;
  readonly change: UiProjectChangeKind;
  readonly source_relation: UiProjectSourceRelation;
  readonly baseline: UiProjectChangesNode | null;
  readonly result: UiProjectChangesNode | null;
}

export interface UiProjectChangesSummary {
  readonly total: number;
  readonly added: number;
  readonly deleted: number;
  readonly content_modified: number;
  readonly mode_modified: number;
  readonly content_and_mode_modified: number;
  readonly symlink_target_modified: number;
  readonly type_changed: number;
  readonly matches_baseline: number;
  readonly matches_result: number;
  readonly diverged: number;
}

export interface UiProjectChangesModel {
  readonly kind: "project_changes";
  readonly runId: string;
  readonly revision: number;
  readonly observation: string;
  readonly summary: UiProjectChangesSummary;
  readonly changes: readonly UiProjectChangeEntry[];
  readonly nextAfter: number | null;
}

export declare function parseProjectChangesDocument(document: unknown, expectedRunId?: string): UiProjectChangesModel;

export type UiProjectChangesRequestState =
  | "idle"
  | "loading_initial"
  | "loading_more"
  | "reloading"
  | "ready"
  | "stale"
  | "error";

export type UiProjectChangesPresentation =
  | "unavailable"
  | "no_changes"
  | "pending"
  | "already_present"
  | "mixed"
  | "diverged";

export interface UiProjectChangesState {
  readonly runId: string;
  readonly inspectRevision: number | null;
  readonly observation: string | null;
  readonly summary: UiProjectChangesSummary | null;
  readonly changes: readonly UiProjectChangeEntry[];
  readonly nextAfter: number | null;
  readonly requestState: UiProjectChangesRequestState;
  readonly presentationState: UiProjectChangesPresentation;
  readonly error: UiApiFailure | null;
}

export declare function createProjectChangesState(runId: string): UiProjectChangesState;

export declare function applyProjectChangesPage(
  state: UiProjectChangesState,
  model: UiProjectChangesModel,
  requestedAfter: number,
): UiProjectChangesState;

export interface UiProjectChangesRequestPathOptions {
  readonly runId: string;
  readonly after: number;
  readonly limit: number;
  readonly observation?: string | null;
}

export declare function projectChangesRequestPath(request: UiProjectChangesRequestPathOptions): string;

export type UiProjectChangesResult =
  | { readonly ok: true; readonly lost: false; readonly model: UiProjectChangesModel | UiErrorModel | UiCommandResultModel }
  | { readonly ok: false; readonly lost: boolean; readonly failure: UiApiFailure };

export declare function requestProjectChangesJson(
  fetchImpl: (path: string, init?: RequestInit) => Promise<Response>,
  path: string,
  init?: RequestInit,
  expectedRunId?: string,
): Promise<UiProjectChangesResult>;

export interface UiProjectChangesTransportIo {
  /** The only changes GET site; receives the request's abort signal. */
  readonly fetchChanges: (runId: string, path: string, signal: AbortSignal) => Promise<UiProjectChangesResult | undefined>;
  /** Fires exactly once per fetch start, before the request goes out. */
  readonly onStarted: (runId: string) => void;
  /** Fires when a superseded request settles with no desired follow-up. */
  readonly onSettled: (runId: string) => void;
}

export interface UiProjectChangesDesire {
  readonly runId: string;
  readonly kind: "initial" | "continuation";
  readonly after: number;
  readonly path: string;
  /** Called only when the request settled and was never superseded. */
  readonly onApplied: (result: UiProjectChangesResult | undefined) => void;
}

export interface UiProjectChangesTransport {
  request(desire: UiProjectChangesDesire): Promise<UiProjectChangesResult | undefined>;
  invalidate(): void;
  dispose(): void;
}

export declare function createProjectChangesTransport(io: UiProjectChangesTransportIo): UiProjectChangesTransport;

export interface UiProjectChangesViewerIo {
  readonly transport: UiProjectChangesTransport;
  readonly onState: (state: UiProjectChangesState | null) => void;
}

export interface UiProjectChangesViewer {
  /** Selection change: discards state and invalidates the wire. */
  transitionToRun(runId: string): void;
  /** Leaves the changes view without a run binding. */
  dropView(): void;
  /**
   * The automatic one-shot initial load driven by the applied
   * authoritative terminal inspect: exactly one GET per not-yet-
   * requested terminal revision; every other boundary issues nothing.
   */
  inspectApplied(model: unknown): void;
  /** The explicit Reload: one after=0 request without an observation. */
  reload(): void;
  /** The explicit Load more: one continuation GET with the observation. */
  loadMore(): void;
  /** The read-only projection for the DOM adapter. */
  currentState(): UiProjectChangesState | null;
}

export declare function createProjectChangesViewer(io: UiProjectChangesViewerIo): UiProjectChangesViewer;

/* ------------------------------------------------------------------ */
/* Project change content (v7): bounded read-only content preview and  */
/* client-side bounded diff over the Project Change Content API.       */
/* ------------------------------------------------------------------ */

/** The raw-byte request chunk of one content GET (the API default). */
export declare const CONTENT_PREVIEW_CHUNK_LIMIT: 65536;
/** The maximum raw bytes the viewer retains per side. */
export declare const CONTENT_PREVIEW_MAX_BYTES: 262144;
/** The maximum bytes of one side rendered through the bounded hex view. */
export declare const CONTENT_HEX_DISPLAY_BYTES: 4096;
/** The bounded line-diff limits. */
export declare const DIFF_MAX_LINES_PER_SIDE: 2000;
export declare const DIFF_MAX_WORK_CELLS: 250000;

export interface UiProjectChangeContentBinding {
  readonly runId: string;
  readonly observation: string;
  readonly changeIndex: number;
  readonly side: string;
}

export interface UiProjectChangeContentModelCommon {
  readonly kind: "project_change_content";
  readonly runId: string;
  readonly revision: number;
  readonly observation: string;
  readonly changeIndex: number;
  readonly side: string;
}

export type UiProjectChangeContentModel =
  | (UiProjectChangeContentModelCommon & {
      readonly status: "available";
      readonly byteCount: number;
      readonly sha256: string;
      readonly offset: number;
      readonly data: Uint8Array;
      readonly nextOffset: number | null;
    })
  | (UiProjectChangeContentModelCommon & {
      readonly status: "implicit_empty";
      readonly byteCount: 0;
      readonly offset: 0;
      readonly data: Uint8Array;
      readonly nextOffset: null;
    })
  | (UiProjectChangeContentModelCommon & {
      readonly status: "metadata_only";
      readonly node: Record<string, unknown>;
    })
  | (UiProjectChangeContentModelCommon & {
      readonly status: "unavailable";
      readonly reason: string;
      readonly node: Record<string, unknown>;
    });

export declare function parseProjectChangeContentDocument(document: unknown, expected: UiProjectChangeContentBinding | null): UiProjectChangeContentModel;

export type UiProjectChangeContentResult =
  | { readonly ok: true; readonly lost: false; readonly model: UiProjectChangeContentModel }
  | { readonly ok: false; readonly lost: boolean; readonly failure: { readonly layer: string; readonly reason: string } };

export declare function requestProjectChangeContentJson(fetchImpl: (path: string, init?: RequestInit) => Promise<Response>, path: string, init: RequestInit | undefined, expected: UiProjectChangeContentBinding | null): Promise<UiProjectChangeContentResult>;

export interface UiProjectChangeContentPathRequest {
  readonly runId: string;
  readonly changeIndex: number;
  readonly side: string;
  readonly observation: string;
  readonly offset?: number;
  readonly limit?: number;
}

export declare function projectChangeContentRequestPath(request: UiProjectChangeContentPathRequest): string;

export interface UiProjectChangeContentTransportIo {
  readonly fetchContent: (runId: string, path: string, signal: AbortSignal, expected: UiProjectChangeContentBinding) => Promise<UiProjectChangeContentResult | undefined>;
  readonly onStarted: (runId: string) => void;
  readonly onSettled: (runId: string) => void;
}

export interface UiProjectChangeContentDesire {
  readonly runId: string;
  readonly kind: "initial" | "continuation";
  readonly side: string;
  readonly expected: UiProjectChangeContentBinding;
  readonly path: string;
  readonly onApplied: (result: UiProjectChangeContentResult | undefined) => void;
}

export interface UiProjectChangeContentTransport {
  request(desire: UiProjectChangeContentDesire): Promise<UiProjectChangeContentResult | undefined>;
  invalidate(): void;
  dispose(): void;
}

export declare function createProjectChangeContentTransport(io: UiProjectChangeContentTransportIo): UiProjectChangeContentTransport;

export interface UiProjectChangeContentViewerIo {
  readonly transport: UiProjectChangeContentTransport;
  readonly onState: (state: UiProjectChangeContentState | null) => void;
}

export interface UiProjectChangeContentSideState {
  readonly phase: "unrequested" | "loading" | "available" | "implicit_empty" | "metadata_only" | "unavailable" | "error";
  readonly byteCount: number | null;
  readonly sha256: string | null;
  readonly loadedBytes: number;
  readonly nextOffset: number | null;
  readonly capped: boolean;
  readonly data: Uint8Array | null;
  readonly error: { readonly layer: string; readonly reason: string } | null;
}

export interface UiProjectChangeContentState {
  readonly runId: string;
  readonly observation: string | null;
  readonly changesCount: number;
  readonly changeIndex: number | null;
  readonly changePath: string | null;
  readonly changeKind: string | null;
  readonly generation: number;
  readonly requestState: "idle" | "loading" | "ready" | "stale" | "error";
  readonly stale: boolean;
  readonly error: { readonly layer: string; readonly reason: string } | null;
  readonly baseline: UiProjectChangeContentSideState;
  readonly result: UiProjectChangeContentSideState;
}

export interface UiProjectChangeContentViewer {
  review(entry: unknown): void;
  loadMoreSide(side: string): void;
  retrySide(side: string): void;
  closeReview(): void;
  invalidateMetadata(): void;
  transitionToRun(runId: string): void;
  metadataApplied(changesState: unknown): void;
  currentState(): UiProjectChangeContentState | null;
}

export declare function createProjectChangeContentViewer(io: UiProjectChangeContentViewerIo): UiProjectChangeContentViewer;

export declare function decodePreviewText(bytes: Uint8Array, options: { readonly incompleteTail?: boolean } | null): { readonly kind: "text"; readonly text: string } | { readonly kind: "binary" };

export interface UiHexPreviewRow {
  readonly offset: string;
  readonly hex: string;
  readonly ascii: string;
}

export declare function hexPreviewRows(bytes: Uint8Array, maxBytes?: number): readonly UiHexPreviewRow[];

export type UiBoundedLineDiff =
  | { readonly kind: "diff"; readonly identical: boolean; readonly rows: readonly { readonly type: "context" | "add" | "delete"; readonly text: string; readonly noEol: boolean }[] }
  | { readonly kind: "refusal"; readonly reason: "not_text" | "too_many_lines" | "work_budget" };

export declare function computeBoundedLineDiff(leftBytes: Uint8Array, rightBytes: Uint8Array): UiBoundedLineDiff;

/* --- Project apply (v8): explicit durable Project Apply. ------------- */

export declare const PROJECT_APPLY_OPERATION_STATUSES: readonly string[];

export declare const PROJECT_APPLY_UI_STATES: readonly string[];

export interface UiProjectApplyOperationModel {
  readonly kind: "project_apply_operation";
  readonly runId: string;
  readonly applyId: string;
  readonly projectId: string;
  readonly observation: string;
  readonly revision: number;
  readonly status: string;
  readonly completedStepCount: number;
  readonly totalStepCount: number;
}

export declare function parseProjectApplyOperationDocument(
  document: unknown,
  expected: { readonly runId: string; readonly applyId: string; readonly observation: string },
): UiProjectApplyOperationModel;

export declare function requestProjectApplyJson(
  fetchImpl: ((path: string, init?: RequestInit) => Promise<Response>) | null,
  path: string,
  init: RequestInit | undefined,
  expected: { readonly runId: string; readonly applyId: string; readonly observation: string },
): Promise<UiEndpointResult>;

export declare function projectApplyRequestPath(runId: string, applyId: string): string;

export declare function projectApplyPostBody(observation: string): { readonly schema_version: 1; readonly observation: string };

export declare const PROJECT_APPLY_INELIGIBILITY_REASONS: readonly string[];

export interface UiProjectApplyEligibility {
  readonly eligible: boolean;
  readonly reason: string | null;
}

export declare function projectApplyEligibility(options: {
  readonly terminalStatus: unknown;
  readonly capability: unknown;
  readonly changesState: unknown;
  readonly applyState: unknown;
  readonly mutationInFlight: unknown;
}): UiProjectApplyEligibility;

export declare const APPLY_RECORD_STORAGE_PREFIX: string;

export interface UiApplyRecoveryRecord {
  readonly runId: string;
  readonly applyId: string;
  readonly observation: string;
}

export declare function parseApplyRecoveryRecord(document: unknown): UiApplyRecoveryRecord;

export interface UiApplyRecoveryStore {
  load(runId: string): { readonly record: UiApplyRecoveryRecord | null; readonly error: string | null };
  save(runId: string, observation: string): { readonly saved: boolean; readonly error: string | null };
  remove(runId: string): boolean;
}

export declare function createApplyRecoveryStore(storage: unknown): UiApplyRecoveryStore;

export interface UiProjectApplyTransport {
  request(desire: {
    readonly runId: string;
    readonly applyId: string;
    readonly observation: string;
    readonly onSettled?: (settlement: { readonly fulfilled: boolean; readonly superseded: boolean; readonly result?: unknown; readonly error?: unknown }) => void;
  }): void;
  invalidate(): void;
  dispose(): void;
}

export declare function createProjectApplyTransport(io: {
  readonly fetchOperation: (
    runId: string,
    applyId: string,
    observation: string,
    signal: AbortSignal | undefined,
  ) => Promise<unknown>;
}): UiProjectApplyTransport;

export interface UiProjectApplyState {
  readonly runId: string;
  readonly uiState: string;
  readonly applyId: string | null;
  readonly observation: string | null;
  readonly revision: number | null;
  readonly completedSteps: number | null;
  readonly totalSteps: number | null;
  readonly error: { readonly layer: string; readonly reason: string } | null;
  readonly recordError: string | null;
  readonly recoveryIssued: boolean;
  readonly submitKind: string | null;
}

export interface UiProjectApplyViewer {
  transitionToRun(runId: string): void;
  dropView(): void;
  changesSnapshotReady(changesState: unknown): void;
  review(): void;
  cancel(): void;
  confirm(observation: string): void;
  retry(): void;
  continueRecovery(): void;
  checkStatus(): void;
  currentState(): UiProjectApplyState | null;
}

export declare function createProjectApplyViewer(io: {
  readonly recoveryStore: UiApplyRecoveryStore;
  readonly transport: UiProjectApplyTransport;
  readonly submit: (identity: { readonly runId: string; readonly applyId: string; readonly observation: string }) => Promise<unknown>;
  readonly onState: (state: UiProjectApplyState | null) => void;
  readonly onApplied?: (identity: { readonly runId: string; readonly applyId: string; readonly observation: string }) => void;
}): UiProjectApplyViewer;
