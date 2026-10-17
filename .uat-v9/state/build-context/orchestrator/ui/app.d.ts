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
