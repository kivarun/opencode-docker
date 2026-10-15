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
