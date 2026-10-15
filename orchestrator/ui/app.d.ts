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
