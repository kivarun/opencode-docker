import { AgentResultError } from "./agent_result.ts";
import { DockerHelperError } from "./docker_helper.ts";
import { PipelineError } from "./pipeline.ts";
import { PipelineExecutionError } from "./pipeline_engine.ts";
import { PipelineStateError } from "./pipeline_state.ts";
import { PipelineStateStoreError } from "./pipeline_state_store.ts";
import { StatePersistError } from "./lifecycle.ts";
import type { FailureReason } from "./pipeline_state.ts";
import type { SignalAbort } from "./lifecycle.ts";

/**
 * Failure types of the multi-state agent run and their mapping to the
 * normalized, text-free failure reasons of the durable pipeline run state.
 */

export class AgentTimeoutError extends Error {
  readonly seconds: number;
  constructor(seconds: number) {
    super(`agent container timed out after ${seconds} seconds`);
    this.name = "AgentTimeoutError";
    this.seconds = seconds;
  }
}

export class WorkspaceInputError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WorkspaceInputError";
  }
}

/**
 * A runtime input declared by an agent state is missing or invalid at
 * activation time. Unprotected inputs may be produced by an earlier state of
 * the same run; a missing one fails the pipeline before the consuming
 * state's Session is created.
 */
export class RuntimeInputError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RuntimeInputError";
  }
}

/**
 * Maps the terminal cause of a run to one normalized failure reason. A
 * recorded signal always wins (the lifecycle records it as the run's cause);
 * everything unrecognized is `internal_error`.
 */
export function classifyRunFailure(
  failure: Error | null,
  signal: SignalAbort | null,
): FailureReason {
  if (signal !== null) {
    return signal.signal === "SIGINT" ? "signal_sigint" : "signal_sigterm";
  }
  if (failure === null) {
    return "internal_error";
  }
  if (failure instanceof AgentTimeoutError) {
    return "worker_timeout";
  }
  if (
    failure instanceof PipelineStateStoreError ||
    failure instanceof PipelineStateError ||
    failure instanceof StatePersistError
  ) {
    return "state_persist_failed";
  }
  if (failure instanceof DockerHelperError) {
    return failure.kind === "cli_failure" ? "worker_failed" : "internal_error";
  }
  if (failure instanceof AgentResultError) {
    return "agent_result_invalid";
  }
  if (failure instanceof WorkspaceInputError) {
    return "protected_input_modified";
  }
  if (failure instanceof RuntimeInputError) {
    return "runtime_input_missing";
  }
  if (failure instanceof PipelineExecutionError) {
    switch (failure.reason) {
      case "unknown_outcome":
        return "unknown_outcome";
      case "invalid_outcome":
        return "invalid_outcome";
      case "transition_budget_exhausted":
        return "transition_budget_exhausted";
      case "invalid_graph":
        return "invalid_graph";
      case "missing_state":
        return "missing_state";
    }
  }
  if (failure instanceof PipelineError) {
    return "execution_failed";
  }
  return "internal_error";
}
