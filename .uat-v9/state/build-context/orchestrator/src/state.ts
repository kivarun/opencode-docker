import { chmod, mkdir, writeFile } from "node:fs/promises";
import { describeError } from "./docker_helper.ts";

interface EnvLike {
  readonly [key: string]: string | undefined;
}

export interface RunState {
  schema_version: number;
  run_id: string;
  workspace: string;
  worker_image: string;
  started_at: string;
  updated_at: string;
  session_id?: string;
  status: string;
}

export class StateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StateError";
  }
}

export function stateDir(env: EnvLike): string {
  const xdgState = env.XDG_STATE_HOME?.trim() || "";
  const base =
    xdgState !== ""
      ? xdgState.replace(/\/+$/, "")
      : `${(env.HOME || "").replace(/\/+$/, "")}/.local/state`;
  return `${base}/orchestrator`;
}

export function stateFileFor(stateDirPath: string, runId: string): string {
  return `${stateDirPath}/smoke-${runId}.json`;
}

export async function saveRunState(
  stateDirPath: string,
  state: RunState,
): Promise<void> {
  const file = stateFileFor(stateDirPath, state.run_id);
  try {
    await mkdir(stateDirPath, { recursive: true, mode: 0o700 });
    await writeFile(file, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
    await chmod(file, 0o600);
  } catch (cause) {
    throw new StateError(
      `cannot persist run state at ${file}: ${describeError(cause)}`,
    );
  }
}
