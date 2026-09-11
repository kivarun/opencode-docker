/**
 * Trusted runtime configuration for the pipeline v2 production runner: the
 * state-root projection maps the orchestrator-side state root onto the
 * absolute path by which the Docker Helper daemon sees the same directory.
 *
 * This is deployment configuration, not a per-run flag: the CLI exposes it
 * only through the two environment variables below and never as
 * `--state-root`/`--daemon-state-root` flags. The resolver only validates
 * and joins strings — it creates no filesystem objects and canonicalizes
 * nothing; the real kind/canonical/dev/ino/mode checks live exclusively in
 * `runPipelineV2`. The resolved paths are runtime configuration: they
 * never become pipeline fields and are never handed to an agent worker.
 */
import { assertCleanAbsolutePath } from "./clean_path.ts";
import type { PipelineV2StateRootProjection } from "./pipeline_v2_runner.ts";

function isSet(value: string | undefined): boolean {
  return value !== undefined;
}

/**
 * Resolves the trusted state-root projection from the environment:
 *
 * - the local root is `ORCHESTRATOR_STATE_ROOT` when set, otherwise
 *   `${XDG_STATE_HOME}/orchestrator`, otherwise
 *   `${HOME}/.local/state/orchestrator`;
 * - the daemon root is `ORCHESTRATOR_DAEMON_STATE_ROOT` when set, otherwise
 *   the local root (host mode);
 * - every variable that is present must satisfy the shared clean absolute
 *   path contract (`clean_path.ts`) before any suffix is joined; an
 *   impossible local root is a CLI configuration error. The offending
 *   environment value is never echoed back: the variable name and the
 *   expected shape are enough for the operator.
 */
export function resolvePipelineV2StateRootProjection(
  env: Readonly<Record<string, string | undefined>>,
): PipelineV2StateRootProjection {
  const explicitLocal = env.ORCHESTRATOR_STATE_ROOT;
  let localRoot: string;
  if (isSet(explicitLocal)) {
    localRoot = assertCleanAbsolutePath(explicitLocal!, "ORCHESTRATOR_STATE_ROOT");
  } else {
    const xdgStateHome = env.XDG_STATE_HOME;
    if (isSet(xdgStateHome)) {
      const cleaned = assertCleanAbsolutePath(xdgStateHome!, "XDG_STATE_HOME");
      localRoot = cleaned === "/" ? "/orchestrator" : `${cleaned}/orchestrator`;
    } else {
      const home = env.HOME;
      if (!isSet(home)) {
        throw new Error(
          "cannot build the orchestrator state root: set ORCHESTRATOR_STATE_ROOT (or XDG_STATE_HOME or HOME)",
        );
      }
      const cleaned = assertCleanAbsolutePath(home!, "HOME");
      localRoot = cleaned === "/" ? "/.local/state/orchestrator" : `${cleaned}/.local/state/orchestrator`;
    }
  }
  const explicitDaemon = env.ORCHESTRATOR_DAEMON_STATE_ROOT;
  const daemonRoot = isSet(explicitDaemon)
    ? assertCleanAbsolutePath(explicitDaemon!, "ORCHESTRATOR_DAEMON_STATE_ROOT")
    : localRoot;
  return { localRoot, daemonRoot };
}
