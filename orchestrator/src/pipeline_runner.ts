import { constants as fsConstants, lstat, mkdir, open as fsOpen, realpath, stat } from "node:fs/promises";
import { isAbsolute, join, relative, resolve } from "node:path";
import {
  AgentResultError,
  verifyAgentResult,
  type ProtectedInputRef,
} from "./agent_result.ts";
import { DockerHelperError, describeError } from "./docker_helper.ts";
import {
  createChildSession,
  deleteChildSession,
} from "./launcher.ts";
import { PipelineError, type MultiStatePlan } from "./pipeline.ts";
import {
  executePipelineGraph,
  type AgentStateView,
} from "./pipeline_engine.ts";
import {
  PipelineRunStateSink,
  type AcceptedAgentResultRecord,
  type PipelineRunSink,
} from "./pipeline_state_sink.ts";
import {
  SESSION_CLEANUP_FAILURE_REASON,
  type PipelineIdentityState,
  type ProtectedInputState,
} from "./pipeline_state.ts";
import type { PipelineStateIo } from "./pipeline_state_store.ts";
import {
  AgentTimeoutError,
  classifyRunFailure,
  ControlPathError,
  RuntimeInputError,
  WorkspaceInputError,
} from "./run_errors.ts";
import type { ResolvedProfile } from "./profile.ts";
import {
  AGENT_SMOKE_DIR,
  agentWorkerSpec,
  pullArgs,
  runArgs,
} from "./worker.ts";
import {
  RunCauseGate,
  StatePersistError,
  SignalAbort,
  childSessionEnv,
  defaultWorkspaceExists,
  lifecycleAuthority,
  runOutcomeExitCode,
  type LifecycleDeps,
  type LifecycleOutcome,
} from "./lifecycle.ts";
import { stateDir } from "./state.ts";

export { AgentTimeoutError, WorkspaceInputError } from "./run_errors.ts";

/**
 * Multi-state pipeline execution substrate for `agent-smoke`.
 *
 * The graph engine (`executePipelineGraph`) remains the single owner of the
 * outcome -> transition -> next-state mapping, the transition budget, and the
 * terminal selection; the runner owns only launching the agent: one child
 * Session per agent-state activation, one worker run with that state's
 * timeout, result verification, and the durable activation bookkeeping. The
 * durable transition hook runs before the engine starts the next activation.
 *
 * The run-level signal/finalization semantics are the shared
 * `RunCauseGate`: signals are recorded while acceptance is open, every
 * activation's Session is deleted exactly once (also on failure and on
 * signals), a cleanup failure stops the pipeline with its usual priority,
 * and the single authoritative final state write happens after the
 * run-level cutoff.
 */

export interface AgentSmokeOptions {
  workspace: string;
  configRoot: string;
  pipelineRoot: string;
  launcherId?: string;
}

export type AgentSmokeDeps = LifecycleDeps & {
  /** IO seam for the durable pipeline run state (tests). */
  pipelineStateIo?: PipelineStateIo;
};

export type AgentSmokeOutcome = LifecycleOutcome;

export function agentRunDirPath(workspace: string, runId: string): string {
  return `${workspace.replace(/\/+$/, "")}/${AGENT_SMOKE_DIR}/${runId}`;
}

/** Separate directory per activation: revisits never share results. */
export function activationDirPath(
  workspace: string,
  runId: string,
  activationIndex: number,
  stateId: string,
): string {
  return `${agentRunDirPath(workspace, runId)}/activations/${activationIndex}-${stateId}/attempt-1`;
}

export function activationResultFilePath(
  workspace: string,
  runId: string,
  activationIndex: number,
  stateId: string,
): string {
  return `${activationDirPath(workspace, runId, activationIndex, stateId)}/result.json`;
}

export function activationExecutionDocPath(
  workspace: string,
  runId: string,
  activationIndex: number,
  stateId: string,
): string {
  return `${activationDirPath(workspace, runId, activationIndex, stateId)}/execution.md`;
}

export interface ResolvedWorkspaceInput {
  workspaceCanonical: string;
  canonical: string;
  pathInWorkspace: string;
  /** The declared input path (verbatim, never rewritten). */
  declaredPath: string;
  sha256: string;
  dev: number;
  ino: number;
}

export interface ResolvedProtectedInput extends ResolvedWorkspaceInput {
  id: string;
}

async function sha256File(path: string): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(await Bun.file(path).arrayBuffer());
  return hasher.digest("hex");
}

/**
 * Resolves one workspace-relative input against the canonical workspace:
 * regular file, contained inside the workspace after realpath, readable.
 */
async function checkWorkspaceInput(
  workspace: string,
  workspaceCanonical: string,
  inputPath: string,
): Promise<{ canonical: string; dev: number; ino: number }> {
  if (isAbsolute(inputPath)) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} must be a workspace-relative path`,
    );
  }
  const candidate = resolve(workspace, inputPath);
  let info;
  try {
    info = await stat(candidate);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not accessible: ${describeError(cause)}`,
    );
  }
  if (!info.isFile()) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not a regular file`,
    );
  }
  let canonical: string;
  try {
    canonical = await realpath(candidate);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
  if (
    canonical !== workspaceCanonical &&
    !canonical.startsWith(`${workspaceCanonical}/`)
  ) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} resolves outside workspace ${workspace}`,
    );
  }
  const pathInWorkspace = relative(workspaceCanonical, canonical);
  if (pathInWorkspace === "" || pathInWorkspace.startsWith("..")) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not a file inside workspace ${workspace}`,
    );
  }
  return { canonical, dev: info.dev, ino: info.ino };
}

export async function resolveWorkspaceInput(
  workspace: string,
  inputPath: string,
  workspaceCanonicalHint?: string,
): Promise<ResolvedWorkspaceInput> {
  const workspaceCanonical = workspaceCanonicalHint ?? (await canonicalWorkspacePath(workspace));
  const checked = await checkWorkspaceInput(workspace, workspaceCanonical, inputPath);
  let sha256: string;
  try {
    sha256 = await sha256File(checked.canonical);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace input ${inputPath} is not readable: ${describeError(cause)}`,
    );
  }
  return {
    workspaceCanonical,
    canonical: checked.canonical,
    pathInWorkspace: relative(workspaceCanonical, checked.canonical),
    declaredPath: inputPath,
    sha256,
    dev: checked.dev,
    ino: checked.ino,
  };
}

export async function canonicalWorkspacePath(
  workspace: string,
): Promise<string> {
  try {
    return await realpath(workspace);
  } catch (cause) {
    throw new WorkspaceInputError(
      `workspace ${workspace} cannot be canonicalized: ${describeError(cause)}`,
    );
  }
}

async function lstatOrNull(path: string): Promise<import("node:fs").Stats | null> {
  try {
    return await lstat(path);
  } catch (cause) {
    if (
      typeof cause === "object" &&
      cause !== null &&
      (cause as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return null;
    }
    throw new ControlPathError(
      `orchestrator control path ${path} cannot be inspected: ${describeError(cause)}`,
    );
  }
}

async function ensureControlDir(
  path: string,
  description: string,
): Promise<void> {
  const info = await lstatOrNull(path);
  if (info === null) {
    try {
      await mkdir(path, { mode: 0o700 });
    } catch (cause) {
      throw new ControlPathError(
        `orchestrator control directory ${path} (${description}) could not be created: ${describeError(cause)}`,
      );
    }
    return;
  }
  if (info.isSymbolicLink() || !info.isDirectory()) {
    throw new ControlPathError(
      `orchestrator control path ${path} (${description}) exists but is ${info.isSymbolicLink() ? "a symbolic link" : "not a directory"}`,
    );
  }
}

/**
 * Prepares the activation control tree for one activation, fail-closed:
 * every parent component is an existing (or freshly created) real directory
 * without symlinks, and both the activation leaf and the attempt leaf must
 * be absent — an existing leaf, symlink, or unexpected object is rejected
 * before any Session is created. The execution document is then created with
 * O_EXCL|O_NOFOLLOW, so a pre-placed file or symlink can never be followed
 * or overwritten.
 */
export async function prepareActivationControlTree(params: {
  workspaceCanonical: string;
  runId: string;
  activationIndex: number;
  stateId: string;
  executionDocument: string;
}): Promise<string> {
  const { workspaceCanonical, runId, activationIndex, stateId } = params;
  const root = join(workspaceCanonical, AGENT_SMOKE_DIR);
  const runDir = join(root, runId);
  const activationsDir = join(runDir, "activations");
  const activationLeaf = join(activationsDir, `${activationIndex}-${stateId}`);
  const attemptLeaf = join(activationLeaf, "attempt-1");
  const docPath = join(attemptLeaf, "execution.md");

  await ensureControlDir(root, `run state root of run ${runId}`);
  await ensureControlDir(runDir, `run directory of run ${runId}`);
  await ensureControlDir(activationsDir, `activations directory of run ${runId}`);

  for (const absent of [activationLeaf, attemptLeaf]) {
    const info = await lstatOrNull(absent);
    if (info !== null) {
      throw new ControlPathError(
        `activation leaf ${absent} must not exist before the activation starts, found ${info.isSymbolicLink() ? "a symbolic link" : info.isDirectory() ? "an existing directory" : "an unexpected object"}`,
      );
    }
  }

  try {
    await mkdir(activationLeaf, { mode: 0o700 });
  } catch (cause) {
    throw new ControlPathError(
      `activation leaf ${activationLeaf} could not be created as a new directory: ${describeError(cause)}`,
    );
  }
  try {
    await mkdir(attemptLeaf, { mode: 0o700 });
  } catch (cause) {
    throw new ControlPathError(
      `attempt leaf ${attemptLeaf} could not be created as a new directory: ${describeError(cause)}`,
    );
  }

  let handle;
  try {
    handle = await fsOpen(
      docPath,
      fsConstants.O_WRONLY | fsConstants.O_CREAT | fsConstants.O_EXCL | fsConstants.O_NOFOLLOW,
      0o600,
    );
  } catch (cause) {
    throw new ControlPathError(
      `execution document ${docPath} could not be created exclusively: ${describeError(cause)}`,
    );
  }
  try {
    await handle.write(Buffer.from(params.executionDocument, "utf8"));
  } finally {
    await handle.close();
  }

  // the whole control tree must still resolve inside the canonical workspace
  const rootCanonical = await realpath(root).catch((cause: unknown) => {
    throw new ControlPathError(
      `orchestrator control directory ${root} cannot be canonicalized: ${describeError(cause)}`,
    );
  });
  if (
    rootCanonical !== workspaceCanonical &&
    !rootCanonical.startsWith(`${workspaceCanonical}/`)
  ) {
    throw new ControlPathError(
      `orchestrator control directory ${root} resolves outside the canonical workspace ${workspaceCanonical}`,
    );
  }
  const attemptCanonical = await realpath(attemptLeaf).catch((cause: unknown) => {
    throw new ControlPathError(
      `activation leaf ${attemptLeaf} cannot be canonicalized: ${describeError(cause)}`,
    );
  });
  if (attemptCanonical !== attemptLeaf) {
    throw new ControlPathError(
      `activation leaf ${attemptLeaf} does not match its canonical path ${attemptCanonical}`,
    );
  }
  return attemptLeaf;
}

/**
 * The expected result file must not exist before the worker starts: the
 * result of this activation can only be produced by the worker run itself.
 */
export async function checkResultFileAbsent(resultPath: string): Promise<void> {
  const info = await lstatOrNull(resultPath);
  if (info !== null) {
    throw new ControlPathError(
      `expected result file ${resultPath} already exists before the agent run, found ${info.isSymbolicLink() ? "a symbolic link" : info.isDirectory() ? "a directory" : "an unexpected object"}`,
    );
  }
}

/**
 * Reads the activation's result file fail-closed: the path itself must be a
 * regular non-symlink file inside the exact activation leaf (realpath
 * equality, no follow), and the bytes are read through an O_NOFOLLOW file
 * descriptor. A missing file is a missing agent result; anything else about
 * the control path is a control-path violation.
 */
export async function readResultFileNoFollow(resultPath: string): Promise<Uint8Array> {
  const activationDir = resolve(resultPath, "..");
  const attemptCanonical = await realpath(activationDir).catch((cause: unknown) => {
    throw new ControlPathError(
      `activation leaf ${activationDir} cannot be canonicalized: ${describeError(cause)}`,
    );
  });
  if (attemptCanonical !== activationDir) {
    throw new ControlPathError(
      `activation leaf ${activationDir} does not match its canonical path ${attemptCanonical}`,
    );
  }
  let info = await lstatOrNull(resultPath);
  if (info === null) {
    throw new AgentResultError(`agent result not readable at ${resultPath}: no such file`);
  }
  if (info.isSymbolicLink() || !info.isFile()) {
    throw new ControlPathError(
      `expected result file ${resultPath} is ${info.isSymbolicLink() ? "a symbolic link" : "not a regular file"}`,
    );
  }
  let handle;
  try {
    handle = await fsOpen(resultPath, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  } catch (cause) {
    throw new ControlPathError(
      `expected result file ${resultPath} cannot be opened without following symlinks: ${describeError(cause)}`,
    );
  }
  try {
    const opened = await handle.stat();
    if (opened.isSymbolicLink?.() === true || !opened.isFile()) {
      throw new ControlPathError(
        `expected result file ${resultPath} is not a regular file`,
      );
    }
    return new Uint8Array(await handle.readFile());
  } finally {
    await handle.close();
  }
}

/**
 * Re-resolves every declared protected input path against the canonical
 * workspace and compares the full filesystem identity — canonical target,
 * device, inode, and content digest — against the recorded baseline. An
 * unchanged in-workspace symlink still passes; a retargeted symlink, a
 * replaced inode, a changed file type, a disappearance, or a content change
 * fails as `protected_input_modified`.
 */
export async function verifyProtectedInputs(
  workspaceCanonical: string,
  baselines: readonly ResolvedProtectedInput[],
  when: string,
): Promise<void> {
  for (const baseline of baselines) {
    const declared = baseline.declaredPath;
    const candidate = resolve(workspaceCanonical, declared);
    let info;
    try {
      info = await stat(candidate);
    } catch (cause) {
      throw new WorkspaceInputError(
        `protected input ${declared} disappeared ${when}: ${describeError(cause)}`,
      );
    }
    if (!info.isFile()) {
      throw new WorkspaceInputError(
        `protected input ${declared} is no longer a regular file ${when}`,
      );
    }
    let canonical: string;
    try {
      canonical = await realpath(candidate);
    } catch (cause) {
      throw new WorkspaceInputError(
        `protected input ${declared} cannot be canonicalized ${when}: ${describeError(cause)}`,
      );
    }
    if (canonical !== baseline.canonical) {
      throw new WorkspaceInputError(
        `protected input ${declared} now resolves to ${canonical} instead of the recorded target ${baseline.canonical} ${when}`,
      );
    }
    if (info.dev !== baseline.dev || info.ino !== baseline.ino) {
      throw new WorkspaceInputError(
        `protected input ${declared} was replaced with a different file (device/inode ${baseline.dev}/${baseline.ino} -> ${info.dev}/${info.ino}) ${when}`,
      );
    }
    let sha256: string;
    try {
      sha256 = await sha256File(canonical);
    } catch (cause) {
      throw new WorkspaceInputError(
        `protected input ${declared} cannot be re-read ${when}: ${describeError(cause)}`,
      );
    }
    if (sha256 !== baseline.sha256) {
      throw new WorkspaceInputError(
        `protected input ${declared} was modified ${when} (sha256 ${baseline.sha256} -> ${sha256})`,
      );
    }
  }
}

export function executionDocument(params: {
  runId: string;
  stateId: string;
  activationIndex: number;
  attempt: number;
  inputs: readonly { id: string; path: string }[];
  resultPathInWorkspace: string;
  allowedOutcome: string;
  promptContent: string;
}): string {
  const lines = [
    "# Execution document",
    "",
    `- run_id: ${params.runId}`,
    `- state: ${params.stateId}`,
    `- activation_index: ${params.activationIndex}`,
    `- attempt: ${params.attempt}`,
    `- result (workspace-relative): ${params.resultPathInWorkspace}`,
    `- allowed outcome: ${params.allowedOutcome}`,
    "",
    "## Inputs",
    "",
  ];
  if (params.inputs.length === 0) {
    lines.push("- none");
  } else {
    for (const input of params.inputs) {
      lines.push(`- ${input.id} (workspace-relative): ${input.path}`);
    }
  }
  lines.push(
    "",
    "## Agent instruction",
    "",
    params.promptContent.trimEnd(),
    "",
    "## Required result format",
    "",
    "When the work described above is actually finished, write the result file to",
    "the result path above. The result file must contain a single JSON object with",
    "exactly these fields and no others:",
    "",
    `{"schema_version":2,"run_id":"${params.runId}","state_id":"${params.stateId}","activation_index":${params.activationIndex},"attempt":${params.attempt},"status":"${params.allowedOutcome}","summary":"<one sentence describing the work performed>","artifacts":["<workspace-relative paths of files the task created>"]}`,
    "",
    "- schema_version: exactly 2",
    "- run_id: the run_id from this document, verbatim",
    "- state_id: the state from this document, verbatim",
    "- activation_index: the activation_index from this document, verbatim",
    "- attempt: the attempt from this document, verbatim",
    `- status: exactly ${JSON.stringify(params.allowedOutcome)}`,
    "- summary: a non-empty one-sentence description of the work performed",
    "- artifacts: workspace-relative paths of files the task created; never",
    "  absolute paths, never paths outside the workspace, never the input file",
    "",
    "The result file must contain valid JSON and nothing else. If the work cannot",
    "be completed, do not write a conforming result; explain what is missing in",
    "your normal output.",
  );
  return lines.join("\n");
}

export interface MultiStateRunParams {
  plan: MultiStatePlan;
  /** Every referenced profile, loaded and validated before Launcher auth. */
  profiles: ReadonlyMap<string, ResolvedProfile>;
  /** Canonical workspace path (the identity recorded in the run state). */
  workspaceCanonical: string;
  /** All resolved protected inputs, digested before Launcher auth. */
  protectedInputs: readonly ResolvedProtectedInput[];
  makeStateSink: (runId: string, stateDirPath: string) => PipelineRunSink;
}

export interface MultiStateRunOptions {
  workspace: string;
  launcherId?: string;
}

interface ActivationExecution {
  outcome: string;
  sessionId: string | undefined;
  accepted: AcceptedAgentResultRecord;
}

/**
 * Runs one agent-state activation: durable activation bookkeeping, one child
 * Session, one worker run with the state's timeout, result and protected
 * input verification, and the session cleanup — exactly one delete per
 * activation, also on failure and on recorded signals.
 */
async function runActivation(
  deps: AgentSmokeDeps,
  options: MultiStateRunOptions,
  params: MultiStateRunParams,
  ctx: {
    gate: RunCauseGate;
    sink: PipelineRunSink;
    auth: { launcher_id?: string };
    baseOperatorEnv: Record<string, string>;
    runId: string;
    state: AgentStateView;
  },
): Promise<ActivationExecution> {
  const { gate, sink, runId, state } = ctx;
  const profile = params.profiles.get(state.profile);
  if (profile === undefined) {
    throw new PipelineError(
      `agent state ${JSON.stringify(state.id)} references profile ${JSON.stringify(state.profile)} which was not loaded before Launcher authentication`,
    );
  }

  // after a recorded signal no new Session may be created
  gate.checkAbort();

  const activationIndex = await sink.startActivation(state.id, profile.profileName);

  // the workspace-relative control paths of this activation; the host-side
  // control tree is built from the fixed canonical workspace below
  const resultPathInWorkspace = `${AGENT_SMOKE_DIR}/${runId}/activations/${activationIndex}-${state.id}/attempt-1/result.json`;
  const executionDocPathInWorkspace = `${AGENT_SMOKE_DIR}/${runId}/activations/${activationIndex}-${state.id}/attempt-1/execution.md`;
  const resultPathHost = activationResultFilePath(
    params.workspaceCanonical,
    runId,
    activationIndex,
    state.id,
  );

  const declaredInputs = state.inputs.map((inputId) => {
    const spec = params.plan.pipeline.inputs.find((input) => input.id === inputId);
    if (spec === undefined) {
      throw new PipelineError(
        `agent state ${JSON.stringify(state.id)} references undeclared input ${JSON.stringify(inputId)}`,
      );
    }
    return spec;
  });

  let sessionId: string | null = null;
  let accepted: ActivationExecution | null = null;
  let failure: Error | null = null;
  try {
    // orchestrator-owned control paths are built from the fixed canonical
    // workspace and prepared fail-closed before any Session exists
    const activationDir = await prepareActivationControlTree({
      workspaceCanonical: params.workspaceCanonical,
      runId,
      activationIndex,
      stateId: state.id,
      executionDocument: executionDocument({
        runId,
        stateId: state.id,
        activationIndex,
        attempt: 1,
        inputs: declaredInputs.map((input) => ({ id: input.id, path: input.path })),
        resultPathInWorkspace,
        allowedOutcome: "completed",
        promptContent: state.promptContent,
      }),
    });
    console.error(
      `orchestrator: activation ${activationIndex} of state ${JSON.stringify(state.id)} in ${activationDir}`,
    );

    // every input of this state must exist before its Session is created
    for (const input of declaredInputs) {
      const candidate = resolve(params.workspaceCanonical, input.path);
      let info;
      try {
        info = await stat(candidate);
      } catch (cause) {
        throw new RuntimeInputError(
          `input ${JSON.stringify(input.id)} (${input.path}) required by agent state ${JSON.stringify(state.id)} is not accessible: ${describeError(cause)}`,
        );
      }
      if (!info.isFile()) {
        throw new RuntimeInputError(
          `input ${JSON.stringify(input.id)} (${input.path}) required by agent state ${JSON.stringify(state.id)} is not a regular file`,
        );
      }
      const canonical = await realpath(candidate).catch((cause: unknown) => {
        throw new RuntimeInputError(
          `input ${JSON.stringify(input.id)} (${input.path}) cannot be canonicalized: ${describeError(cause)}`,
        );
      });
      if (
        canonical !== params.workspaceCanonical &&
        !canonical.startsWith(`${params.workspaceCanonical}/`)
      ) {
        throw new RuntimeInputError(
          `input ${JSON.stringify(input.id)} (${input.path}) resolves outside workspace ${params.workspaceCanonical}`,
        );
      }
    }

    // the declared protected inputs are re-resolved and compared against the
    // recorded baseline (canonical target, device, inode, digest) before the
    // Session is created
    await verifyProtectedInputs(
      params.workspaceCanonical,
      params.protectedInputs,
      `before activation ${activationIndex}`,
    );

    // final synchronous signal check: no await between this check and the
    // Session create call, so a signal recorded during the durable
    // start_activation or the filesystem preparation cannot create a Session
    gate.checkAbort();

    const child = await createChildSession(
      deps.cli,
      deps.config,
      options.workspace,
      ctx.baseOperatorEnv,
    );
    sessionId = child.sessionId;

    if (
      ctx.auth.launcher_id !== undefined &&
      child.launcherId !== undefined &&
      child.launcherId !== ctx.auth.launcher_id
    ) {
      throw new DockerHelperError(
        "unexpected_response",
        `created session belongs to launcher ${child.launcherId}, expected ${ctx.auth.launcher_id}`,
      );
    }
    console.error(`orchestrator: child session ${child.sessionId} created (activation ${activationIndex})`);
    // recorded immediately after the create, before any other operation
    await sink.activationSessionCreated(child.sessionId);

    // a signal during the create lets it complete; the Session is then
    // deleted and the worker never runs
    gate.checkAbort();

    console.error(`orchestrator: pulling agent image ${profile.image}`);
    const pull = await deps.cli(
      pullArgs(profile.image, deps.config.socketPath),
      childSessionEnv(child.token),
      "inherit",
    );
    if (pull.code !== 0) {
      console.error(
        `orchestrator: warning: docker-helper pull ${profile.image} failed (exit ${pull.code}); continuing, the image may already be present locally`,
      );
    }

    gate.checkAbort();

    await sink.activationAgentRunning();

    // the expected result file of this activation must not exist before the
    // worker starts: a pre-placed file or symlink is a control-path violation
    await checkResultFileAbsent(resultPathHost);

    const spec = agentWorkerSpec({
      runId,
      stateId: state.id,
      activationIndex,
      attempt: 1,
      childSessionToken: child.token,
      workerImage: profile.image,
      inputs: declaredInputs.map((input) => ({ id: input.id, pathInWorkspace: input.path })),
      resultPathInWorkspace,
      executionDocPathInWorkspace,
      profileEnv: profile.env,
      opencodeConfigContent: profile.opencodeConfigContent,
    });

    console.error(`orchestrator: starting agent in child session (activation ${activationIndex})`);
    // after a recorded signal no new worker run may start: check synchronously
    // right before the CLI call, with no await in between
    gate.checkAbort();
    const run = await deps.cli(runArgs(spec, deps.config.socketPath), childSessionEnv(child.token), "inherit", {
      signalOnAbort: true,
      timeoutSeconds: state.timeout_seconds,
    });
    if (run.timedOut === true) {
      throw new AgentTimeoutError(state.timeout_seconds);
    }
    if (run.code !== 0) {
      throw new DockerHelperError(
        "cli_failure",
        `agent container failed (exit ${run.code})`,
      );
    }

    // re-verify every protected input's full filesystem identity after the
    // agent run, before the result can be accepted
    await verifyProtectedInputs(
      params.workspaceCanonical,
      params.protectedInputs,
      "after the agent run",
    );

    // the result file must be a regular non-symlink file inside the exact
    // activation leaf; the bytes are read without following symlinks
    const resultFile = resultPathHost;
    const bytes = await readResultFileNoFollow(resultFile);
    const resultHasher = new Bun.CryptoHasher("sha256");
    resultHasher.update(bytes);
    const resultSha256 = resultHasher.digest("hex");
    const raw = new TextDecoder().decode(bytes);
    const result = await verifyAgentResult(
      raw,
      { runId, stateId: state.id, activationIndex, attempt: 1 },
      params.workspaceCanonical,
      params.protectedInputs.map((input) => ({
        canonical: input.canonical,
        dev: input.dev,
        ino: input.ino,
      })),
    );

    console.error(
      `orchestrator: agent result verified for activation ${activationIndex} of state ${JSON.stringify(state.id)}`,
    );
    // durable result_accepted: digest + artifact paths only, never the summary
    await sink.activationResultAccepted({ resultSha256, artifacts: result.artifacts });
    // the validated outcome only; the graph engine selects the next state
    accepted = {
      outcome: result.status,
      sessionId: child.sessionId,
      accepted: { resultSha256, artifacts: result.artifacts },
    };
  } catch (cause) {
    failure = cause instanceof Error ? cause : new Error(String(cause));
  }

  // The active Session is deleted exactly once — on success, on failure, and
  // after a recorded signal.
  if (sessionId !== null) {
    try {
      await deleteChildSession(deps.cli, deps.config, sessionId, ctx.baseOperatorEnv);
    } catch (cause) {
      const cleanupCause = cause instanceof Error ? cause : new Error(String(cause));
      console.error(`orchestrator: cleanup failed: ${cleanupCause.message}`);
      gate.recordCleanupFailure(cleanupCause);
      try {
        await sink.activationFailed(SESSION_CLEANUP_FAILURE_REASON, "failed");
      } catch {
        // the sink refused the write (not_committed/poisoned); the cleanup
        // failure stands as the run's terminal cause
      }
      throw cleanupCause;
    }
  }

  if (failure !== null) {
    try {
      await sink.activationFailed(
        classifyRunFailure(failure, gate.recordedSignal),
        "completed",
      );
    } catch {
      // the sink refused the write; the original failure remains the run's
      // terminal cause and the previous snapshot stays authoritative
    }
    throw failure;
  }

  await sink.activationCleanupCompleted();
  if (accepted === null) {
    throw new PipelineError("the activation finished without an accepted result");
  }
  return accepted;
}

/**
 * Runs the multi-state pipeline: the graph engine drives the activations and
 * owns all transition decisions; the runner owns the per-activation Session
 * lifecycle and the run-level signal/finalization semantics.
 */
export async function runMultiStatePipeline(
  deps: AgentSmokeDeps,
  options: MultiStateRunOptions,
  params: MultiStateRunParams,
): Promise<LifecycleOutcome> {
  const runId = deps.randomId ? deps.randomId() : crypto.randomUUID();
  const stateDirPath = deps.stateDirPath ?? stateDir(deps.baseEnv ?? {});
  const gate = new RunCauseGate(deps.onSignal);
  const sink = params.makeStateSink(runId, stateDirPath);

  let lastSessionId: string | null = null;
  let lastAccepted: AcceptedAgentResultRecord | null = null;

  try {
    await sink.initialize();

    if (!(await (deps.workspaceExists ?? defaultWorkspaceExists)(options.workspace))) {
      throw new Error(
        `workspace ${options.workspace} is not accessible to the orchestrator; ` +
          "mount it at the same absolute path used for the docker-helper session",
      );
    }

    const { auth, baseOperatorEnv } = await lifecycleAuthority(deps, options);

    gate.checkAbort();

    const execution = await executePipelineGraph(
      params.plan.pipeline,
      async (state) => {
        const activation = await runActivation(deps, options, params, {
          gate,
          sink,
          auth,
          baseOperatorEnv,
          runId,
          state,
        });
        lastAccepted = activation.accepted;
        lastSessionId = activation.sessionId ?? null;
        return activation.outcome;
      },
      {
        onTransitionCommit: (step) => {
          if (lastAccepted === null) {
            throw new PipelineError(
              "no accepted agent result is available for the committed transition",
            );
          }
          return sink.recordTransition(step, lastAccepted);
        },
      },
    );
    await sink.recordTerminal(execution.terminalStateId, execution.terminalResult);
    console.error(
      `orchestrator: graph execution terminal ${execution.terminalStateId} (${execution.terminalResult}, ${execution.transitionCount} transition(s))`,
    );
    if (execution.terminalResult !== "success") {
      throw new PipelineError(
        `pipeline execution ended at terminal ${JSON.stringify(execution.terminalStateId)} with result failed`,
      );
    }
  } catch (cause) {
    gate.recordFailure(cause);
  } finally {
    // Run-level cutoff. Every activation's Session cleanup has settled
    // inside the activation executor; snapshot the accepted causes and
    // compute the final status synchronously, then close signal acceptance
    // with no await between the cause snapshot and this line. A signal
    // delivered from here on — including while the single authoritative
    // final state write is in flight — is late and can no longer change the
    // recorded outcome or the exit code. The terminal status is written
    // exactly once and is never rewritten afterwards.
    const finalFailure = gate.failure.error;
    const finalSignal = gate.recordedSignal;
    const finalStatus = gate.freezeFinalStatus();
    try {
      await sink.finalize({
        status: finalStatus,
        failure: finalFailure,
        signal: finalSignal,
        sessionId: lastSessionId,
      });
    } catch (cause) {
      gate.failure.error ??= cause instanceof Error ? cause : new StatePersistError();
    }
  }

  const failure = gate.failure.error;
  const cleanupError = gate.cleanup.error;
  if (failure !== null && !(failure instanceof SignalAbort)) {
    console.error(`orchestrator: agent-smoke failed: ${failure.message}`);
  }
  if (cleanupError !== null) {
    console.error(`orchestrator: cleanup failed: ${cleanupError.message}`);
  }

  const exitCode = runOutcomeExitCode(failure, cleanupError, gate.recordedSignal);

  return {
    ok: exitCode === 0,
    exitCode,
    runId,
    sessionId: lastSessionId ?? undefined,
    status: sink.currentStatus(),
    detail: failure?.message,
  };
}
