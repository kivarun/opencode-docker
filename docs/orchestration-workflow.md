# Orchestrator and Agent Workflow — Design Sketch

## Status

This document records the intended working model for the opencode-docker
orchestrator, agent workers, pipeline authors, and users.

It is a design sketch, not a claim that every described interface is already
implemented. The current implementation is the tested `smoke` and
`agent-smoke` baseline with the first trusted execution profile increment.
New behavior becomes a product contract only after it is implemented, tested,
and reflected in the canonical architecture documentation.

The stable project boundary and design principles are defined in
[manifesto.md](manifesto.md).

## Current baseline

The current orchestrator proves one complete delegated agent execution driven
by the default declarative pipeline:

1. The user starts `orchestrator agent-smoke` with a workspace, an operator
   configuration root, and optionally an explicit pipeline bundle root
   (`--pipeline-root`; the default is the bundled
   `/opt/orchestrator/pipelines/default`).
2. The orchestrator loads and validates the pipeline, checks that it matches
   the supported one-step execution shape, resolves the execution profile
   named by the agent state, and resolves the declared protected workspace
   input (regular file, workspace confinement, digest). All of that happens
   before Launcher authentication and before any child Session; a failure
   exits 1 without creating anything.
3. The orchestrator verifies its docker-helper Launcher credential.
4. It creates a child Session for the workspace.
5. It starts OpenCode non-interactively in the child Session. The worker is
   launched through the official docker-helper CLI 2.1.0 (`docker-helper run`,
   spawned as an argv array); until docker-helper issue #3 is implemented,
   resolved worker environment values (including secrets and the OpenCode
   config content) are visible in that CLI process's argv — a consciously
   accepted temporary risk. The pipeline's `timeout_seconds` bounds the run:
   the runner sends SIGTERM at the deadline, marks the result timed out, and
   the run fails normally.
6. OpenCode reads the orchestrator-owned execution document (run identity,
   state, attempt, input/result paths, allowed outcome, pipeline prompt,
   result format) and writes a structured `result.json`.
7. The orchestrator verifies the result schema (exact fields), run identity,
   artifact paths, workspace confinement, the unchanged protected input
   digest, and maps the validated outcome through the pipeline transition to
   the success terminal state.
8. On cancellation, the first SIGINT/SIGTERM is recorded by the lifecycle; a
   running `docker-helper run` process receives the same signal and docker-helper
   performs a bounded synchronous best-effort cancel. The orchestrator never
   confirms a terminal operation state; `session create`, `pull`, and
   `session delete` always run to completion, and the child Session is deleted
   only in the single lifecycle cleanup path after the active step settles.
   Signal acceptance closes in the same synchronous tail that completes the
   authoritative final state write: a signal accepted while that write was in
   flight rewrites a persisted `success` to `failed`, and a signal delivered
   after the write completes can no longer change the recorded outcome or the
   exit code.
9. It deletes the child Session and records the final cleanup result.
10. Process exit status reports overall success or failure.

OpenCode output and docker-helper CLI output are inherited by the orchestrator
process: docker-helper 2.1.0 delivers the container operation output already
mixed and prints it on the CLI's stdout, while the CLI's own warnings and
errors go to stderr; the original stream separation is not preserved. The
orchestrator's own diagnostics stay on stderr. Worker stdin is not interactive;
the orchestrator implements no polling, log cursor, or operation-API parsing.

Worker output is raw, untrusted container output. It must be treated as a
sensitive observation of a concrete run, never as a safe audit log and never as
authoritative state: the worker sees everything it was given and may print task
content, file fragments, or secret values it holds. Secret values and the task
body never appear in the orchestrator's own structured diagnostics or state
files; worker environment values do appear as `docker-helper run` argv elements
(see step 5).

The current implementation does not yet provide:

- pipeline graph execution beyond the supported one-step shape (multi-state
  execution is not implemented; unsupported pipelines fail closed before any
  Session);
- arbitrary user-supplied JSON Schema validation (the result schema must equal
  the standard agent result contract verbatim);
- retries (`max_attempts` must be 1 today), durable pipeline state, resume,
  user input, a local control API, and concurrency;
- a multi-step durable state machine;
- run listing or inspection commands;
- an event stream;
- a T3 integration.

## Pipelines (schema version 1: loader, validator, one-step execution)

`orchestrator/src/pipeline.ts` implements the declarative pipeline contract:
`parsePipelineSpec(raw)` validates the structure and graph in memory,
`loadPipeline(bundleRoot)` additionally loads the bundle from an absolute
directory containing exactly `pipeline.yaml` (`pipeline.yml` and JSON are not
supported) and resolves bundle files. `planOneStepExecution(pipeline)` builds
the one-step execution plan used by `agent-smoke`; the default bundled
pipeline is the production input for `agent-smoke`, and an external bundle is
selected with `--pipeline-root`.

Schema version 1 is fixed. Every mapping accepts only its exact field set;
unknown and missing fields fail closed:

- `schema_version: 1`, `entry_state: <state id>`,
  `max_transitions: <positive safe integer>`;
- `inputs`: list of `{id, path, protected}`; `id` is a safe unique identifier,
  `path` is a clean workspace-relative path (no absolute path, `~`, empty
  segments, `.` or `..`), and each workspace path may map to only one input
  declaration (duplicated paths are rejected, case-sensitively, as a lexical
  check), `protected` is boolean;
- `states`: non-empty list of `agent` and `terminal` states;
- agent state: `id`, `type: agent`, `profile` (validated with the execution
  profile name grammar), `prompt` and `result_schema` (bundle-relative paths),
  `inputs` (declared input ids, no duplicates), `timeout_seconds`,
  `max_attempts` (positive safe integers), and a non-empty `transitions` list
  of `{outcome, to}`;
- terminal state: only `id`, `type: terminal`, `result: success|failed`;
- states, inputs, and transitions are lists (not mappings) so uniqueness never
  depends on YAML last-wins behavior.

Graph invariants checked before a resolved pipeline is returned:

- the entry state exists; input and state identifiers are unique and safe;
- every agent input reference names a declared input;
- every transition has a non-empty outcome and an existing target; outcomes
  are unique within one state; every agent has at least one transition;
- at least one terminal state exists, every state is reachable from the entry
  state, and every agent state has a path to a terminal state; cycles are
  allowed because the whole run is bounded by `max_transitions`;
- `max_transitions`, `timeout_seconds`, and `max_attempts` are positive safe
  integers.

Bundle file validation: `loadPipeline` requires an absolute bundle root that
is a directory, and `pipeline.yaml` itself obeys the same fail-closed
containment contract as the other bundle files (a symlink to a file inside the
bundle is allowed, a symlink escape is rejected, and the file is read from the
verified canonical path). `prompt` and `result_schema` must be clean
bundle-relative paths that resolve (through `realpath`, symlinks inside the
bundle allowed) to regular files inside the bundle; absolute paths, traversal,
and symlink escapes fail closed. The prompt must be readable and non-empty;
the result schema must be a valid JSON object. The resolved pipeline carries
the accepted `schema_version`, the already-read prompt content, and the parsed
result schema. A pipeline
cannot declare images, environment, mounts, credentials, Docker options,
shell commands, JavaScript, or host callbacks — those are rejected by the
same exact-field validation. Workspace input existence and `run_id`,
artifact-confinement, and protected-task checks are orchestrator semantics,
not JSON Schema or loader checks.

Not implemented yet for pipelines: multi-state graph execution, arbitrary
JSON Schema support, retries, durable pipeline state, resume, user input,
API, and concurrency.

### One-step execution bridge (implemented)

`planOneStepExecution` accepts exactly one execution shape and rejects every
other structurally valid pipeline with a clear error before Launcher
authentication and before any child Session:

- exactly two states; the entry state is the agent state; the second state is
  a terminal with `result: success`;
- the agent state has exactly one transition, its outcome is `completed`, and
  its target is that terminal state (state and input identifiers are
  arbitrary; nothing is hardcoded);
- `max_transitions` is 1 and the agent `max_attempts` is 1;
- the agent uses exactly one declared input, the pipeline declares no other
  inputs, and that single input is `protected: true`;
- the agent's `result_schema` equals the standard agent result contract
  (`STANDARD_AGENT_RESULT_SCHEMA`) as a verbatim structural comparison — JSON
  key order does not matter; no generic JSON Schema engine is involved;
- `timeout_seconds` is within the single JS-timer bound
  (`MAX_RUN_TIMEOUT_SECONDS` = 2147483); larger values are rejected, never
  clamped.

Execution: the orchestrator materializes a non-secret execution document
inside the orchestrator-owned run directory of the workspace (run id, state
id, attempt 1, workspace-relative input/result paths, allowed outcome, the
pipeline prompt body, and the exact result format). The OpenCode command
receives only a short static instruction pointing at that document; prompt
and input bodies never appear in argv, env, state, or diagnostics; the
pipeline bundle and config root are not mounted into the worker. After the
verified result, the outcome is mapped through the declared transition table
of the agent state; success is possible only by reaching a success terminal
state. The pipeline's `timeout_seconds` is enforced by the CLI runner on the
signalable worker `docker-helper run` only; the deadline sends SIGTERM, the
result is marked timed out, and the run fails normally with a single cleanup.
`max_attempts` is 1, so no retries are implemented.

## Execution profiles (implementation complete, end-to-end UAT pending)

The first increment of trusted execution profiles is implemented for
`agent-smoke` and covered by deterministic tests. The end-to-end
profile-backed `agent-smoke` UAT (real launcher credential, real docker-helper,
real OpenCode/LLM run accepted through the profile path) has not been executed
yet; until that run succeeds, the profile increment is not a proven baseline.

A profile is a small YAML document under an operator-controlled
configuration root:

```text
<config-root>/
  profiles/<profile-name>.yaml
  opencode/<configuration files>
```

The minimal schema (schema_version 1) is:

```yaml
schema_version: 1
image: ghcr.io/kivarun/opencode-docker/base:latest
opencode_config: opencode/default.jsonc

env:
  LLM_SERVER:
    from_env: LLM_SERVER
    required: true
  LLM_KEY:
    from_env: LLM_KEY
    required: true
  OPENCODE_ENABLE_EXA:
    from_env: OPENCODE_ENABLE_EXA
    required: false
```

Only `.yaml` profile files are supported; JSON and `.yml` are not accepted as
alternative profile formats.

Implemented rules:

- the profile name comes from the CLI and is a single safe path component;
- the profile file and the referenced OpenCode configuration must be regular
  files that resolve inside the configuration root; traversal and symlink
  escape are rejected;
- unknown and missing fields are rejected; `schema_version` must be 1;
- `env` bindings are exact: destination and source must be valid environment
  variable names, only `from_env` and `required` are accepted, there is no
  ambient inheritance and no wildcard forwarding;
- a missing required source variable fails before any child Session is
  created; a missing optional source variable is not forwarded; an empty
  required source value is treated as missing;
- orchestrator-owned control variables (`DOCKER_HELPER_*`, `AGENT_SMOKE_*`,
  `ORCHESTRATOR_*`, `OPENCODE_CONFIG_CONTENT`) and operator environment path
  variables (`HOME`, `XDG_CONFIG_HOME`, `XDG_STATE_HOME`, `XDG_RUNTIME_DIR`)
  can be neither destinations nor sources; these variables remain available to
  the orchestrator itself and to its minimal CLI environment;
- the orchestrator reads the OpenCode configuration and forwards it to the
  worker as `OPENCODE_CONFIG_CONTENT`;
- profile files may reference secret environment-variable names but must never
  contain secret values; resolved values reach the worker only as separate
  `--env KEY=VALUE` arguments of the `docker-helper run` CLI call. Until
  docker-helper issue #3 is implemented these values are visible in that CLI
  process's argv (a consciously accepted temporary risk); they never appear in
  the orchestrator's own diagnostics or state files, and the task body is never
  placed into argv or env (the task travels by workspace path only). Raw worker
  output remains an untrusted stream that may contain whatever the worker
  chooses to print;
- the profile comes from the pipeline's agent state (`agent-smoke` has no
  `--profile` flag) and the workspace input path comes from the pipeline's
  declared inputs (no `--task` flag); an external pipeline bundle is selected
  with `--pipeline-root` (default: the bundled default pipeline). There is no
  `--image` flag, the worker image comes only from the selected profile.
  Plain `smoke` keeps `--image`.

Profile-selected file projections, resource limits, and profile inheritance
are not implemented yet.

## Containerized orchestrator workspace projection (proven integration pattern)

A containerized orchestrator is launched with the Session workspace mounted
twice through relative `source: "."` mounts:

- the canonical host workspace path (the path passed to
  `docker-helper session create`; docker-helper validates it on its own host
  view), and
- `/workspace` as an ergonomic local path.

Both mount targets map the same live Session workspace; writes through one
target are immediately visible through the other. No symlinks, workspace
copies, ambient host mounts, or permission changes are involved. This pattern
was verified against docker-helper 2.1.0 with identical device and inode
identities across both targets.

A helper-launched container cannot receive the docker-helper unix socket:
`docker-helper run` only accepts workspace-relative mount sources, so a
containerized orchestrator cannot reach the docker-helper API from inside a
helper-launched container. A containerized orchestrator must therefore be
launched directly by the operator (plain `docker run` with the docker-helper
socket, the Launcher credential, and the dual workspace mounts), or
docker-helper needs a future feature for trusted socket projection. This is a
known docker-helper integration limitation, not an orchestrator defect; until
it is addressed, the end-to-end profile-backed `agent-smoke` UAT cannot be
executed from inside a helper-launched orchestrator container.

## Responsibility and trust boundaries

| Layer | Responsibility | Trust |
| --- | --- | --- |
| Operator configuration | Images, OpenCode configuration, exact environment and file bindings, execution limits | Trusted |
| Pipeline bundle | States, prompts, artifact contracts, result schemas, outcomes, transitions, and bounds | Declarative input |
| Run input | Workspace, task contract, and explicit run parameters | User-owned |
| Orchestrator state | Current state, attempts, accepted results, transition history, and cleanup status | Private and authoritative |
| Agent worker | Perform one bounded step and report a structured outcome | Untrusted execution |
| docker-helper | Docker-facing capability, Session ownership, workspace policy, and runtime enforcement | Authoritative capability boundary |
| UI integration | Present state and events and submit user commands | Optional client |

A lower-trust layer may select from capabilities offered by a higher-trust
layer, but it cannot widen them. In particular, a pipeline can reference a
named execution profile but cannot add images, environment variables, host
files, mounts, credentials, or Docker options.

## Configuration layout

The orchestrator should receive one operator-controlled configuration root
instead of mounting the user's home configuration into every worker.

That root contains:

- the orchestrator configuration;
- named execution profile definitions;
- OpenCode configuration files referenced by those profiles;
- optional files that profiles may project into a worker.

The configuration root is mounted read-only into the orchestrator. It is not
mounted wholesale into agent workers.

Secret values do not belong in profile files, pipeline bundles, prompts, or the
workspace. Profiles contain explicit references to operator-provided secret
sources, initially exact environment-variable names.

The orchestrator resolves the selected profile and constructs the worker
environment from exact bindings. Ambient environment inheritance and wildcard
forwarding are not allowed.

OpenCode configuration should normally be read by the orchestrator and passed
to the worker through `OPENCODE_CONFIG_CONTENT`. Secret values remain separate
environment bindings referenced from that OpenCode configuration.

Optional projected files must use sources relative to the configured root and
fixed container targets. The orchestrator canonicalizes the source, rejects
symlink escape, and projects only the files declared by the selected profile.
File projection is not implemented yet.

A secret required by a worker must be considered visible to that worker.
Security therefore depends on purpose-specific credentials, narrow scope, and
minimal projection.

Launcher credentials, administrative tokens, parent authority, and private
orchestrator state are never projected into a worker. The worker receives only
the child Session capability required for its step.

## Pipeline bundles

The project ships a default pipeline as data under:

```text
pipelines/default/
  pipeline.yaml
  prompts/
  schemas/
```

The same loader and validator accept an external pipeline directory selected by
the user. Creating a custom pipeline must not require rebuilding the
orchestrator or changing project code.

A pipeline may declare:

- its schema version and entry state;
- states and stable state identifiers;
- the execution profile used by each state;
- relative prompt paths;
- required and protected inputs;
- expected output artifacts;
- a result schema;
- allowed outcomes;
- outcome-to-state transitions;
- terminal, blocked, and waiting-for-input states;
- retry, iteration, and timeout bounds;
- explicit concurrency when the engine supports it.

Pipeline files and prompt files are resolved only inside the pipeline bundle.
A pipeline cannot contain arbitrary executable hooks, shell commands, embedded
JavaScript, unrestricted expressions, or host callbacks.

The orchestrator validates the complete pipeline before creating a child
Session. Unknown references, unreachable required states, invalid transitions,
missing prompts or schemas, and unbounded execution fail before agent work
begins.

The exact pipeline schema and versioning contract remain to be defined before
implementation.

## Starting a run

The intended start sequence is:

1. The user selects a workspace, task input, pipeline bundle, and operator
   configuration.
2. The orchestrator canonicalizes all roots and verifies that declared inputs
   exist and remain inside their owning boundaries.
3. It loads and validates the complete operator configuration and pipeline.
4. It resolves every profile reference without exposing secret values to the
   pipeline.
5. It records digests for immutable inputs, including the default pipeline's
   `TASK.md`.
6. It snapshots or otherwise binds the run to the exact pipeline and profile
   definitions used at start.
7. It creates authoritative run state with the pipeline entry state.
8. Only then may it create the first child Session.

A resumed run must use the same accepted pipeline contract unless the user
performs a future explicit migration operation. Silent continuation under a
changed pipeline is not allowed.

The exact snapshot and migration mechanism is still an open design decision.

## Executing one state

For each state, the orchestrator:

1. Reads the authoritative current state and attempt.
2. Resolves the named execution profile.
3. Selects the prompt, inputs, output contract, result schema, and allowed
   outcomes declared by the state.
4. Creates a fresh child Session for the workspace.
5. Constructs the exact worker environment and file projection.
6. Materializes a non-secret execution envelope in the workspace when needed.
7. Starts OpenCode non-interactively.
8. Relays diagnostic events without treating them as state transitions.
9. Waits for process completion.
10. Reads the required structured result.
11. Verifies run, state, and attempt identity.
12. Validates the result against the state's schema.
13. Resolves and verifies every reported artifact.
14. Rechecks immutable and protected inputs.
15. Maps the validated outcome through the declared transition table.
16. Persists the accepted result and transition atomically.
17. Deletes the child Session and records cleanup status.

A missing, malformed, contradictory, or out-of-contract result does not advance
the pipeline.

The agent cannot select an arbitrary next state. It reports only an outcome
allowed by the current state; the orchestrator owns the outcome-to-state
mapping.

Each state execution is a bounded attempt. A retry is a new attempt with its
own identity, result, and lifecycle record.

## Prompts and worker input

Prompts are pipeline input, not orchestration authority.

The orchestrator should avoid placing prompt or task bodies in command-line
arguments, environment variables, operational logs, or audit records. The
worker command should receive stable paths and non-secret identifiers and
instruct OpenCode to read the corresponding files.

Prompts may explain:

- the role of the current agent;
- the current bounded task;
- the files it may use;
- the required outputs;
- the allowed outcome vocabulary.

Prompts cannot override profile policy, protected input rules, result
validation, transition rules, Session scope, or cleanup behavior.

The default pipeline may define architect, implementation, review, security
review, warning, proposal, and rework behavior. Those are default-pipeline
concepts rather than hard-coded engine roles.

## State ownership

The orchestrator's private state is the sole source of truth for pipeline
progress.

It records at least:

- run identity and lifecycle status;
- the bound pipeline identity;
- current state and attempt;
- accepted results and transitions;
- active or last child Session identity;
- timestamps;
- blocked or waiting-for-input status;
- cleanup failures.

Files such as `STATE.md` may be generated for compatibility or human
inspection, but an agent cannot advance the run by modifying them.

State transition and the event describing that transition must be committed as
one authoritative operation. The persistence mechanism may initially be an
atomic file format or SQLite; that choice remains open until concurrency,
query, and recovery requirements are fixed.

## User intervention

User intervention is an explicit state transition, not an interactive agent
side channel.

When a validated result reports an outcome that maps to a blocked or
waiting-for-input state:

1. The orchestrator accepts and persists the result.
2. It records the required question, warning, or proposal as a validated
   artifact or structured payload.
3. It deletes the child Session.
4. It exposes the waiting state through CLI and API.
5. The user inspects the run and submits an explicit response.
6. The orchestrator validates and records that response as new run input.
7. It applies the declared resume transition.
8. A new agent attempt starts only if the resulting state requires one.

The default pipeline keeps the user task immutable. An architectural proposal
that requires changing `TASK.md` blocks until the user accepts and applies the
change. An architectural warning blocks without silently changing the task.

No agent process is kept alive merely to wait for a human answer.

## Observation and control

The orchestrator must remain usable without a graphical interface.

The target client contract consists of:

- CLI commands for starting, listing, inspecting, following, cancelling, and
  resuming runs;
- a local HTTP/JSON control and query API over a Unix socket;
- a one-way event stream suitable for following a run;
- stable normalized lifecycle events and validated results.

Server-Sent Events are the expected initial event transport because observation
is one-way and control commands remain ordinary HTTP requests. WebSocket support
requires a separate demonstrated bidirectional use case.

D-Bus is not a core orchestrator-agent or orchestrator-client transport.
Mounting a desktop Session bus into containers expands host authority, while a
private D-Bus would duplicate a broker and protocol already covered by the
local API. A future desktop notification adapter may use D-Bus at the client
edge.

T3 or another web interface may consume the local API through its backend and
present pipeline graphs, agents, events, artifacts, and waiting user questions.
It remains optional. The browser does not receive direct access to the
orchestrator Unix socket or docker-helper.

## Event and log handling

Normalized orchestrator events may include:

- run created, completed, failed, or cancelled;
- state entered or left;
- attempt started or completed;
- child Session created or deleted;
- validated result accepted or rejected;
- artifact accepted;
- user input required or received;
- cleanup failed.

The event vocabulary and schema must be defined before becoming a public API.

Raw OpenCode JSON events remain diagnostics. They may contain task text, model
output, file fragments, or secrets exposed to the agent. They must not drive
state transitions.

The default behavior should persist bounded normalized lifecycle information
and validated results. Persistence and retention of raw agent output require an
explicit operator setting.

## Failure and recovery

Failures do not create implicit delayed work.

A failed attempt either:

- follows an explicit pipeline transition;
- remains failed for an explicit retry;
- enters a declared recovery or waiting state;
- terminates the run.

Retries, resume, and user input always create visible state changes. The
orchestrator does not hide a queue of actions that may execute later under
different assumptions.

If child Session cleanup fails, the run cannot be reported as fully successful.
The cleanup failure is retained as an observable condition and handled through
an explicit recovery contract.

Uncertain state does not grant authority or become normal managed state.

## Security invariants

Regardless of the selected pipeline or profile:

- docker.sock is never exposed to the orchestrator or workers;
- docker-helper remains the Docker capability and policy boundary;
- the orchestrator holds the Launcher credential;
- workers receive no Launcher or administrative credential;
- environment projection is exact and profile-controlled;
- host files are never selected directly by a pipeline;
- pipeline and profile paths are canonicalized inside their owning roots;
- workspace artifacts cannot escape through relative paths or symlinks;
- immutable inputs are verified after every relevant attempt;
- unvalidated results cannot advance state;
- secrets are excluded from state, results, events, diagnostics, and command
  arguments;
- child Sessions are cleaned up after every attempt, including blocked states;
- a pipeline cannot increase authority beyond its selected profile;
- optional clients cannot bypass the orchestrator state machine.

## Initial implementation order

The design should be introduced through the existing working smoke path rather
than as a parallel implementation.

The first increment is execution profiles (implementation and deterministic
tests complete; end-to-end UAT pending):

1. Define the smallest profile schema required by the current agent smoke.
2. Load one named profile from an operator-controlled configuration root.
3. Replace the current fixed environment allowlist with exact profile bindings.
4. Pass OpenCode configuration through the selected profile.
5. Run the existing real agent smoke without changing its lifecycle and result
   guarantees.

The next increment expresses the current one-step flow through the default
declarative pipeline and generalizes it into the orchestrator-owned state
machine. The one-step bridge is implemented today: `agent-smoke` executes the
default (or an explicitly selected) pipeline in its supported one-step shape
and maps the validated outcome through the declared transition. The
generalization to arbitrary multi-state graphs, durable state, and resume has
not started.
