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

The current orchestrator proves one complete delegated agent execution:

1. The user starts `orchestrator agent-smoke` with an explicit workspace,
   operator configuration root, named execution profile, and task file.
2. The orchestrator loads and verifies its docker-helper Launcher credential.
3. It loads and validates the selected execution profile before creating any
   child Session.
4. It creates a child Session for the workspace.
5. It starts OpenCode non-interactively in the child Session. The worker is
   launched through the docker-helper HTTP API over its unix socket, so
   environment values never appear in process arguments.
6. OpenCode reads the task by workspace path and writes a structured
   `result.json`.
7. The orchestrator verifies the result schema, run identity, artifact paths,
   workspace confinement, and the unchanged task digest.
8. It deletes the child Session and records the final cleanup result.
9. Process exit status reports overall success or failure.

OpenCode JSON events and docker-helper diagnostics currently flow directly to
the orchestrator terminal through inherited stdout and stderr. Worker stdin is
not interactive.

The current implementation does not yet provide:

- general declarative pipelines;
- a multi-step durable state machine;
- user input and resume;
- run listing or inspection commands;
- a local control API or event stream;
- a T3 integration.

## Execution profiles (implemented)

The first increment of trusted execution profiles is implemented for
`agent-smoke`. A profile is a small JSON document under an operator-controlled
configuration root:

```text
<config-root>/
  profiles/<profile-name>.json
  opencode/<configuration files>
```

The minimal schema (schema_version 1) is:

```json
{
  "schema_version": 1,
  "image": "ghcr.io/kivarun/opencode-docker/base:latest",
  "opencode_config": "opencode/default.jsonc",
  "env": {
    "LLM_SERVER": {
      "from_env": "LLM_SERVER",
      "required": true
    },
    "LLM_KEY": {
      "from_env": "LLM_KEY",
      "required": true
    }
  }
}
```

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
  `ORCHESTRATOR_*`, `OPENCODE_CONFIG_CONTENT`) can be neither destinations nor
  sources;
- the orchestrator reads the OpenCode configuration and forwards it to the
  worker as `OPENCODE_CONFIG_CONTENT`;
- profile files may reference secret environment-variable names but must never
  contain secret values; resolved values reach the worker only through the
  docker-helper HTTP API over its unix socket and never appear in process
  arguments, logs, or state files;
- `agent-smoke` takes `--config-root` and `--profile`; there is no `--image`
  flag, the worker image comes only from the selected profile. Plain `smoke`
  keeps `--image`.

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

The first increment is execution profiles (implemented):

1. Define the smallest profile schema required by the current agent smoke.
2. Load one named profile from an operator-controlled configuration root.
3. Replace the current fixed environment allowlist with exact profile bindings.
4. Pass OpenCode configuration through the selected profile.
5. Run the existing real agent smoke without changing its lifecycle and result
   guarantees.

The next increment expresses the current one-step flow through the default
declarative pipeline and generalizes it into the orchestrator-owned state
machine. That work has not started.
