# Project Manifesto

## Purpose

opencode-docker exists to run OpenCode agents in reproducible container
environments and coordinate them through explicit, deterministic,
operator-controlled pipelines.

The project provides a small family of agent images and a local orchestrator.
It does not fork OpenCode, modify its internals, or expose the Docker daemon to
agents.

Its product boundary is controlled execution of local agent workflows:
selecting an execution profile, running bounded pipeline steps, validating their
results, advancing durable state, handling user intervention, and cleaning up
the delegated runtime.

## OpenCode is an execution engine, not the state authority

OpenCode executes an agent step. It does not own the pipeline, select arbitrary
transitions, or define whether a run has succeeded.

Model output, console text, tool events, and process exit status are evidence,
not authoritative pipeline state. A step completes only when the orchestrator
receives and validates the structured result required by the current pipeline
state.

Provider, model, plugin, and OpenCode permission choices belong to trusted
execution profiles. They are not hard-coded into the pipeline engine.

opencode-docker should consume stable OpenCode interfaces rather than mirror
OpenCode internals or grow into a second implementation of its session and
agent model.

## The orchestrator owns orchestration

The orchestrator is the sole owner of:

- current run and pipeline state;
- valid state transitions;
- step and attempt identity;
- execution ordering and explicitly declared concurrency;
- retry and iteration limits;
- child Session lifecycle;
- result and artifact validation;
- transitions into blocked or waiting-for-input states;
- cancellation and cleanup.

An agent performs one bounded step and reports an outcome from the contract of
that state. It cannot advance the pipeline by editing a state file or by naming
an arbitrary next state.

The orchestrator maps a validated outcome through the declarative transition
table and persists the transition atomically.

Human-readable state files may be generated as views or agent inputs, but they
are not the authoritative state store.

## Declarative pipelines, not embedded programs

A pipeline is data, not project code.

A pipeline bundle may define:

- states and their stable identifiers;
- the execution profile used by each state;
- prompt files;
- declared input and output artifacts;
- result schemas;
- allowed outcomes;
- transition tables;
- iteration, retry, and timeout bounds;
- terminal, blocked, and waiting-for-input states.

Users must be able to create and modify pipelines without rebuilding the
orchestrator or changing its source code. The project should ship a useful
default pipeline, while accepting external pipeline bundles through the same
validated contract.

Pipeline flexibility comes from an explicit graph and typed contracts, not from
arbitrary shell commands, embedded JavaScript, unrestricted expressions, or
host callbacks. A pipeline is not a plugin with ambient process authority.

Unknown states, outcomes, fields, or transitions fail closed.

## Prompts are inputs, not authority

Prompts describe the work requested from an agent. They do not define runtime
authority, secret access, host mounts, or valid state transitions.

A pipeline may reference user-provided prompt files, but the orchestrator still
owns:

- which agent profile runs;
- which inputs it receives;
- which outputs are accepted;
- which files are protected;
- how the reported outcome changes the run state.

Prompt content should be passed as controlled run input. It should not be
embedded unnecessarily in process arguments, environment variables, audit
records, or operational logs.

The orchestrator must never infer a transition from free-form model prose when a
structured outcome contract exists.

## The user task remains user-owned

The declared task input is the solution contract for a run. In the default
pipeline this is `TASK.md`.

Agents must not silently rewrite, replace, or delete that contract. The
orchestrator records its identity and content digest and verifies protected
inputs after every relevant step.

When an agent determines that the task contract must change, it produces an
explicit proposal and the pipeline waits for user action. When progress is
blocked without requiring a task change, it produces an explicit warning or
request for input.

The user remains the authority for changing the task and resolving questions
that the pipeline cannot decide from its accepted inputs.

A waiting pipeline does not keep an agent process or child Session alive merely
to wait for a human response. The orchestrator records the blocked state,
cleans up the step runtime, accepts explicit user input, and resumes through a
new validated transition.

## Trusted execution profiles form the capability ceiling

Execution profiles are trusted operator configuration. A profile may select:

- an approved agent image;
- an OpenCode configuration;
- exact environment bindings;
- explicitly projected files;
- resource and execution limits;
- other policy-controlled runtime choices supported by the orchestrator.

A pipeline may reference a profile by name. It cannot widen that profile,
introduce new environment variables, select arbitrary host paths, inject
credentials, or add Docker options.

Configuration and secret values do not belong in pipeline bundles or prompts.
Secrets are resolved by the orchestrator from explicit operator-controlled
sources and forwarded only when the selected profile requires them.

Environment forwarding uses exact bindings, not ambient inheritance or broad
prefix matching.

Any secret provided to a worker must be considered visible to that worker.
Security therefore comes from narrow, purpose-specific credentials, minimal
projection, and bounded authority—not from pretending that an executing agent
cannot inspect its own environment.

Launcher credentials, administrative credentials, orchestrator state paths, and
other control-plane authority are never forwarded to workers. A worker receives
only the narrow child Session capability required for its step.

## Images are execution environments, not policy bundles

Agent images provide reproducible OpenCode environments and demonstrated
toolchains. They should contain common runtime dependencies and safe project
defaults, not user credentials, task-specific prompts, pipeline state, or
operator policy.

A specialized image should exist because a demonstrated workload needs a
distinct execution environment, not merely because another possible image can
be imagined.

Runtime configuration belongs to execution profiles. Pipeline behavior belongs
to pipeline bundles. Durable state belongs to the orchestrator.

Keeping these responsibilities separate allows images to be rebuilt and shared
without carrying user-specific authority or workflow decisions.

## Safe by construction

Dangerous behavior should be absent from the contract rather than accepted and
filtered later.

The orchestrator must enforce, at its own boundaries:

- validated pipeline and profile schemas;
- workspace and artifact path confinement;
- canonical path handling and symlink-escape rejection;
- immutable input verification;
- exact environment projection;
- structured result validation;
- run, state, and attempt identity;
- bounded retries, iterations, output, and runtime;
- child Session cleanup;
- fail-closed handling of missing or contradictory state;
- exclusion of secrets and control credentials from logs and results.

A useful test for every new feature is:

- What demonstrated user workflow requires it?
- Which existing owner should implement it?
- What is the smallest declarative contract that satisfies the need?
- Which authority or data would become reachable?
- Can the same result be achieved without another state, adapter, or execution
  path?

## Control plane, not capability provider

opencode-docker is intentionally a local agent control plane. It is not a
container security layer, Docker socket proxy, generic host-command service, or
replacement for docker-helper.

docker-helper remains the authoritative owner of Docker-facing capability,
authentication, Session ownership, workspace policy, mount enforcement,
mandatory access control, and container cleanup semantics.

The orchestrator translates pipeline intent into docker-helper capabilities. It
must not duplicate docker-helper policy or treat backend Docker state as its own
authority.

This dependency is one-way: docker-helper remains independently usable and
contains no opencode-docker-specific orchestration behavior.

## Observable by contract

A run must be understandable without interpreting model prose.

The orchestrator should expose stable records for:

- run creation and completion;
- current and previous states;
- active profile and step;
- attempts and validated outcomes;
- lifecycle failures and cleanup failures;
- blocked states and required user input;
- accepted artifacts;
- cancellation.

CLI and API clients consume the same run and event contracts.

Raw OpenCode events may be relayed as diagnostics, but they are not pipeline
state and may contain task content, model output, file fragments, or other
sensitive material. Their persistence and retention must therefore be explicit
and bounded.

Durable normalized lifecycle events and validated results remain the primary
observability contract.

## Standalone core, optional integrations

The orchestrator must remain usable through its own CLI and local API.

Web interfaces, desktop notifications, T3, and other integrations belong at the
client edge. They may present runs, stream events, collect user input, and issue
control requests through stable orchestrator contracts.

No optional UI, external control plane, or shared service should become
mandatory for executing and observing a local pipeline.

Standardize interfaces and conventions where integration is useful. Do not
create mandatory shared runtime code merely to connect projects.

## Simple by default

A user should be able to run a useful agent pipeline without designing a state
machine, assembling container policy, or reproducing security configuration by
hand.

The project should provide:

- a safe default execution profile;
- a documented default pipeline;
- narrow required configuration;
- actionable validation failures;
- useful CLI output;
- progressive disclosure of advanced controls.

Defaults must remain real product behavior rather than examples that bypass the
normal contracts.

Advanced users may replace profiles and pipeline bundles, but ordinary users
should not need to understand every internal state or backend mechanism before
running a task.

## Development philosophy

Develop from demonstrated workflows rather than inventing a general agent
platform in advance.

A use case demonstrated by one real operator is valid evidence. Speculative
compatibility with every model provider, agent runtime, workflow engine,
container backend, or user interface is not.

Future extensibility is an architectural constraint, not a requirement to
implement future components early.

## Presumption of non-existence

For product and design decisions, anything not established by evidence does not
exist.

A pipeline concept, lifecycle state, agent role, profile field, compatibility
promise, recovery path, integration, or authority must be supported by an
accepted contract, reachable production behavior, reproducible observation, or
explicit current use case.

Names, example files, model suggestions, backend objects, old prompts, and
possible future integrations are leads to investigate, not proof that a product
concept exists.

Every additional noun, state, owner, adapter, alias, configuration field, and
production path increases conceptual entropy and carries a burden of proof.

Prefer:

- one canonical term for one domain concept;
- one authoritative owner for each state and policy;
- one production path for equivalent behavior;
- explicit translation at integration boundaries;
- deletion or consolidation when distinctions are no longer supported.

## Local agent workflows, not a distributed AI platform

opencode-docker is designed for local and small shared-host agent workflows
where reproducibility, bounded authority, deterministic progress, human
oversight, and predictable cleanup matter more than maximum throughput or
continuous availability.

It is not a general workflow engine, multi-tenant cloud platform, cluster
scheduler, high-availability service, or autonomous organization of agents.

Remote control, multiple simultaneous runs, richer interfaces, and additional
execution environments may be added when demonstrated use cases require them.
The current architecture should avoid closing those paths without predesigning
their implementation.

## Architectural constraints

- one project — one coherent responsibility: controlled OpenCode execution and
  orchestration;
- OpenCode executes agent steps; it does not own pipeline state;
- the orchestrator is the sole authority for run state and transitions;
- pipelines are declarative data, not executable extensions;
- prompts guide agents but do not grant authority;
- trusted execution profiles form the capability ceiling;
- workers receive only explicitly required files, environment, and Session
  authority;
- user-owned task inputs cannot be changed silently;
- state advances only from validated structured results;
- docker-helper remains the Docker capability and security boundary;
- common workflows remain simple through safe defaults and progressive
  disclosure;
- CLI and API remain sufficient without optional integrations;
- integrations live at the client edge;
- contracts and conventions are shared where useful, not mandatory runtime
  code;
- product concepts are presumed not to exist until evidence establishes them;
- one canonical term and one production owner represent equivalent semantics;
- local, remote, and multi-run futures remain possible without being designed
  prematurely.
