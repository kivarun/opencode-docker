# Pipeline Model

## Status

This document records the target product model for user-defined pipelines in
opencode-docker. It complements the project manifesto: the manifesto defines
principles and trust boundaries, while this document describes what a pipeline
is from the user's point of view.

The model is intentionally broader than the currently implemented execution
path. A feature described here is not considered implemented until it is
present in the production path and covered by its contracts and tests.

## Goal

The orchestrator must remain independent of any particular workflow. Users
should be able to define almost all workflow-specific behavior as declarative
data, without rebuilding the orchestrator or changing project code.

The user controls two planes:

- the **process plane**: roles, prompts, states, transitions, decisions, cycles,
  limits, and points where human input is required;
- the **data plane**: pipeline inputs, outputs, shared context, protected files,
  and accepted artifacts.

The user controls agent roles and selects their execution profiles. Each
profile may specify the agent image, model/provider configuration, and
explicit environment bindings, allowing different roles to use different
images and LLM backends without requiring the project to ship images for
every use case.

The user does not manage the container mechanics derived from those profiles.
Mount construction, fixed container paths, Session credentials, helper
transport, capability projection, and test containers launched by agents
remain runtime concerns owned by validated profiles, docker-helper, and the
orchestrator. Pipeline states refer to profiles by name and do not embed
mounts, credentials, helper endpoints, or arbitrary container commands.

## Responsibility model

| Concern | Owner |
| --- | --- |
| Workflow meaning and data contracts | Pipeline author |
| Runtime and model capability ceiling | Operator-controlled profiles |
| Run state, validation, transitions, and cleanup | Orchestrator |
| Container capability and workspace policy | docker-helper |
| One bounded unit of work | Agent |
| Task changes and unresolved decisions | User |

A lower layer may select only capabilities already offered by the layer above
it. In particular, a pipeline may reference trusted profiles but cannot widen
their authority.

## Pipeline bundle

A pipeline is a portable, versioned bundle of declarative data. The project
ships `pipelines/default/` as the canonical example and useful default. An
external bundle is loaded through the same contract; the bundled default has
no privileged execution path.

A bundle contains, conceptually:

- the state graph and its bounds;
- reusable role definitions;
- agent prompts;
- input, output, context, and result schemas;
- optional deterministic pipeline tests.

All referenced files remain confined to the bundle. A bundle cannot contain
arbitrary shell commands, JavaScript, callbacks, host paths, raw environment
variables, credentials, Docker options, or helper connection settings.

## Roles and profiles

A role describes the workflow responsibility of an agent. It combines:

- a role prompt;
- an execution profile reference;
- a model profile reference;
- the structured result contract expected from the role.

Roles are separate from states. The same role may be reused by several states,
and a pipeline author may add, remove, or rename roles without adding concepts
to the orchestrator. Names such as architect, coder, reviewer, or security
reviewer belong to the default pipeline, not to the engine.

Profiles are trusted operator configuration and are not stored in the pipeline
bundle:

- an **execution profile** selects the approved agent image, runtime limits,
  exact environment and file projection, and other execution capabilities;
- a **model profile** selects an OpenAI-compatible endpoint, model, credential
  reference, and allowed non-secret model settings.

Different roles may use different endpoints, models, and credentials. On each
activation the worker receives only the configuration and credentials required
by that role, plus its child Session capability. Credentials of other roles,
the Launcher credential, and administrative authority are never forwarded.

The supported provider boundary is the OpenAI-compatible protocol. Native
vendor-specific protocols and authentication adapters are outside the project.
A provider that does not expose a compatible interface requires an external
gateway. The minimum compatibility contract must be documented and tested
before it becomes a public promise.

## State graph

The workflow is an explicit, bounded graph. The orchestrator understands a
small set of universal state kinds rather than workflow-specific roles:

- **agent** — execute one bounded role attempt and accept a structured outcome;
- **decision** — choose a transition using deterministic conditions over
  accepted context and results;
- **action** — apply declared, safe context or data changes without running an
  agent;
- **wait** — persist a question or requirement and wait for explicit user or
  external input;
- **terminal** — finish with a declared result.

Agent output never selects an arbitrary next state. The agent returns an
outcome allowed by the current state; the orchestrator validates it and owns
the outcome-to-transition mapping.

In the executable pipeline schema (v2) this is concrete: an agent state
performs the work and has exactly one transition whose outcome is
`completed` (its target is the user's choice and may lead to an agent,
decision or terminal state), and `max_attempts` is exactly 1. The agent and
its runtime never choose an outcome or a target; content-based branching
belongs to a decision state, whose outcomes remain the declared model
outcomes. Retries are not implemented yet, so `max_attempts` must be 1
(today the production schema v1 planner enforces the same shape).

Decision conditions use a small typed expression language over declared data.
They may combine comparisons, existence checks, boolean operators, and similar
deterministic predicates. They cannot inspect ambient environment, filesystem,
network, wall-clock time, or execute code. Fuzzy judgments belong in a
structured agent result; hard limits and priority rules remain deterministic
orchestrator decisions.

Cycles are allowed only with explicit bounds. Every reachable path must either
terminate, wait for input, or remain covered by a finite transition or
iteration budget.

## Stages and tasks

A stage is a plan-level coordination boundary, not necessarily one monolithic
unit of work. Each stage contains a finite task graph. Tasks may be independent,
strictly ordered, or joined after several prerequisites.

The serialized plan may keep tasks in declaration order for stable identity and
diagnostics, but execution semantics come from explicit dependencies. A stage
with one task and no dependencies is the baseline used by the initial default
pipeline; it is a degenerate one-node task graph rather than a different model.

A task is the smallest plan-defined unit that may be assigned a role and
produce independently accepted outputs. A task is not a pipeline state: states
define the reusable control protocol, while tasks are runtime instances created
from the task plan. The same task subpipeline may be instantiated for many
tasks without generating workflow-specific states in the orchestrator.

An architect model may propose the task decomposition and dependency graph as
structured plan data. It does not schedule agents directly. Before execution,
the orchestrator validates unique task ids, dependency references, acyclicity,
bounds, input/output compatibility, and the accepted plan revision. It then
computes the ready set deterministically: tasks with no unfinished dependencies
may run subject to the declared concurrency ceiling; dependent tasks become
ready only after all required predecessors have completed successfully.

The authoritative root state machine remains ordered and single-owned. A
bounded task-DAG scheduler is a composite execution facility beneath a stage,
not a collection of model-controlled cursors. Task completion, failure,
cancellation, retry eligibility, and stage completion are recorded as
orchestrator events. A stage can close only after its required task graph has
reached an accepted state and the stage decision permits closure.

Parallel task executions require isolated writable workspaces or another
explicit synchronization boundary. They must not concurrently mutate the same
project tree by accident. Source-control-backed isolation with one worktree or
branch per task is a future integration described in
[the SCM and Git-flow draft](scm-integration-draft.md); the file-only pipeline
continues to work without any SCM provider.

Plan changes are versioned operations. Re-decomposition may add, supersede, or
invalidate pending work according to an explicit decision, but it cannot
rewrite the recorded history of completed task activations. If a proposed plan
cannot be reconciled safely with the current stage, the pipeline waits for an
architectural or user decision instead of guessing.

## Pipeline data

Inputs and outputs have logical identifiers and declared contracts. A pipeline
may define any number of task files, specifications, reports, patches, review
records, or other artifacts; `TASK.md` and the current result filename are
defaults, not engine concepts.

States declare which resources they read and produce. The orchestrator validates
presence, paths, protected-input integrity, schemas, and accepted artifacts.
These declarations define the pipeline contract and result acceptance. They do
not by themselves turn a read-write workspace into a complete per-file
sandbox; stronger filesystem enforcement belongs to the execution/runtime
boundary.

The pipeline also declares typed shared context and its initial value. Context
contains workflow-specific facts such as a stage sequence, current stage,
iteration counters, review findings, or decision flags. The orchestrator owns
the authoritative context. Agents receive only the declared view and propose
structured changes; they never mutate authoritative state directly.

Human-readable files such as `STATE.md` may be generated as compatibility
views or agent inputs, but they are not the source of truth.

## docker-helper boundary

Before using the orchestrator, the operator installs and configures
docker-helper, creates an appropriate Launcher, and provides its credential to
the orchestrator.

The orchestrator consumes Launcher authority but does not administer the
Launcher. It may inspect its identity and available capabilities and create the
child Sessions and operations allowed by its policy. It cannot create or
modify Launcher policy, allowed images, quotas, ports, credentials, or other
administrator-owned settings, and it never requires a Principal/admin
credential.

The docker-helper connection is runtime configuration, not pipeline data. For
container and UI integration the target model is a network endpoint with
explicit TLS and trust configuration plus the Launcher credential. Injecting a
host Unix socket into the orchestrator container is not part of that model.

Execution profiles must fit within the selected Launcher's policy. Validation
may confirm this compatibility, but it cannot change the policy to make a
pipeline pass.

## Validation and compilation

A pipeline is treated as a small declarative program and must be compiled
before any agent Session is created. Validation and execution use the same
compiler and the same accepted compiled representation, so a run cannot bypass
checks performed by a separate validation command.

Compilation covers, at a product level:

- syntax with precise source locations and duplicate-key rejection;
- schema version and exact structural contracts;
- bundle path confinement and local reference resolution;
- graph references, reachability, terminal/wait paths, and finite bounds;
- role, resource, context, transition, and decision consistency;
- data flow between state inputs and outputs;
- existence and compatibility of referenced profiles;
- optional runtime preflight against the configured docker-helper target.

Remote schema references and unbounded or executable expressions are not
accepted. Validation diagnostics should be stable and machine-readable as well
as useful to a person: each error identifies a code, file, location or data
path, and explanation.

Running a pipeline always repeats the relevant validation and preflight. A run
is bound to the accepted pipeline and profile definitions; it does not silently
continue under changed definitions.

## Creation wizard and defaults

The CLI should provide a pipeline creation wizard with progressive disclosure.
It asks about workflow roles, prompts, configured profile choices, inputs,
outputs, transitions, bounds, and waiting points. It never asks the user to
design mounts, container paths, raw environment projection, Session handling,
helper sockets, or secret transport.

Accepting every suggested value produces the pipeline shipped in
`pipelines/default/`. The wizard should achieve this by copying or instantiating
the canonical default bundle, not by maintaining a second implementation of
the same workflow. A compatibility test must prove that the all-default result
has the same canonical execution meaning as the packaged default.

The intended user-facing lifecycle is:

- `pipeline init` — create a bundle without overwriting an existing target;
- `pipeline validate` — compile it and report actionable diagnostics;
- `pipeline test` — execute deterministic graph scenarios with mocked agent
  outcomes and inputs;
- `pipeline preflight` — verify referenced operator/runtime capabilities;
- run — repeat compilation and preflight, then start from the accepted graph.

These command names describe the target user experience, not a claim that all
commands are already implemented.

## Testing and observation

Optional bundle tests provide mocked agent results and context, then assert the
transition trace, terminal result, or waiting state. They run against the pure
state-machine semantics and do not require an LLM or containers. The default
pipeline ships such tests as executable documentation and regression coverage.

Observation is expressed in generic pipeline terms: run, state, role, attempt,
validated outcome, transition, accepted context change, artifact, wait request,
user response, cancellation, and cleanup. No observer should need to interpret
free-form model text to determine progress.

The CLI remains sufficient for local use. A future local API and one-way event
stream may expose the same contracts to T3 or another UI. D-Bus is not a core
orchestrator-agent or orchestrator-client transport; it may only be useful at a
desktop-notification edge.

## Migration of the established default workflow

The existing `PIPELINE_RULES.md` and `PIPELINE_RULES.architect.md` are source
material for the default bundle, not future runtime instructions.

Their content is separated by ownership:

- role behavior and review criteria become role prompts;
- agent output formats become result and artifact schemas;
- the architect/coder/reviewer/security-review flow becomes the declarative
  graph;
- stage sequence, current stage, iteration counters, findings, and decision
  flags become orchestrator-owned context;
- hard constraints and decision priority become deterministic decision states;
- plans, stage contracts, reviews, warnings, and proposals remain declared
  workspace artifacts;
- agent-written `STATE.md` updates are removed from the authority path.

The migration is behavior-preserving, not a rewrite from memory. Existing rule
scenarios become default-pipeline tests, and each rule remains traceable to its
new prompt, schema, context field, or transition. Contradictions and uncovered
decision combinations are recorded explicitly and resolved deliberately; the
migration must not silently invent behavior. The old rules already contain at
least one such uncovered decision combination, so an explicit decision table
is required before declaring parity.

The [legacy pipeline oracle](pipeline-oracle/README.md) preserves the original
source pair, the extracted decision table and state effects, and formal
scenarios for this migration. It marks uncovered and ambiguous behavior
explicitly; its extraction audit is not a production parity test.

The dynamic solution-stage plan remains pipeline context generated from the
user task. It is not expanded into hard-coded engine states. The stable control
graph may therefore move among architect, implementation, review, security
review, decision, rework, wait, and terminal states while operating on a
user-defined sequence of solution stages.

## Non-goals

This model does not turn opencode-docker into:

- a docker-helper administration or policy service;
- a generic container or cluster scheduler;
- a host command runner or arbitrary-code workflow engine;
- an adapter library for every proprietary model protocol;
- a system in which prompts, agents, or UI clients can bypass authoritative
  orchestrator state;
- a mandatory web platform or distributed multi-tenant service.

The project remains a local control plane for bounded OpenCode workflows. Its
extensibility comes from portable declarative process and data contracts while
runtime authority stays narrow and operator-controlled.
