# SCM and Git-flow integration — design draft

## Status

This document records a post-v1 design direction. SCM integration, Git-flow
policies, provider plugins, task branches, pull-request review, and parallel
task worktrees are not implemented. The examples are illustrative and do not
define a released schema.

The file-only pipeline remains a complete supported mode. A pipeline must not
require Git, GitHub, GitLab, or another hosting service unless it explicitly
selects the corresponding capability.

## Goal

The orchestrator should be able to apply a project-defined Git workflow
without hard-coding one organization's branch model. Rules such as creating
task branches from `dev`, synchronizing `dev` before a build, maintaining
release branches, or merging release fixes back into development are external
declarative input.

Provider-specific API behavior belongs in plugins. The orchestrator owns the
workflow intent, durable state, preconditions, and accepted results; agents
produce code and structured review content rather than operating repository
authority directly.

## Responsibility boundary

| Concern | Owner |
| --- | --- |
| Branching, review and integration policy | Pipeline/SCM policy author |
| Task dependencies and readiness | Orchestrator |
| Durable side-effect lifecycle and conflict routing | Orchestrator |
| Local Git and hosting-provider operations | SCM plugin |
| Tokens, identities and capability ceiling | Operator-controlled SCM profiles |
| Source changes and review content | Agent |
| Unresolved conflict or policy change | Architect/user |

The core must not know that a development branch is named `dev`, that a
release branch has a particular prefix, or that reviews use GitHub pull
requests. It knows only typed capabilities, pinned object identities,
preconditions, outcomes, and durable external references.

## Determinism boundary

Git operations are deterministic only relative to fixed repository objects,
inputs, metadata, and policy. Branch names and pull requests are mutable
external state; commit identities also include commit metadata. The
orchestrator therefore treats SCM changes as controlled side effects, not as
pure decision functions.

Every mutating operation has an explicit expected state, normally an
`expected_head_sha` or equivalent provider version. If the remote state no
longer matches, the operation returns a declared conflict outcome. It must not
force-push, silently rebase, retry against a new head, or choose a nearby
branch.

Pure pipeline decisions remain separate from SCM effects. For fixed accepted
inputs, the orchestrator deterministically decides which declared action is
next. The plugin performs that action and returns a typed observation of what
actually happened.

## External Git-flow policy

A pipeline references a declarative SCM policy or an operator-approved policy
profile. The final split between portable policy data and trusted profile data
is unresolved, but credentials and authority always remain outside the
pipeline bundle.

An illustrative policy may express:

```yaml
provider_profile: project-github

branches:
  development: dev
  release: release/*
  task: task/{task_id}

task_work:
  create_from: development
  synchronize_before_build_from: development
  integration_target: development

release_work:
  create_from: development
  fixes_merge_back_to: development
```

This syntax is not a contract. Its purpose is to show that branch topology and
merge directions are inputs rather than orchestrator constants.

Policy compilation validates branch templates, references, operation support,
credential-profile availability, and compatibility with the selected provider
plugin before any remote mutation.

## Action states and plugin capabilities

SCM operations fit the pipeline's generic action-state model. A pipeline
selects a known capability; it never supplies a shell command or executable
plugin body.

Illustrative capabilities include:

- inspect or materialize a pinned repository revision;
- create an isolated task branch and worktree;
- record an orchestrator-owned commit from an accepted change set;
- synchronize or merge fixed commit identities;
- open or update a change request;
- read review threads and required checks;
- publish structured agent review output;
- merge an accepted change request;
- remove orchestrator-owned temporary branches or worktrees.

A plugin advertises a versioned manifest, named capabilities, input and output
schemas, required credential scope, and supported idempotency semantics. The
operator installs and configures the plugin. A pipeline may reference only
capabilities and profiles that the operator has made available.

Arbitrary plugin code is never embedded in a pipeline bundle. The plugin
execution/transport mechanism is deliberately unresolved; it must preserve
credential isolation, typed data contracts, cancellation, auditability, and
bounded resource use.

## Durable side-effect lifecycle

Before dispatching a mutating SCM action, the orchestrator records:

- the action identity and idempotency key;
- the provider/profile capability selected;
- pinned input commit identities;
- the expected external state;
- a digest of the value-free request contract.

After completion it records the typed outcome, resulting object identities,
external resource ids, and relevant commit SHAs. Credentials, raw provider
responses, untrusted review bodies, and tokens are not durable control state.

On interruption or resume, the orchestrator observes the external resource by
its recorded identity before deciding whether an action completed. It does not
blindly repeat a create, push, comment, merge, or delete operation.

Remote operations expose explicit outcomes such as success, precondition
conflict, merge conflict, review required, provider unavailable, authorization
denied, and external object missing. Pipeline policy maps these outcomes to
retry, rework, wait, intervention, or failure.

## Tasks, branches and parallel work

The plan-level task graph determines concurrency. Independent ready tasks may
receive separate branches and worktrees from the same pinned integration base;
dependent tasks start only after their required predecessor outputs have been
accepted.

Each task records its repository base and produces an accepted commit SHA.
Branch names are derived from validated policy templates and orchestrator-owned
task identities. Agents do not choose branch names, integration targets, or
merge directions.

Parallel tasks must not share one writable checkout. Each receives an isolated
worktree or equivalent project snapshot. At a join, the orchestrator invokes
the declared integration policy with exact source and expected target SHAs.
A conflict is pipeline data requiring a declared resolution path, not a reason
for an implicit merge strategy.

Sequential tasks may be based on accepted predecessor commits or on an updated
integration branch according to the external policy. The task DAG determines
when work is ready; the SCM policy determines how the corresponding repository
view is materialized.

The initial default pipeline remains the one-task-per-stage case and needs no
parallel SCM behavior.

## Change requests and review data

GitHub pull requests, GitLab merge requests, and equivalent provider objects
are presentation and collaboration surfaces, not the authoritative pipeline
state.

Provider plugins normalize external data into declared JSON inputs, for
example:

- change-request identity and pinned head/base SHAs;
- changed-file metadata;
- review threads and dispositions;
- required check names and results;
- mergeability observations.

Agents receive only the views declared for their role. An agent may produce a
structured review, proposed comment, or disposition; the plugin publishes it
through a declared action after the orchestrator validates and durably accepts
the result. Free-form remote text never selects a pipeline transition.

A remote review or comment can later be edited or deleted. Such mutation does
not rewrite an already accepted pipeline decision. Durable state retains the
normalized accepted snapshot or digest plus the external ids and versions
needed for audit and subsequent observation.

## Credentials and identities

Different roles may use different SCM identities and permission ceilings:

```text
coder   -> github-coder
reviewer -> github-reviewer
release  -> github-release
```

These are operator-controlled SCM credential profiles. Tokens are resolved and
used only by the provider plugin or its trusted host. They are not mounted into
agent containers and do not appear in prompts, data ports, argv, logs, or
durable pipeline state.

The agent returns content and intent in its declared output. The orchestrator
chooses the configured credential profile for the action, and the plugin posts
or commits under that identity. Provider mechanisms capable of short-lived,
narrowly scoped credentials are preferred, but the core contract must not
depend on one vendor's authentication model.

Direct provider credentials inside an agent would be a separate privileged
capability and are not part of the baseline design.

## Plugin boundary and provider support

The core defines a provider-neutral SCM capability protocol and normalized
data contracts. GitHub may be delivered as the first reference plugin. GitLab,
Forgejo, Gitea, or another provider can implement the same relevant
capabilities without changes to the orchestrator's state machine.

Plugins do not need identical feature sets. Pipeline validation rejects a
policy requiring capabilities that the selected plugin does not advertise.
The project does not promise to implement and maintain adapters for every
provider.

Local Git and remote hosting may be separate plugins or capability groups.
That packaging choice remains open; the logical boundary between repository
operations and provider API operations is retained either way.

## Security and cleanup

An SCM integration must preserve these constraints:

- pipeline data cannot widen an installed plugin or credential profile's
  authority;
- every writable repository/worktree path is orchestrator-owned and confined;
- only accepted project changes enter an orchestrator-created commit;
- no force update or destructive remote deletion occurs without an explicit
  policy capability and exact precondition;
- cleanup targets only resources whose ownership is proven;
- provider output and review text are untrusted data;
- plugin failure cannot mutate the deterministic decision table or durable
  history retroactively.

## Relationship to overlays

Pipeline overlays may add SCM-aware states, task policy, or review inputs
without copying the entire default pipeline. Overlay composition still occurs
before the run and produces one fully validated immutable effective pipeline.
It cannot change Git-flow policy or plugin authority after execution begins.

The testing overlay is a useful future demonstration: one placement can run a
tester in a task branch before reviewers, while another can publish the same
structured test report after reviewers at the end of an iteration.

## Open questions

The following are intentionally deferred:

- the exact SCM policy schema and its split from trusted profiles;
- the plugin process/transport and installation model;
- the minimum provider-neutral capability set;
- commit author/committer identity and timestamp policy;
- change-set validation before an orchestrator-owned commit;
- branch/worktree lifetime and cleanup after partial failure;
- integration ordering for several simultaneously completed tasks;
- resume and reconciliation when external objects changed while the
  orchestrator was offline;
- whether the first reference implementation includes local Git and GitHub in
  one plugin or two.

This work starts after the file-based v1 pipeline and pipeline-v2 execution
contract are stable. It must be layered on top of them rather than replacing
their provider-independent process and data model.
