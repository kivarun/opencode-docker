# Dogfood development pipeline plan

Status: **planned**

Last updated: 2026-11-05

Evidence baseline: `6b2ed6263efc8ec990dd6218c02b1434e702bf91`

## Purpose

Build a development pipeline that can safely work on `opencode-docker`, including
the orchestrator and pipeline definitions themselves. The pipeline must use
different execution profiles for planning, implementation and review, carry the
accepted run plan into the worker data plane, support real revision iterations,
and finish with the existing explicit Project Apply operation.

The current browser UI remains a diagnostic/operator surface until this pipeline
has completed a realistic dogfood UAT. UI design follows observed operator work;
it does not lead the backend work.

## Evidence from the proof-only audit

The proof-only audit at the evidence baseline established the following facts:

| Capability | As found |
| --- | --- |
| Different profile per agent state | Available |
| Different LLM server and model per profile | Available |
| Durable model/profile configuration identity | Missing; only the profile name is durable |
| Accepted task revision reaches the stage worker | Missing |
| Stage/plan/task/iteration context reaches the worker | Missing |
| Durable reviewer decision | Available |
| Operator-driven `continue-stage` / `revise-task` loop | Available |
| Automatic task-revision loop | Missing |
| Hard read-only reviewer workspace | Missing |
| General multi-task/multi-stage scheduler | Not ready |
| Pipeline definition frozen for an active run | Available |
| Profile definition frozen for an active run | Missing |
| Current browser UI can drive existing manual boundaries | Available, but diagnostic |

The decisive data-plane failure is that accepted plan and task revisions are
durable but worker-invisible. In the audit reproduction both stage activations
read the original protected pipeline input even after `revise-task`; the
`REVISION-ONE` and `REVISION-TWO` bodies existed only in run-plan manifests.

## Target topology

```mermaid
flowchart TD
    P["Planner profile"] --> I["Implementer profile"]
    I --> R["Read-only reviewer profile"]
    R -->|accepted| T["Terminal success"]
    R -->|changes requested| V["Durable task revision"]
    V --> I
```

The first useful pipeline deliberately supports one active task in one stage.
General task scheduling and arbitrary stage DAG execution are later work.

## Architectural decisions

1. **Profiles are semantic pipeline references.** Pipeline states refer to
   `architect`, `code` and `review` profiles. Deployment configuration binds
   those profiles to worker images, OpenCode configuration, providers, models,
   servers and credentials. Pipeline documents do not contain server URLs,
   credential paths or secrets.

2. **The active run remains frozen.** Pipeline identity continues to be bound by
   the execution snapshot digest. Applying a change to pipeline source must not
   alter the active run.

3. **The development pipeline is separate.** Keep `pipelines/default-v2/` as a
   minimal smoke/reference bundle. Add a separate development bundle only after
   the required runtime contracts exist.

4. **Project Apply remains explicit.** A terminal run never updates the registered
   project automatically. Review and Apply remain separate operator decisions.

5. **Hard filesystem authority is required for review.** Prompt instructions or
   OpenCode permissions do not constitute a read-only boundary.

6. **No implicit task selection.** The first stage-task data source is valid only
   when the selected stage has exactly one task. Zero or multiple tasks fail
   closed before a worker Session. This increment must not grow into a scheduler.

7. **UI work is deferred.** Record UI defects and workflow friction during UAT,
   but do not redesign the UI before the development pipeline has been exercised.

## Ordered increments

| Order | Increment | Status |
| ---: | --- | --- |
| 1 | Durable stage activation context | Planned |
| 2 | Durable profile/model epoch | Planned |
| 3 | Hard read-only reviewer | Planned |
| 4 | Separate development pipeline bundle | Planned |
| 5 | Real dogfood UAT with revision and Apply | Planned |
| 6 | UI model and implementation based on UAT evidence | Planned |

Do not combine adjacent increments merely because they touch the same runner or
state schema. Each increment needs its own red-before evidence, contract,
permanent tests and delivery report.

## Increment 1: durable stage activation context

### Goal

Make the current accepted task revision and content-free orchestration identity
part of the exact worker-visible stage activation.

### Pipeline contract

Add one explicit data-port source form, provisionally:

```yaml
inputs:
  - id: task
    source:
      stage_task: {}
```

The final spelling may change during implementation if an existing source
vocabulary has a better fit, but the following semantics are fixed:

- valid only on an agent state whose trusted execution role is `stage`;
- the task is resolved only from the trusted accepted plan and current durable
  generation/stage context;
- no HTTP, UI, CLI or worker caller supplies a task ID, revision or path;
- the selected stage must contain exactly one task;
- zero tasks or multiple tasks produce a typed failure before Session creation;
- the worker-visible file is the exact UTF-8 byte encoding of the task revision's
  `body`, not the JSON task manifest;
- the input is published through the existing activation-data substrate and
  mounted read-only;
- the binding identifies at least task ID, task revision, task-manifest digest and
  digest of the exact worker-visible bytes;
- task body content does not enter `state.json`, public projections or
  diagnostics.

The execution document gains deterministic content-free orchestration metadata
sufficient to identify the activation:

- plan revision and plan digest;
- stage ID, position and template;
- task ID, revision and manifest digest;
- generation index;
- iteration index.

Previous results, reviewer feedback, profile identity, budget policy, reviewer
filesystem authority, Apply and UI are outside this increment.

### Durable-state rule

Reuse the existing immutable activation input binding if it can prove the exact
task revision and worker-visible bytes after restart. Add a versioned state field
and schema bump only if the existing durable substrate is insufficient. Do not
add duplicate digest or parser ownership merely to create a new schema version.

### Red-before

The permanent red-before must reproduce the observed production behavior:

1. task revision 1 contains `REVISION-ONE`;
2. the first stage activation receives the original pipeline input instead;
3. `revise-task` creates task revision 2 containing `REVISION-TWO`;
4. the next stage activation still receives the original pipeline input;
5. neither execution document contains stage/task/revision/iteration identity.

### Acceptance criteria

- before revision, the stage worker sees the exact bytes of
  `REVISION-ONE`;
- after `revise-task`, the next stage worker sees the exact bytes of
  `REVISION-TWO`;
- the selected task identity is recoverable and reverified after process restart;
- missing, substituted, non-canonical or digest-divergent task manifests fail
  before Session creation;
- missing or modified activation input artifacts fail closed;
- zero-task and multi-task stages fail before Session creation;
- a non-stage state cannot declare the source form;
- no caller-controlled filesystem path reaches the resolver;
- task bodies remain absent from durable state, public HTTP/UI documents,
  content-free errors and argv;
- existing pipeline-input and state-output source forms retain their behavior;
- no new event kind, UI surface, Apply behavior or profile-binding behavior is
  introduced.

## Increment 2: durable profile/model epoch

Eliminate silent profile drift between separate mutating invocations.

The durable contract must bind a non-secret execution-profile epoch sufficient
to detect changes to the worker image and effective OpenCode/provider/model
configuration. Secrets and credential values remain outside durable state and
public documents.

Before implementation, choose and document whether the epoch is run-wide or
activation-specific. Either choice must make a post-restart execution's effective
profile identity provable and must reject silent substitution with a typed
failure. Explicit profile rebinding is not part of this increment unless
separately designed.

## Increment 3: hard read-only reviewer

Add an OS-enforced read-only project workspace for reviewer activations while
leaving implementer activations read-write.

Required properties:

- the reviewer cannot modify the workspace through edit tools, shell,
  subprocesses, direct writes, rename/unlink or symlink tricks;
- Tool Session authority must not reintroduce write access;
- the policy is selected only by trusted compiled pipeline metadata;
- callers cannot request broader authority;
- direct write attempts are covered by runtime tests;
- prompt and OpenCode permission settings remain defense-in-depth only.

Do not combine this with reviewer decision semantics; durable agent outputs and
decision states already exist.

## Increment 4: development pipeline bundle

Add a new bundle, provisionally `pipelines/development-v1/`, without expanding
`default-v2`.

Initial roles:

| State | Profile | Workspace |
| --- | --- | --- |
| Planner | `architect` | As required by the accepted planning contract |
| Implementer | `code` | Read-write |
| Reviewer | `review` | Hard read-only |

The first bundle uses one stage and one task. It must:

- consume the accepted task revision through the stage-task source;
- pass reviewer feedback through declared, digest-bound data ports;
- use the existing operator-driven `continue-stage` and `revise-task`
  controllers;
- reach terminal success only through reviewer acceptance;
- keep Project Apply explicit and post-terminal.

Automatic task rewriting, arbitrary task scheduling and multi-stage progression
remain out of scope.

## Increment 5: dogfood UAT

Run a real, moderately complex change against `opencode-docker`. The task should
touch production code, tests and documentation, but remain small enough for
manual inspection.

The UAT must traverse:

1. planning with the architect profile;
2. implementation with the code profile;
3. reviewer rejection or an intentional operator revision boundary;
4. a durable task revision;
5. a second implementation iteration that visibly consumes the revised task;
6. reviewer acceptance;
7. terminal success;
8. Project Changes and bounded content review;
9. explicit Project Apply;
10. restart and read-only recovery/inspection of the applied operation.

The scenario must guarantee a second iteration. A first-pass success is not
evidence that the revision loop works.

Capture:

- actual provider/model/profile identity per execution;
- exact task revision consumed by each iteration;
- reviewer filesystem authority;
- session cleanup;
- registered-project fingerprints before review and before Apply;
- UI actions that were unclear, missing or unnecessarily difficult.

## Increment 6: UI after dogfood

Design the operator UI from the dogfood workflow rather than extending the
current debug surface incrementally.

The UI model should be based on the observed primary operations:

- plan and task review;
- current stage/task/iteration identity;
- comparison between iterations;
- reviewer decision and feedback;
- explicit continue/revise actions;
- Project Changes/content review;
- explicit Apply and recovery state.

No UI contract is selected by this plan yet.

## Deferred work

The following are intentionally deferred until after the first development
pipeline UAT:

- multiple active tasks per stage;
- dependency scheduling;
- automatic progression across arbitrary stages;
- automatic task rewriting from reviewer output;
- automatic Apply;
- rollback;
- final-workspace sealing;
- broad UI redesign;
- migration to OpenCode V2.

## Drift-control rules

This file is the durable owner of the agreed ordering and scope.

- Update this document in the same commit that intentionally changes an
  increment's contract, ordering or status.
- Mark an increment complete only after its delivery commit, tests and CI result
  exist; add the commit SHA and evidence links to the status table.
- Implementation reports may refine evidence but must not silently expand the
  next increment.
- A later audit finding may change the plan, but the change must be explicit in
  this file before dependent implementation begins.
- Production documentation describes shipped behavior; this plan may describe
  future behavior and must remain clearly marked as such.
- Keep one production theme per increment and per commit.

## Open decisions

These are deliberately unresolved and must not be silently chosen by unrelated
work:

1. Run-wide versus activation-specific durable profile epochs.
2. Exact schema spelling for trusted per-state workspace access.
3. Whether reviewer feedback alone is sufficient for the next iteration or
   whether a separate operator-authored task revision is always required.
4. Multi-task scheduling semantics after the single-task dogfood pipeline.
5. The future UI information architecture.
