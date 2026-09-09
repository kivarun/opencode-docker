# Legacy pipeline oracle

This extraction freezes the established Aider-era workflow from the two
user-supplied rule files before implementation of the new default pipeline.
It is a **partial specification with explicit unresolved cases**, not a claim
that the old text defines every execution or that production supports these
scenarios today. The existing orchestrator and `pipelines/default/` are unchanged.

These files are reference data, not runtime schemas, agent instructions, or
extensions to the graph engine.

## Evidence and deliverables

| File | Purpose |
| --- | --- |
| [sources.json](sources.json) | Archive/member digests, byte/line counts and source precedence |
| [sources/PIPELINE_RULES.md](sources/PIPELINE_RULES.md) | Exact shared rules; reference id `shared` |
| [sources/PIPELINE_RULES.architect.md](sources/PIPELINE_RULES.architect.md) | Exact architect rules; reference id `architect` |
| [decision-table.json](decision-table.json) | Flags/facts, consistency relations, hard constraints and ordered predicates |
| [decision-vectors.json](decision-vectors.json) | Frozen expectations for 96 vectors satisfying the extracted relations |
| [state-transitions.json](state-transitions.json) | 16 state/procedure contracts with explicit effects and unknowns |
| [scenarios.json](scenarios.json) | 101 named decision, transition, validation, recovery, artifact and sequence scenarios |
| [specification-notes.json](specification-notes.json) | Ten open boundaries and two source-precedence resolutions |
| [policy-resolutions.json](policy-resolutions.json) | Explicit post-extraction decisions for the future default pipeline |
| [audit.py](audit.py) | Offline provenance, decision-transcription and corpus consistency audit |

References use the source id and inclusive original line numbers. Source copies
are byte-for-byte unchanged, including spelling and whitespace. Their original
trailing whitespace/blank line is deliberately retained; whitespace checks for
authored files exclude these two hash-verified evidence files. The architect
file names `PIPELINE_RULES.shared.md`; we explicitly map that name to the supplied
`PIPELINE_RULES.md`. Referenced `OUTPUT_RULES.md` and role files were not supplied.

Source precedence is retained: specific over general, architect over shared
(`architect:5–17`). Resolutions based on that rule are labelled
`resolved_by_source_precedence`, not presented as new user-approved policy.
Other gaps retain their conditions and partial effects without an invented
continuation.

## Preserved process model

**Solution stages** are named units of implementation derived from TASK and
PLAN. Their number, order and objectives vary with the work. A plan-change action
may insert/remove stages or retarget the current stage, including backwards.

**Control steps** run roles within each stage: coder, reviewer, security reviewer,
architect decision, then a separate architect control action. Stage rework repeats
this cycle. It does not create a new solution stage or mean a transport retry.

The decision step computes facts, applies constraints, selects the first
applicable decision, and fixes its effect on the current stage. It does not open
the next stage, increment the iteration, or modify the plan. These are separate
procedures (`architect:488–578,982–1012,1028–1036`). A closing decision also
archives the current contract by rename.

Returning the project to an earlier solution stage is explicit in the source
and retained in `T11` and `S03`. It does not specify undoing filesystem changes,
restoring archived contracts, or invalidating later implementation work.

## Decision table

Shorthand here is document notation, not a proposed DSL:

| Symbol | Source flag/fact | Ownership after migration |
| --- | --- | --- |
| T | `requires_task_change` | Architect semantic assessment |
| L | `iteration_limit_reached` | Stage iteration >= stage limit |
| I | `issues_exist` | Accepted findings contain an issue |
| M | `has_major_issue` | Architect-classified findings contain a non-minor issue |
| H | `has_only_minor_issues` | I and not M |
| S | `needs_stage_contract_change` | Architect semantic assessment |
| P | `needs_pipeline_plan_change` | Architect semantic assessment |
| E | `stage_cannot_continue_without_external_input` | Semantic assessment, excluding TASK change |
| N | `no_blocking_issues` | not M and not E and not T |
| C | `can_close_normally` | A and not I |
| A | Acceptance criteria satisfied | Explicit semantic fact; not an eleventh legacy flag |

Validate H, N and C against their definitions; require M implies I, not A
implies M, and E implies not T (`architect:683–773`). Other listed consistency
implications follow from these relations. JSON uses full names; vectors declare
their bit order. Semantic assessments are explicit fixture inputs.

Apply **all** active hard constraints before priority selection:

| Constraint | Condition | Effect | Source |
| --- | --- | --- | --- |
| HC1 | T | Only proposal | architect:784–795 |
| HC2 | L | Forbid same-stage and changed-contract rework | architect:799–819 |
| HC3 | M | Forbid ignore-minor close | architect:823–833 |
| HC4 | E and not T and not P and not S | Only warning | architect:837–849 |
| HC5 | not T and P | Forbid same-stage and changed-contract rework | architect:853–865 |

Then select the first matching **allowed** decision in this order:

| Priority | Decision | Applicability | Source |
| --- | --- | --- | --- |
| 1 | `close_stage` | C | F, architect:970–978 |
| 2 | `close_stage_ignore_minor` | L and not T and H and not C | E, architect:955–966 |
| 3 | `rework_same_stage` | not T and not E and not C and not P and not S and not L | B, architect:907–920 |
| 4 | `rework_change_stage_contract` | not T and not E and M and not P and S and not L | C, architect:924–937 |
| 5 | `rework_change_pipeline_plan` | not T and M and P | D, architect:941–951 |
| 6 | `architectural_proposal` | T | A, architect:895–903 |
| 7 | `architectural_warning` | HC4 condition | architect:837–849 |

HC4 is the only explicit positive warning condition. A list of decisions allowed
at the limit does not make them all applicable. There is no general fallback
warning rule. N is computed but unused in selection; the oracle does not add it.

Enumeration covers 2,048 assignments of ten flags plus A; 1,952 violate at least
one extracted relation. The remaining 96 produce:

| Result | Vectors |
| --- | ---: |
| `close_stage` | 14 |
| `close_stage_ignore_minor` | 7 |
| `rework_same_stage` | 3 |
| `rework_change_stage_contract` | 2 |
| `rework_change_pipeline_plan` | 16 |
| `architectural_proposal` | 32 |
| `architectural_warning` | 8 |
| No applicable decision (`uncovered`) | 14 |

This is finite **syntactic coverage under the stated relations**, not proof that
all 96 vectors describe realizable tasks. Questionable selected combinations
remain literal witnesses with G11 references, not approved business behavior.

Ordinary uncovered witness `D14`: a major issue remains at the iteration limit;
no TASK, plan, or contract change is needed; external input is not reported as
necessary. Rework is forbidden, close is inapplicable, and no warning matches.
Expected decision is `null`. `uncovered` is an extraction classification, not a
new production state or an agent outcome.

User policy resolution P01 covers this specific family of cases in the future
default pipeline. Unfinished major work at the iteration limit, when TASK and
the pipeline plan do not need changes, enters a `wait` state with reason
`stage_iteration_limit_exhausted`. The orchestrator exposes the bounded evidence
and waits for an explicit user decision. It does not guess whether the limit,
coder model, reviewer models, or several of them caused the failure.

The wait exposes two deterministic user intents. `continue_stage` grants a
positive number of additional iterations while the protected TASK revision stays
unchanged; the next state is coder. It may also replace trusted model-profile
bindings for future activations. `revise_task` declares an explicit TASK revision;
the next state is architect. PLAN and STAGE remain architect-owned: the user does
not edit them, and the orchestrator starts coder only after it has accepted the
architect's updated PLAN and STAGE with a fresh stage budget.

A model-profile change alone cannot release an exhausted budget. A visible
"reset" creates a new budget grant or epoch; it never erases old iteration records
or reuses activation identities. Model changes name trusted profiles. Provider
endpoints, model names, and credential values remain operator-controlled profile
data rather than user response fields. These typed intents replace the legacy
ability to edit STATE.md and TASK.md directly; the user never edits durable
orchestrator state.

## State transition contracts

Transition contracts use TR01–TR16; named scenarios use D/T/V/R/S prefixes.
Legacy field names are retained. The comma-separated `STAGE_SEQUENCE` becomes an
ordered array in the oracle; stage indexes remain 1-based. Baseline fixture values
are **test inputs**, not inferred initialization defaults.

| Contract | Required effect | Retained boundary |
| --- | --- | --- |
| TR01 Initialize | Plan, first contract and state; first stage; iteration 1; next coder | Some required fields have no defaults |
| TR02 Coder | Coder done; next reviewer | Required outputs and scope/input gates apply |
| TR03 Reviewer | Reviewer done; next security reviewer | Failed review still continues to security review |
| TR04 Security reviewer | Security reviewer done; next architect | Unsafe findings still go to architect decision |
| TR05 Decide | Only decision/status state writes; archive on close | No stage opening, plan change or iteration increment |
| TR06 Same-stage rework | Iteration +1; workers pending; next coder | Plan, contract and stage identity/sequence fixed |
| TR07 Changed-stage rework | Iteration +1; reset workers; change STAGE or PLAN | Specific procedure overrides general definition |
| TR08 Plan change | Update plan; optional sequence/target change; iteration 1; reset workers | Earlier targets allowed; index/total agree; no physical undo specified |
| TR09 Open next | Next stage/index; iteration 1; new contract; reset workers | New stage status and consumption of old decision unassigned |
| TR10 Final completion | Architect done; next user | No next stage or legacy exit-code contract |
| TR11 Block | Blocked; next user; no progression | Global recovery obligation and admission need clarification |
| TR12 Apply proposal | After external TASK change: in progress; reset workers; next coder | Recovery admission and iteration/decision handling incomplete |
| TR13 Archive | Rename STAGE to `<archive>/<stage_index>_<stage_name>.md` | Destination collision/revisit behavior unspecified |
| TR14 Enter recovery | Recovery true; next architect; stop immediately | No state repair procedure granted |
| TR15 Hold recovery | Workers cannot act; architect may only validate | Normal execution stopped |
| TR16 Exit recovery | After recovery completed with valid nonblocked state: in progress, recovery false; validate | Repair authority and next-actor reconstruction unspecified |

`state_subset` asserts only named postconditions. Omitted fields are not implicitly
preserved, reset or deleted. `state_unchanged` is explicit. Procedure cases assert
local effects after admissible invocation; they cannot override recovery/actor/
input gates. A sequence crossing a gap asserts its known prefix or effects, not
a successful full trace. No transition simulator is shipped here.

## Detection and recovery

`R01–R16` and `R24` retain missing/incoherent state, prior execution evidence with
no state, missing plan/contract/proposal, and incomplete iteration detection.
Non-architect input failures stop without writes (`V01–V05`); they do not gain
architect recovery powers.

The source's recovery gate is narrower than ordinary rework. Entry stops the
architect immediately. While recovery is true, only state validation is allowed.
Clearing it requires valid and nonblocked state, but the text does not define
who repairs the state, which evidence permits repair, or how an external block
is cleared before those checks pass.

The manifesto's general recovery principle is a target contract. It does not
establish that the old files contain a complete repair, retry, or crash-resume
algorithm. Ordinary plan rework cannot repair untrusted state simply because
its state effects look useful. Corrupt durable storage and unconfirmed commit
durability also remain separate, existing orchestrator contracts.

## Specification notes

Evidence and source ranges are recorded in `specification-notes.json`:

| Note | Status | Boundary |
| --- | --- | --- |
| G01 | Partially resolved by P01 | Exhausted unfinished major work waits for the user; other uncovered combinations remain |
| G02 | Open | Recovery repair procedure/authority undefined |
| G03 | Open | Blocked decisions, recovery writes and proposal/user admission overlap |
| G04 | Open | New-stage status and control-decision consumption/routing incomplete |
| G05 | Open | Initialization values and limit default incomplete |
| G06 | Source precedence | Specific changed-stage procedure permits STAGE or PLAN changes |
| G07 | Open | Archive rename, current-contract validity and revisits lack resolution |
| G08 | Open | Optional decision artifacts versus later requirements and report wording |
| G09 | Source precedence | Specific initialization permits grouped parts across multiple stages |
| G10 | Open | Semantic criteria and referenced role/output files not fully supplied |
| G11 | Open | Questionable literal closing cases; feasibility not established |
| G12 | Open | No global budget, physical rollback or durable recovery contract |

G06/G09 follow the existing precedence rule. P01 is an explicit new default
policy recorded without altering the extracted legacy table or vectors. The
remaining open notes do not authorize an implementer to choose a convenient
interpretation.

## Preservation map

| Legacy concern | Scenarios | New owner/representation |
| --- | --- | --- |
| Input/actor/state validation | V01–V06, V12–V18, V24; R01–R24 | Orchestrator admission and context validation |
| Output completeness and ownership | V07–V11, V19 | Result acceptance and orchestrator context changes |
| Role cycle | T03–T05, S01–S02 | Default graph and role prompts |
| Decision facts, constraints, priority/scope | D01–D31, T06–T07; P01-S01–P01-S12 | Structured facts plus deterministic policy and explicit user wait |
| Dynamic stages and rework | T01–T02, T08–T15, S03 | Default context and declared actions |
| User proposal/warning and task ownership | T16–T18, V22–V23, S04 | Artifacts, waits, user inputs and protected task |
| Reports and classification boundary | V20–V23, D24–D31 | Role prompts and result/artifact schemas |
| Recovery | R01–R24 | Orchestrator gates and an explicitly resolved policy |
| Close/archive timing | T06, T14–T15, R23, S01 | File action and separate stage progression |

Architect/coder roles, TASK, stages and the seven decisions belong to the default
bundle. The generic orchestrator owns accepted state, validation, deterministic
effects, limits and execution authority. Agents supply facts and proposed data
changes; they cannot write authoritative state or choose arbitrary targets.
Migration preserves process meaning while deliberately replacing agent-written
`STATE.md` authority.

## Audit and use

From the repository root:

```sh
python3 docs/pipeline-oracle/audit.py
```

This read-only standard-library audit checks hashes, JSON duplicate keys, source
range bounds, fixture/reference integrity and coverage of the 16 transitions.
For decisions, it compares the table with an independent direct transcription
and frozen vectors across the finite input space. The 23 named valid decision
cases have explicitly chosen expectations; malformed/inconsistent witnesses
are checked separately. It also checks that P01 points to still-uncovered legacy
cases and preserves the accepted user-wait invariants.

It neither imports production code nor runs Docker/LLMs, arbitrary expressions
or a state-machine simulator. The other named cases are formal assertions for
future implementation tests; their structure is audited, not their execution.
Passing this audit does not prove semantic judgments, settle open policy, or
demonstrate production parity.

Before implementing a path crossing an open note, record an explicit resolution
and resolved expected cases alongside the original witness. Keep extracted
behavior distinguishable from deliberate new policy. Production tests must
compare observations against these expectations, never generate expected
decisions from the code under test.
