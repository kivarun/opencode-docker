# Orchestrator and Agent Workflow — Design Sketch

## Status

This document records the intended working model for the opencode-docker
orchestrator, agent workers, pipeline authors, and users.

It is a design sketch, not a claim that every described interface is already
implemented. The current implementation is the tested `smoke` and
`agent-smoke` baseline with the first trusted execution profile increment,
the durable per-run pipeline state, and the multi-state execution
substrate (one child Session per agent-state activation, per-activation
result identity, run state schema version 2). Pipeline schema v2 has its
own production entrypoint now: `orchestrator run` (see the "The production
pipeline v2 CLI" section) drives the assembled pipeline v2 stack —
graph execution, the run-owned project copy, the data plane, decision
states, the Docker Helper runtime adapter, the durable state schema v5,
and output publication — through the single production runner
`runPipelineV2`. `agent-smoke` remains the v1 diagnostic command with its
own schema version 1 loader; the bundled default pipeline has not been
migrated to v2.
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
   the supported multi-state execution shape, loads and validates every
   profile named by the pipeline's agent states, canonicalizes the
   workspace, and resolves and hashes every `protected: true` input (regular
   file, workspace confinement, digest). All of that happens before Launcher
   authentication and before any child Session; a failure exits 1 without
   creating anything.
3. The orchestrator verifies its docker-helper Launcher credential.
4. For every agent-state activation the orchestrator runs one activation in
   its own child Session: durable `activation_started`, runtime-input
   existence checks, fail-closed activation control-tree preparation
   (parents re-validated, leaves created as new absent directories, the
   execution document created `O_EXCL|O_NOFOLLOW`), re-resolution of every
   declared protected input against its recorded baseline (canonical target,
   device, inode, digest), a final synchronous signal check, child Session
   creation with an immediate durable `session_created`, an image pull
   (non-fatal), durable `agent_running` after verifying the expected result
   file is still absent, the `docker-helper run` bounded by that state's
   `timeout_seconds`, full re-verification of the protected inputs' declared
   paths and filesystem identities, result verification (identity, schema,
   artifact confinement, alias protection, and a symlink-free read of the
   result file inside the exact activation leaf), durable `result_accepted`,
   the child Session delete, and durable `session_cleanup_completed`; only
   then does the validated outcome reach the graph engine, whose
   transition-commit hook records the transition and cursor before the next
   activation. Sessions are never reused between states or revisits.
5. It starts OpenCode non-interactively in the child Session. The worker is
   launched through the official docker-helper CLI 2.1.0 (`docker-helper run`,
   spawned as an argv array); until docker-helper issue #3 is implemented,
   resolved worker environment values (including secrets and the OpenCode
   config content) are visible in that CLI process's argv — a consciously
   accepted temporary risk. The agent state's `timeout_seconds` bounds the
   run: the runner sends SIGTERM at the deadline, marks the result timed out,
   and the run fails normally.
6. OpenCode reads the orchestrator-owned per-activation execution document
   (run identity, state id, activation index, attempt, input/result paths,
   allowed outcome, pipeline prompt, result format) and writes the
   activation's structured `result.json`.
  7. The orchestrator verifies the result schema (exact fields), the
     `run_id`/`state_id`/`activation_index`/`attempt` identity, artifact
     paths (workspace confinement, plus canonical-path and dev+inode alias
     checks against every protected input), and the unchanged protected
     inputs (each declared path re-resolved fresh; canonical target,
     device, inode, and digest compared against the recorded baseline). The
     graph engine maps the validated outcome through the
     state's declared transition; every accepted transition, the reached
     terminal, and the final run status are committed to the durable
     pipeline run state under the operator state root.
 8. On cancellation, the first SIGINT/SIGTERM is recorded by the lifecycle; a
    running `docker-helper run` process receives the same signal and docker-helper
    performs a bounded synchronous best-effort cancel. The orchestrator never
    confirms a terminal operation state. First-wins applies to the active
    signalable worker: the first terminal cause — runner `timeout` or
    `user_signal` — is recorded once; a user signal that wins suppresses the
    timer (`timedOut` stays false, exit 130/143), and a user signal that loses
    to the runner timer is neither delivered nor forwarded to the lifecycle
    (the run stays a timeout failure, exit 1); signals outside a worker run
    keep the plain lifecycle semantics. `session create`, `pull`, and
    `session delete` always run to completion, and the child Session is deleted
    only in the single lifecycle cleanup path after the active step settles.
   Signal acceptance closes in the same synchronous tail after cleanup and
   before the single authoritative final state write: causes are snapshotted
   and signal acceptance closes with no await in between, then exactly one
   `finalize` writes the already-decided terminal status. A signal delivered
   after the cutoff — including while the final write is in flight — is late
   and can no longer change the recorded outcome or the exit code; the
   terminal status is never rewritten.
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

- pipeline graph execution beyond the supported multi-state shape
  (content-based branching, retries, user-input states, and arbitrary JSON
  Schemas are not implemented; unsupported pipelines fail closed before any
  Session);
- arbitrary user-supplied JSON Schema validation (the result schema must equal
  the standard agent result contract verbatim);
- retries (`max_attempts` must be 1 today), resume, user input, a local
  control API, and concurrency;
- multi-process run-state coordination: two processes writing the same run in
  the same state root are not serialized across process boundaries;
- run-state migration (schema version 2 only; version 1 documents are
  rejected as unsupported and never rewritten);
- run listing or inspection commands;
- an event stream;
- a T3 integration.

## The production pipeline v2 CLI (`orchestrator run`)

`orchestrator run` is the first user-facing pipeline v2 entrypoint. The CLI
only parses arguments, resolves the trusted state-root projection from the
environment, assembles the per-call dependencies, and invokes the single
production runner `runPipelineV2` exactly once; every pipeline, profile,
authentication, run-root, durable-state, runtime, and coordination decision
lives inside the runner stack. There is no second graph loop, no second auth
layer, and no manual Session, mount, or Docker Helper argv in the CLI.

```
orchestrator run \
  --pipeline-root /absolute/pipeline-bundle \
  --config-root /absolute/operator-config \
  --project /absolute/project-source \
  [--input SAFE_ID=/absolute/source]... \
  [--launcher-id dhl_...] \
  [--json]
```

- `--pipeline-root`, `--config-root`, and `--project` are required and must
  be absolute paths. There is no default pipeline for `run`: the bundled
  pipeline is still the v1 diagnostic pipeline and is never selected by
  `run`.
- `--input` is repeatable in the form `SAFE_ID=ABSOLUTE_PATH`; the pair is
  split at the first `=`, so `=` inside the path is allowed. Zero inputs are
  valid when the pipeline declares no run inputs; the trusted loader and
  data plane verify that every declared run input is bound exactly once.
- Duplicate singleton flags (including `--launcher-id`) are rejected instead
  of silently taking the last value; `--json` takes no value and cannot
  repeat; `--workspace`, `--image`, `--profile`, `--task`, output-path flags,
  and state-root flags are rejected for `run`, as is every unknown flag.
  Parse failures exit 2 with the usage text before any authentication or
  filesystem side effect.
- The state-root projection is runtime configuration, not a per-run flag:
  the local root is `ORCHESTRATOR_STATE_ROOT`, else
  `${XDG_STATE_HOME}/orchestrator`, else
  `${HOME}/.local/state/orchestrator`; the daemon root is
  `ORCHESTRATOR_DAEMON_STATE_ROOT`, else the local root. Both variables must
  be non-empty absolute clean paths; the resolver creates and canonicalizes
  nothing, and the runner itself verifies kind, canonical form, dev/ino
  identity, and mode 0700. The paths never become pipeline fields and never
  reach a worker.
- The runner copies the project source once into the run-owned directory
  `<state-root>/pipeline-runs/<run-id>/project`; every activation mounts that
  copy read-write at `/workspace` and worker changes persist there across
  activations. The source directory is never modified, and its path never
  reaches the worker, the pipeline, the durable state, or the results.
- The durable run state is `<state-root>/pipeline-runs/<run-id>/state.json`
  (state schema version 5), and the published run outputs stay at the fixed
  location `<state-root>/pipeline-runs/<run-id>/outputs`. Run outputs are
  never copied to a user-chosen path; the CLI reports the location but does
  not relocate it.
- The pipeline's agent states select the profiles; the worker image, the
  OpenCode configuration, and the exact environment bindings belong to those
  profiles. The user never sets mounts, container paths, Session
  credentials, or the helper transport.
- Human mode prints exactly one stderr summary line — success
  `orchestrator: run ok (run <RUN_ID>, state <RUN_ROOT>/state.json, outputs
  <RUN_ROOT>/outputs)`, a post-run-root failure
  `orchestrator: run failed (run <RUN_ID>, reason <REASON>, state
  <RUN_ROOT>/state.json)` — and a failure before the run root exists relies
  on the runner's content-free diagnostics alone. `--json` writes exactly
  one JSON `PipelineV2RunOutcome` document plus a trailing newline to
  stdout (no wrapper, content-free, machine-readable for success and
  failure); the exit code is the outcome's exit code. In JSON mode every
  inherit-asked CLI call (image pull, worker run) is captured and its output
  is forwarded to stderr, so stdout stays a single JSON document; argv, env,
  and every runtime decision stay untouched.
- The signal wiring is shared with v1: the first SIGINT/SIGTERM is forwarded
  to a running worker `run` CLI process through `killActive` (first-wins),
  and the lifecycle records a user abort only when the signal may still be
  classified as one. There is no second `RunCauseGate` in the CLI.

## Pipelines (schema version 1: loader, validator, multi-state execution plan)

`orchestrator/src/pipeline.ts` implements the declarative pipeline contract:
`parsePipelineSpec(raw)` validates the structure and graph in memory,
`loadPipeline(bundleRoot)` additionally loads the bundle from an absolute
directory containing exactly `pipeline.yaml` (`pipeline.yml` and JSON are not
supported) and resolves bundle files. `planMultiStateExecution(pipeline)`
builds the fail-closed multi-state execution plan used by `agent-smoke`; the
default bundled pipeline is the production input for `agent-smoke`, and an
external bundle is selected with `--pipeline-root`.

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
- at least one terminal state exists; agent states must be reachable from the
  entry state; terminal states may sit beyond the reachable part (a cycle can
  never leave itself, and the transition budget bounds the whole run);
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

Not implemented yet for pipelines: content-based branching, arbitrary JSON
Schema support, retries, resume, user input states, API, and concurrency.

### Multi-state execution plan (implemented)

`planMultiStateExecution` accepts exactly one execution shape and rejects
every other structurally valid pipeline with a clear error before Launcher
authentication, before any child Session, and before any durable state
exists:

- any number of agent and terminal states; the entry state may be an agent
  state or a terminal state;
- sequential states, revisits of the same state, and cycles are allowed; the
  whole run is bounded by `max_transitions`;
- every agent state has `max_attempts` 1 (no retries);
- every agent state has exactly one transition, and its outcome is
  `completed` (the engine resolves the target from the declaration);
- the agent's `result_schema` equals the standard multi-state agent result
  contract (`STANDARD_AGENT_RESULT_SCHEMA`, schema version 2) as a verbatim
  structural comparison — JSON key order does not matter; no generic JSON
  Schema engine is involved;
- every `timeout_seconds` is within the single JS-timer bound
  (`MAX_RUN_TIMEOUT_SECONDS` = 2147483); larger values are rejected, never
  clamped;
- all profiles named by the agent states load and validate before
  authentication; unprotected inputs are allowed and may be created by
  earlier states.

Anything else — extra transitions, foreign outcomes, retries, custom
decision payloads — is rejected as unsupported, and unsupported graphs are
never partially executed.

Execution: the orchestrator materializes a non-secret per-activation
execution document inside the orchestrator-owned run directory of the
workspace
(`activations/<activation-index>-<state-id>/attempt-1/execution.md`: run id,
state id, activation index, attempt 1, workspace-relative input/result
paths, allowed outcome, the pipeline prompt body, and the exact result
format). The OpenCode command receives only a short static instruction
pointing at that document; prompt and input bodies never appear in argv,
env, state, or diagnostics; the pipeline bundle and config root are not
mounted into the worker. Each activation's `result.json` lives next to its
execution document, so a revisit never sees a previous activation's result
as its own.

### Pure graph engine (implemented, single transition-mapping owner)

`orchestrator/src/pipeline_engine.ts` implements the pure, deterministic
graph execution core. The execution loop exists exactly once
(`runCompiledGraph`); version-specific compilation adapters (`compileV1Graph`
over `ResolvedPipeline`, `compileV2Graph` over the provenance-checked
`ResolvedPipelineV2`) each build the same immutable, engine-owned snapshot of
exactly the graph data it needs (`entry_state`, `max_transitions`, state
ids/types, terminal results, ordered transitions with original indices, and
each state's frozen, transition-free execution view) and validate it
fail-closed; during execution the loop reads transitions, terminal results,
and the budget only from that snapshot, so mutations of the source pipeline —
synchronous inside the callback or external while a callback is pending —
cannot redirect the graph. For a v1 agent state the callback receives a
separate frozen, deeply isolated, transition-free execution view (state id,
profile, prompt, inputs, and a deep-cloned, recursively frozen JSON result
schema — a corrupted non-JSON schema in an artificially damaged resolved
pipeline is rejected as `invalid_graph` before the callback); for a v2
agent/decision state the frozen views are the exact `V2AgentExecutionView`/
`V2DecisionExecutionView` compiled at snapshot time (no transitions, targets,
agent/decision state the frozen views are the exact `V2AgentExecutionView`/
`V2DecisionExecutionView` compiled at snapshot time (no transitions, targets,
transition indexes, data ports, schemas, or credentials). A v2 agent callback
returns nothing: after it resolves, the engine applies the fixed lifecycle
outcome `completed` from its own compiled snapshot, and a value returned by
the callback against the void contract is dropped and never reaches
transition selection; only a decision executor still reports a validated
outcome. Neither executor can ever select the next state, and the
agent/decision dispatch is bound at compile time (`agent` →
`executeAgent` only, `decision` → `executeDecision` only, `terminal` → no
callback). The engine starts strictly at `entry_state`, resolves
the declared transition by outcome itself, enforces `max_transitions` with a
single reachable contract — a transition-bearing cursor (agent or decision)
with an exhausted budget fails before the callback, and reaching a terminal
exactly at the boundary succeeds — and finishes at a terminal state with
`result: success|failed`. It
returns the terminal state id, the terminal result, the applied transition
count, and an ordered transition trace (`from`, `outcome`, `to`,
`transition_index`); it contains no timestamps or randomness. At the core
level it supports sequential states, branching by outcome, and cycles bounded
by `max_transitions`. It fails closed with `PipelineExecutionError` (stable
reasons: `invalid_graph`, `missing_state`, `unknown_outcome`,
`invalid_outcome`, `invalid_executor`, `transition_budget_exhausted`) when
the cursor is missing,
an outcome is not declared by the current state, a transition-bearing state
(agent or decision) would run with an exhausted shared transition budget, an
outcome is empty or not a string, or the resolved graph is internally
inconsistent. The budget-exhaustion message names `agent state`/`decision
state` explicitly. A callback failure
propagates unchanged: no transition is recorded and the cursor does not move.
A terminal `result: failed` is a normal graph result, not an engine
exception. Free text, stdout, and exit codes never participate in transition
selection.

Production `agent-smoke` runs every step through this engine (it
is the single owner of the outcome → transition → next-state mapping; no
parallel hand-written mapping exists), restricted by
`planMultiStateExecution`. The orchestrator registers a transition-commit hook
with the engine: after the callback's outcome is validated and before the
cursor moves, the engine calls the hook with a frozen transition step; the
hook records the committed transition and, on terminal arrival, the terminal
in the durable pipeline run state. A hook failure stops the graph immediately
and propagates unchanged: a `not_committed` failure rejects before the
durable write lands, so no transition is recorded anywhere; a
`durability_unknown` failure means the rename already landed, so the new
candidate revision may already be visible on disk even though the hook
failed — the cursor never moves in either case, the next agent callback never
runs, and the run fails with exit 1 and a single cleanup. Retries, resume,
and concurrency are still not implemented. The
engine is not a second production path and not a generic workflow engine.

The pipeline's `timeout_seconds` is enforced by the CLI runner on the
signalable worker `docker-helper run` only; the deadline sends SIGTERM, the
result is marked timed out, and the run fails normally with a single cleanup.
`max_attempts` is 1, so no retries are implemented.

**Pipeline v2 through the same engine (pure adapter implemented; the production
runner uses it).** `executePipelineV2Graph(pipeline, executors)`
accepts the provenance-checked `ResolvedPipelineV2` snapshot — clones, casts,
spreads, and Proxies are rejected by `requireResolvedPipelineV2Provenance`
before any content read or callback — captures both executor functions
exactly once into an engine-owned snapshot (a missing or non-function
executor fails `invalid_executor` before compilation and before any
callback; reassigning the caller's object mid-run cannot change the
dispatch), and compiles the same engine-owned graph snapshot with one
additional state kind: an agent state binds
`executeAgent` with a frozen `V2AgentExecutionView` (type/id/profile/
prompt/timeout/attempts only), a decision state binds only `executeDecision`
with an identity-only frozen `V2DecisionExecutionView` (no model, facts,
schemas, data paths or credentials ever cross this boundary), and a terminal
state runs no callback. `PipelineV2GraphExecutors` is
`{ executeAgent: (state: V2AgentExecutionView) => void | Promise<void>;
executeDecision: (state: V2DecisionExecutionView) => string | Promise<string> }`:
an agent callback reports nothing — after it resolves, the engine applies the
fixed lifecycle outcome `completed` from its own compiled snapshot (a value
returned against the void contract is dropped and never reaches transition
selection; a defensive `compileV2AgentContract` guard repeats the loader's
agent-state contract — exactly one transition with outcome `completed`,
`max_attempts` exactly 1 — against an internally damaged agent state as
`invalid_graph` before the first callback), and only a decision executor
still reports its selected model outcome. Neither executor selects the next
state, and the engine never parses decision facts or knows the decision
table — the future production runner closes the trusted pipeline/run data
over `executeDecision` (e.g. `evaluateDecisionStateFromData(...).outcome`).
Agent and decision states share one transition budget (the exhaustion
message names the state kind explicitly), reserved decision outcomes
(`uncovered`, `inconsistent_facts`, `invalid_facts`) are routed like any
other declared outcome, and all v1 hook/budget/mutation semantics are
unchanged and covered by `orchestrator/tests/pipeline_v2_engine.test.ts`,
including a run whose decision executor is the real
`evaluateDecisionStateFromData`. The
production `agent-smoke` wiring for v2 is absent: the production loader
still rejects schema version 2 before Launcher auth and before any Session.

### Pure decision-table substrate (implemented, not yet used by the production runner)

`orchestrator/src/decision.ts` implements a pure, declarative decision-table
substrate (schema version 1): `orchestrator/src/bundle_file.ts` holds the
shared bundle-file containment helpers extracted from `pipeline.ts`
(unchanged pipeline behavior, same messages), and
`pipelines/default/decisions/architect.yaml` is the runtime decision model
mechanically transferred from the oracle
(`docs/pipeline-oracle/decision-table.json` +
`decision-vectors.json`). **The production pipeline runner does not use any
of this yet** — `pipeline.yaml`, `pipeline_engine.ts`, `pipeline_runner.ts`,
`agent-smoke`, and the durable state schema are untouched; the substrate is
not a production path, not a runner hook, and not referenced by the CLI.

A decision model is a YAML document with exact-field validation at every
level: `schema_version` (exactly 1), `facts` (non-empty list of unique safe
ids), `decisions` (non-empty list of unique safe ids), `relations` (unique
`id` + `assert`), `constraints` (unique `id` + `when` + exactly one of
`only`/`forbid` over declared decisions), `rules` (unique `id` + `when` +
one declared `decision`). Expressions are a closed boolean DSL with exactly
one node form each: `{fact: <id>, equals: true|false}`, `{all: [...]}`,
`{any: [...]}`, `{not: <expression>}` — no JavaScript, shell, callbacks,
executable strings, environment/filesystem access, arbitrary JSON
comparisons, or computed field names. Static limits (64 facts/decisions/
relations/constraints/rules, expression depth 16, 512 nodes per expression,
4096 nodes per document, 128-character ids) reject oversized documents at
compile time. The loader takes an absolute bundle root plus a clean
bundle-relative path (`*.yaml` only), enforces realpath containment with the
shared helpers (internal symlinks allowed, symlink escapes and lexical
traversal rejected, regular files only), and compiles into a deep-frozen,
engine-owned snapshot; evaluation reads only the snapshot, so later
mutations of the parsed source object cannot change results, and repeated
evaluation of the same input is structurally identical.

`evaluateDecision` is deterministic: validate the fact assignment (missing,
extra, or non-boolean facts and non-mapping inputs throw
`DecisionModelError` — they never become `uncovered`); check every
consistency relation in declaration order and fail with
`inconsistent_facts` plus all violated relation ids in declaration order;
narrow the allowed set (initially every declared decision) with each active
hard constraint in declaration order (`only` intersects, `forbid` removes)
recording `active_constraint_ids` in declaration order; then select the
first rule whose `when` is true and whose decision is still allowed — a
matched but forbidden rule never terminates the search. If no rule applies,
the outcome is `uncovered` — a normal computed result, not an exception,
warning, or fallback. Outcomes are an exact discriminated union:
`selected` (decision, rule_id, active_constraint_ids), `uncovered`
(active_constraint_ids), `inconsistent_facts` (violated_relation_ids).

The compiled default decision document holds exactly the oracle content that
is meaningful at runtime: the 11 boolean facts with their exact names, the
6 consistency relations FC1–FC6, the 5 hard constraints HC1–HC5, the 7 rules
in the original priority order, and the 7 exact decision ids, with no
automatic fallback. Source line references, explanatory text, stage effects,
state writes, recovery semantics, and the P01 user policy are deliberately
not part of the runtime document; P01 remains the next policy layer above
the base evaluator. `orchestrator/tests/decision.test.ts` proves equivalence
against the unchanged oracle: it enumerates all 2048 boolean assignments in
`bit_order` and asserts 1952 → `inconsistent_facts` (violations matching an
independent transcription), 82 → `selected` with the exact frozen expected
decisions and rule ids (checked against both the frozen vectors and an
independent table transcription), 14 → `uncovered` that never turn into a
warning or another decision, plus the exact per-decision distribution
(close_stage 14, close_stage_ignore_minor 7, rework_same_stage 3,
rework_change_stage_contract 2, rework_change_pipeline_plan 16,
architectural_proposal 32, architectural_warning 8, uncovered 14). Generic
unit tests cover arbitrary non-legacy fact/decision ids, priority order,
skipped forbidden rules, `only`/`forbid` intersection, declaration-order id
lists, fail-closed fact/reference/duplicate/field/expression errors, all
static limits, loader containment (internal symlink accepted, symlink
escape and lexical traversal rejected), snapshot immutability under source
mutation, and repeated-evaluation determinism.

### Pipeline schema v2 data ports and activation layout planner (implemented, not yet used by the production runner)

`orchestrator/src/pipeline_v2.ts` implements the `schema_version: 2`
compile branch as pure substrate. The v2 agent-state contract is fixed:
every agent state declares exactly one transition whose outcome is
`completed` (the target is the user's choice — an agent, decision or
terminal state) and `max_attempts` exactly 1; zero transitions, several
transitions, any other outcome, and `max_attempts` other than 1 are
rejected with a `PipelineError` during the trusted load before provenance
registration. An agent state performs the work; it does not manage the
state machine — content-based branching belongs to a decision state and
agent failures are not transitions. Retries are not implemented yet, so
`max_attempts` must be 1. This is the current executable semantics of
schema v2, not a rule of the default pipeline. v2 replaces workspace paths
with named ports: run-level `inputs` are logical run inputs whose host paths are bound
later by the CLI/API (never in the document); run-level `outputs` are the
data handed back to the user after completion (`required: true` outputs
fail the run if absent at the terminal; optional outputs may be absent —
both are compile-declared); each agent state declares local input and
output ports. Port sources have exactly one form
(`{pipeline_input: ID}` or `{state_output: {state, output}}`); port types
are `file`, `directory`, or `json` (json ports require a bundle-relative
JSON schema file loaded through the same realpath containment, parsed as a
JSON object, and compiled as JSON Schema Draft 2020-12 at load time — a
schema that cannot compile, or one that compiles to an asynchronous
validator, rejects the load; pipeline v2 supports synchronous validators
only). Pipeline inputs and agent
outputs are the only places that declare a type or schema: agent input
types are derived from their sources, a run output is exactly
`{id, required, source}` with type and JSON schema fully derived from the
source (declaring `type`/`schema` on a run output is rejected), and a
stricter output contract is a separate transforming state, not a
re-interpretation of the same value. The JSON contract travels by data
flow: a run-level json input keeps its resolved `schemaPath/schema`; an
agent input port sourced from it (directly or through `state_output`)
receives the derived type and the immutable schema snapshot value; schema
paths stay with the declaring sites and never enter a layout plan.
Exact-field validation applies at every level;
ids are safe and unique per collection; references to pipeline inputs,
states, and state outputs must exist; self-references and cycles between
state outputs compile (value availability is a runtime question). Runtime
semantics fixed in the module doc for later increments: a `state_output`
value is the last successfully accepted output of the named state from an
earlier activation; without one the run fails closed (no fallback); a
missing `required: true` run output fails the run at the terminal while an
optional output may be absent. There are no user port paths, no mount
targets, and `image`/`env`/`mounts`/`command`/docker/helper/session options
remain forbidden; the activation completion envelope is orchestrator-owned
and work products are the declared outputs only (no free-form artifact
list).

`loadPipelineV2` returns an engine-owned deep-frozen snapshot;
`planActivationLayout(pipeline, stateId)` is a pure deterministic planner
producing an immutable plan: ordered input ports (`source`, `type`,
optional deep-frozen schema snapshot without `schemaPath`, fixed target
`/pipeline/inputs/<id>`, `read_only: true`), ordered output ports (`type`,
optional parsed schema snapshot, fixed target `/pipeline/outputs/<id>`,
`read_only: false`), project mount `/workspace` RW,
`reject_undeclared_outputs: true` — and only logical/structural data,
never bearers, credentials, env values, host paths, or schema paths.
Mutations of the source YAML object or of the returned view cannot change
a built plan. Provenance is fail-closed before any target path is built:
`planActivationLayout` first checks membership in a module-private
`WeakSet` registry that only the fully resolved, deep-frozen snapshot
returned by `loadPipelineV2` is registered in; hand-built objects, casts,
shallow/deep clones (`structuredClone`), Proxies and corrupted objects are
rejected with a stable `PipelineError` before any field is read (getters
never run), and there is no second validation pass — structural
correctness is owned once by the compiler. `stateId` remains a plain
planner input (safe id + agent state of the trusted snapshot).

Session capability contracts are fixed next to the planner and implemented
by the Docker Helper runtime adapter: Execution Session (`type:
"execution"`, scope run root; orchestrator-only bearer, used only to launch
workers, never passed to a worker in any form), Tool Session (`type:
"tool"`, scope project only; bearer handed to the worker; nested containers
cannot reach pipeline inputs/outputs through helper), and worker mounts
(project `/workspace` RW, prepared activation inputs `/pipeline/inputs` RO,
activation outputs `/pipeline/outputs` RW). The helper socket projection is
not a session capability: it lives in the separate immutable Worker Launch
contract (`WORKER_LAUNCH_CONTRACT`) because it happens once, at worker
launch through the Execution Session —
the socket is transport, the Tool Session bearer is the worker's
authority. No wide-Tool-Session workaround exists; the docker-helper#8
allowed-roots RO/RW refinement is not required by this increment.

The production boundary stays sharp: `agent-smoke` and the current runner
execute v1 without any behavior change (v1 representation is unchanged;
`pipeline.ts` shares exact-field validators and `checkGraphShape` with v2),
v2 loads and compiles only through the pure v2 APIs, and any YAML document
with `schema_version: 2` is rejected by the production loader with exactly
`pipeline schema version 2 is not executable yet` before Launcher auth and
before any Session — no "genuine v2" shape sniffing.
`orchestrator/tests/pipeline_v2.test.ts` covers v1 regression,
compilation, exact-field/union validation, duplicate/unknown references,
schema propagation through `state_output`, schema containment, frozen
snapshots, planner determinism/fixed targets/flags, absence of secrets,
host paths, and schema paths in plans, planner fail-closed regressions
(forged port ids, hand-built/cloned/proxied objects, getters never
invoked, cyclic schemas, duplicate state casts), load-time JSON Schema
compilation (a schema that cannot compile rejects the load), and the
production rejection before auth/session. The decision evaluator, durable
state, lifecycle, signal handling, helper transport, and the default bundle
are untouched; wiring v2 execution into `agent-smoke` is a later increment.

### v2 run-input snapshots and host-side activation data layout (used by the production runner through the coordinator)

`orchestrator/src/pipeline_v2_runtime.ts` materializes the v2 data plane on
the host, without Sessions or containers. Run layout: `<runRoot>/project` is
the shared project directory of the whole run. The coordinator prepares the
run-owned copy from the caller's source directory via
`prepareRunProject(projectSourcePath, runRoot)` before the run-input
snapshot: the caller (the user today, a future API or SCM provider
tomorrow) hands over only the source directory path, the source is never
modified, its path never enters the pipeline, execution document, worker
env/argv, durable state or results, and the internal run-root layout is not
presented to the user. The copy is staged in an exclusive hidden staging
directory inside the canonical run root and published with one atomic
`rename` — real directories (0700), regular files (0600, or 0700 when the
source carries any execute bit), symlinks copied as symlinks with verbatim
target text, hidden entries including `.git` and empty directories, in
deterministic code-unit sorted order; FIFOs, sockets, devices and
kind/inode changes between scan and open fail closed; every block is fully
written by an internal write-all loop (partial writes advance the buffer
offset and file position; zero-progress or impossible write counts fail
after exactly one attempt), close errors never replace an already failing
copy, and every expected filesystem failure — source
`lstat`/`realpath`/`readdir`/`readlink`/open/read/close, destination
open/write/fsync/close, staging create/chmod/inspect, target absence
checks and the final rename — is a
`PipelineV2RuntimeError("run_input_invalid", <sanitized diagnostic>)`
whose diagnostic never contains the absolute source path, absolute paths
of its descendants, file contents or raw system error messages: source
entries are named only by their `JSON.stringify`-encoded relative path
and a safe operation class, and errno codes are rendered separately. A
failure before the rename removes exactly the created staging tree, and
only after an ownership proof succeeds — the recorded `dev`/`ino` must
still identify a real non-symlink directory canonically resolving to the
expected path inside the canonical run root; a vanished tree is left
alone, a substituted object is never removed, and a cleanup failure never
replaces the original failure. Every expected failure reason stays
`run_input_invalid`; after the rename the copy is authoritative and is
never removed by the data plane even on later run failures. Honest
boundaries: a portable `rename()` can replace a concurrently created empty
directory; there is no protection against a trusted host process mutating
the source while it is being read, and there is no crash recovery. The data
plane itself still requires the prepared `<runRoot>/project` to exist as a
real non-symlink directory before `snapshotRunInputs` is called and never
clears or copies it itself, and every activation mounts exactly this one
directory at `/workspace` read-write, so changes persist across activations —
there is no per-activation project directory. `snapshotRunInputs` binds every
declared pipeline input exactly once to an absolute host path whose real
object kind must match the declared `file`/`directory`/`json` type (`json`
must parse; the loaded schema snapshot is used, never re-read), then copies
each input into the orchestrator-owned snapshot
`<runRoot>/data/inputs/<input-id>` — files byte-for-byte, directories in
deterministic sorted order with only real directories and regular files
allowed — and records a deterministic SHA-256 digest over type, relative
paths, and content only (directory entries contribute their kind tag,
length-framed relative path, and length-framed content, so names, entry
kinds, and empty directories all change the digest). After the snapshot, user
source paths are no longer read: the returned frozen metadata (canonical run
root, inputs root, canonical shared project root, per-input paths and
digests) is the run's only data source. The frozen snapshot object is
registered in a module-private `WeakMap` together with the exact trusted
pipeline and the canonical run/project roots; `prepareActivationData` accepts
only that exact object for the same pipeline object (hand-built objects,
casts, clones, Proxies, and snapshots of another pipeline are rejected before
any field is read). Sources are never mounted directly to agents.
`prepareActivationData` then builds a fresh
`<runRoot>/activations/<index>-<state>/data/` tree per activation with
`inputs/` (declaration-order copies from the run-owned snapshot or from the
runner-owned accepted records `{state, output, activation_index, digest}` —
no user paths and no types are accepted: the type derives from the declared
output port and the object must exist at the fixed orchestrator-derived path
`<runRoot>/activations/<index>-<state>/data/outputs/<output>`; before
anything is prepared the whole accepted history is validated — every record,
including old and non-winning ones, is resolved to its fixed location and
its digest recomputed and compared, and only then is the winner per
`state`/`output` pair selected by highest activation index, so an accepted
output that changed after acceptance fails the next activation before its
leaf is created), and `outputs/` (pre-created directories for `directory`
outputs; `file`/`json` outputs absent until the worker creates them). The
activation index is globally unique within the run: any existing
`activations/` entry with the same `<index>-` prefix rejects the request
before the leaf is created. Activation data carries no project directory.
Returns exact mount descriptors: shared project → `/workspace` RW, inputs →
`/pipeline/inputs` RO, outputs → `/pipeline/outputs` RW. The prepared layout
is registered in a module-private `WeakMap` (pipeline + canonical run root)
after success, so only the exact frozen object can be handed to acceptance.
Everything is fail-closed: bindings and accepted records are validated
before any mutation, the activation index must be globally unused, the
activation leaf must be absent, symlink traps on run-owned paths are
rejected, directories are 0700 and files 0600 (worker-UID compatibility is
an explicit limitation of the next increment), failed operations remove
exactly the objects they created, existing snapshots are never overwritten,
and the shared project root is never created, modified, or removed by the
runtime. Honest boundaries: no defense against a trusted host process
mutating a source during the read, and no crash-recovery contract.

Output acceptance (`acceptActivationOutputs`) validates the finished
`outputs/` tree of one prepared activation and releases trusted accepted
records — one deep-frozen `{state, output, activation_index, digest}` per
declared output, in declaration order, with a lowercase SHA-256 digest over
the separate `pipeline-v2-output` domain and the declared type (the same
unambiguous framing as input digests: length-framed bytes for file/json;
kind tag, length-framed relative path, and length-framed file content in
code-unit sorted order for directories — names, entry kinds, and empty
directories participate; host paths, inodes, permissions, and timestamps
never do). Acceptance accepts only the exact prepared object for the same
pipeline (provenance `WeakMap`; hand-built, cloned, proxied, and
other-pipeline objects rejected before any field is read), creates, fixes,
renames and deletes nothing, re-examines the tree fresh (the worker held the
outputs root read-write), requires exactly one top-level entry per declared
output port (a missing declared output fails, any undeclared entry fails —
`reject_undeclared_outputs: true` is an enforced invariant since this
increment), requires each output to be a real non-symlink object of its
declared type whose canonical path stays inside the outputs root (`file`
read through `O_NOFOLLOW`; `json` valid and schema-conforming; `directory`
containing only real directories and regular files), and never records
paths, types, schemas, summaries, timestamps, or output content.
Diagnostics never contain file contents or JSON values: malformed JSON in
a user data value is reported as the stable, content-free message
`<what> <path> is not valid JSON` — the parser message, offending token,
position, or input fragment never enter it. A changed accepted
output is detected by digest recomputation of the whole accepted history
(including old, non-winning records) before the next activation leaf is
created. Accepted records remain runner-owned input; agent envelopes,
stdout, and worker files can never create one. Not implemented: Session
creation, mounts, worker launch, and terminal state execution are later
increments.

### Run-input integrity and run-level output publication (used by the production runner through the coordinator)

Two integrity guarantees apply before anything else happens on the v2 data
plane. First, accepted-history coherence: for every recorded
`{activation_index, state}` pair the recorded set must be exactly the
declared output ports of that agent state — one record per declared output,
no missing and no extra records. `acceptActivationOutputs` always releases a
full set, so a correct runtime path is unaffected; a partially constructed
runner history is rejected as incoherent before any location is resolved,
both before the next activation (`prepareActivationData`) and before
publication (`collectRunOutputs`). Second, run-input integrity:
`verifyRunInputsSnapshot` re-verifies the full snapshot before every
`prepareActivationData` call and before `collectRunOutputs` — every entry
must still be a real non-symlink object of its declared kind resolving
exactly to its recorded snapshot path inside the canonical run root,
directory snapshots may contain only real directories and regular files,
and every digest is recomputed with the `pipeline-v2-input` framing and
compared. A modified, relocated or escaped snapshot fails before the
activation leaf or the run-output staging tree is created; the original
user binding paths are never read again.

Run-level output publication (`collectRunOutputs`) materializes the declared
run outputs once the graph runner has reached a terminal state — deciding
whether a terminal may be entered is the graph runner's job; the function
takes no terminal id, no user paths, no types, no mounts, and no Docker
options (arity 3: trusted pipeline, trusted run-input snapshot, runner-owned
accepted history). The fixed `<runRoot>/outputs` path must be absent
beforehand: any pre-existing file, directory, or symlink fails closed
before anything is created, so a repeated call after a successful
publication is rejected and never overwrites. The whole accepted history is
validated again (shape, per-activation coherence, fixed-location
resolution, digest recomputation of every record including old non-winning
ones) before any winner is selected; there is no current-activation bound
at the terminal — activation existence is proven by resolving each record's
fixed location. Each declared run output is then resolved strictly in
declaration order: a `pipeline_input` source resolves only from the
verified run-input snapshot (a pipeline input counts as existing once its
snapshot succeeded, and its digest is re-verified); a `state_output` source
resolves only from the runner-owned winner map (highest activation index
wins). A present source is published regardless of `required`; a missing
`required: true` source fails the whole operation before publication; a
missing `required: false` source is recorded as `present: false` with no
filesystem entry — optionality never masks a damaged accepted history.
`json` sources are re-parsed and re-validated against the same
loader-compiled Draft 2020-12 schema carried by the resolved run output
(parser diagnostics stay content-free), and the original JSON bytes are
published — never a reserialization. The publication is staged in an
exclusive temporary sibling directory inside the canonical run root
(directories 0700, files 0600; only real directories and regular files —
symlinks, FIFOs, sockets, devices rejected; no shell, no `cp`, no `tar`),
every staged byte is hashed with the separate `pipeline-v2-run-output\0`
domain (same unambiguous framing as input/output digests; host paths,
inodes, permissions, and timestamps never participate), the target is
defensively re-checked absent, and a single `rename()` publishes the
finished staging tree atomically. A failure before the rename removes
exactly the staging tree — run inputs, activations, accepted outputs, and
the shared project are never modified. The returned deep-frozen
`RunOutputsSnapshot` lists one discriminated entry per declared run output
in declaration order (`id`, `type`, `required`, `present`; published
entries additionally carry the fixed orchestrator-owned `snapshot_path` and
the digest of the actually published copy) and is registered in a
module-private provenance registry only after the atomic publish succeeded
— no serializable markers; a future download/API layer can consume only
registered snapshots. Honest limitations: a trusted host process may mutate
a source during the read; an adversarially created empty directory at the
rename target could in principle be replaced; crash recovery and a
directory `rename` no-replace primitive remain absent. Implemented and used by
the production runner: terminal state execution, decision evaluator
integration, Sessions/mounts/worker launch, and output publication. Still
not implemented: resume, API/T3 download, retries, quotas, and
old-activation cleanup; `agent-smoke`'s schema version 1 loader still
rejects schema v2 before Launcher auth/session.

### Pipeline v2 deterministic decision states (used by the production runner through the coordinator)

A v2 pipeline may declare a third state type `type: decision`. A decision
state runs no container and no Session: it names a decision model (a clean
bundle-relative `.yaml` path loaded and compiled once at trusted load time
through the existing decision substrate), receives exactly one `json` input
data port whose parsed value is the boolean fact assignment, and routes the
deterministic evaluation outcome through an exhaustive transition table.
Exact fields are `id`, `type`, `model`, `inputs`, `transitions`; container
concerns (`profile`, `prompt`, `outputs`, `timeout_seconds`,
`max_attempts`, `image`, `env`, `mounts`, `command`, `credentials`,
`paths`) are unknown fields there. The input type is derived from its
source and must resolve to `json`; the user never re-declares it, and the
port keeps the normal data-port contract including the inherited JSON
schema snapshot. Model loading goes through the single
`loadDecisionModelResolved` chain: the declared bundle-relative `.yaml`
path is validated, canonicalized, read, and compiled exactly once, and the
returned pair (`modelPath`, `model`) is atomic for the caller — `modelPath`
is the canonical path the compiled model was actually read from, so there
is no symlink-retarget window between resolution and compilation. Realpath
containment is unchanged (internal symlinks allowed; escapes, traversal,
absolute paths, `~`, directories, and missing files rejected) and every
model error carries the state id. The compiled model travels deep-frozen on
the resolved state and is never re-read afterwards; states referencing one
canonical model file (for example through an internal symlink) share one
compiled snapshot via a cache local to the single `loadPipelineV2` call,
keyed by the resolved canonical path — no global mutable cache, no second
decision compiler or evaluator.

The outcome contract is explicit and closed. The reserved outcomes
`uncovered`, `inconsistent_facts`, and `invalid_facts` may never be
declared as model decision ids, and a decision state must declare exactly
one transition per model decision id plus one per reserved outcome — no
extras, no duplicates, document order preserved, several outcomes may
share a target. Every routable situation is therefore declared by the
pipeline author: there is no automatic fallback and no hidden terminal
failure. The shared graph-shape layer treats agent and decision states as
transition-bearing (terminals have none) and includes decision states in
reachability and cycle checks; the v1 representation and the production
engine are unchanged, and neither `pipeline_engine.ts` nor production
`agent-smoke` executes v2 decision states.

The pure adapter `evaluatePipelineDecisionState(pipeline, stateId, facts)`
(provenance-gated, decision states only) maps an already-parsed fact
assignment through the existing `evaluateDecision` to a deep-frozen result
union: `selected` (whose `outcome` equals the selected decision id),
`uncovered`, `inconsistent_facts` (with the violated relation ids), and
`invalid_facts` for the fail-closed fact-validation failures of the shared
evaluator. Only the evaluator's typed fact-validation error — a subclass of
its model error, distinguishable from compilation and model failures — maps
to `invalid_facts`; any other unexpected error propagates instead of being
disguised. The `invalid_facts` branch is structured and canary-free: its
`reason` is the stable code `not_mapping`, `unknown_fact`, `missing_fact`,
or `non_boolean_fact`, plus — only where the code allows it — the
model-declared `fact_id` (`missing_fact`, `non_boolean_fact`) and the
value's type name (`non_boolean_fact`). It never embeds a fact value or a
fact body, and for an unknown fact not even the provided property name is
returned or logged; the evaluator's own error message is canary-free for
the same reasons. The adapter selects no target state (targets come only from the
transition table) and contains no timestamps, randomness, stdout, LLM,
callbacks, expressions, or user code.

A pure host-side data adapter,
`evaluateDecisionStateFromData(pipeline, runInputs, acceptedOutputs,
stateId, nextActivationIndex)` in `pipeline_v2_runtime.ts`, now resolves
the `json` facts port through the existing v2 data plane and calls that
evaluator. It is read-only substrate: it creates nothing (no decision
activation leaf, no `data/inputs`, no `data/outputs`), modifies and
deletes nothing, launches no Session or container, and consumes no
activation index — `nextActivationIndex` is only the bound every accepted
record's index must stay strictly below. Execution order is strict: both
provenance gates (pipeline, then the snapshot minted for that same
pipeline object) before any field is read, then the bound, the safe state
id and the `type: "decision"` check, then the canonical run/project root
checks, then the full `verifyRunInputsSnapshot` of every run input (not
only the decision's own input; the original user binding paths are never
re-read), then the complete accepted history through the one shared
`resolveAcceptedHistory` chain also used by `prepareActivationData` and
`collectRunOutputs` — parse, per-activation coherence, fixed-location
resolution and digest recomputation of every record including old
non-winning ones, and only then highest-index winner selection. The
single input port then resolves by its declared source: a pipeline input
only from the verified snapshot (the original user binding path is never
read again) or a state output only from the verified winner map
(missing/forward/first-visit references, kind mismatches, symlinks and
digest mismatches fail before the evaluator). The JSON bytes are read
from the fixed orchestrator-owned path through `O_NOFOLLOW`, parsed with
the content-free diagnostic (`<what> <path> is not valid JSON`), and
validated against the port's loader-compiled schema snapshot. Malformed
JSON and schema failures are `PipelineError`s and never become
`invalid_facts`; only a schema-conforming value that fails the model's
fact-assignment contract yields the typed `invalid_facts`. Raw JSON
bytes, parsed facts, and fact values never appear in results, errors or
diagnostics; the deep-frozen result carries no transition target and is
never written to state. Not implemented yet: wiring decision states into
the production runner (the production loader still rejects schema v2
before Launcher auth), graph-engine integration, durable state/activation
records for decision states, an `intervention` state with pause/resume,
and treating engine-level `max_transitions` exhaustion as a decision
outcome; P01 and specific stage names stay out of the generic
orchestrator.

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
  `--profile` flag) and each state's workspace input paths come from the
  pipeline's declared inputs (no `--task` flag); an external pipeline bundle
  is selected
  with `--pipeline-root` (default: the bundled default pipeline). There is no
  `--image` flag, the worker image comes only from the selected profile.
  Plain `smoke` keeps `--image`.

Profile-selected file projections, resource limits, and profile inheritance
are not implemented yet.

## Transport boundary (direction, not implementation)

The current local mode keeps the plain docker-helper CLI over a Unix socket.
A future container/remote (T3) integration will not inject the helper socket
into worker containers: the orchestrator will receive a network endpoint and
a transport trust configuration, while the agent worker will receive only the
endpoint of its child Session together with that Session's bearer. The
network transport itself is not implemented; this section records the
direction only.

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

For `agent-smoke` this is implemented as the durable pipeline run state
(`orchestrator/src/pipeline_state.ts`, `pipeline_state_store.ts`,
`pipeline_state_sink.ts`): exactly one JSON document per run at
`<XDG state root>/pipeline-runs/<run-id>/state.json`, written and read only by
the orchestrator. The document is schema-versioned (`schema_version: 2`) and
validated in both directions with exact-field rules: unknown fields, missing
fields, and any structural inconsistency fail closed when the document is
written (pure reducer) and when it is loaded again (store loader). There is
no version migration: v1 documents are rejected as an unsupported version and
are never rewritten.

It records:

- run identity, lifecycle status (`active|success|failed|cleanup_failed`),
  lifecycle phase, and the workspace path;
- the bound pipeline identity: pipeline schema version, bundle root, the
  execution snapshot digest (canonical-JSON SHA-256 over the resolved
  pipeline bundle content, `pipeline_digest.ts`), entry state, and transition
  budget;
- every protected workspace input: declared id, workspace-relative path, and
  content digest;
- the ordered activations: a global monotonic activation index starting at 1,
  the state id, attempt (always 1 today), profile, activation phase
  (`creating_session|session_created|agent_running|result_accepted|session_cleanup_completed|failed`),
  the child Session id (recorded after creation, when the Session was
  created), the session cleanup outcome, the accepted result digest and
  artifact paths, and a normalized activation failure reason when the
  activation failed;
- the execution cursor (current state id, applied transition count);
- every committed transition: original transition index (per state), `from`,
  `outcome`, `to`, the referenced activation, the SHA-256 digest of the
  verified result bytes, and the committed artifact paths;
- the reached terminal state id and its `success|failed` result;
- a normalized, text-free failure reason (`run_errors.ts` maps the terminal
  cause to one stable reason; signals win over other causes);
- an ordered discriminated event journal (`run_created`, `activation_started`,
  `session_created`, `agent_running`, `result_accepted`,
  `session_cleanup_completed`, `activation_failed`, `transition_committed`,
  `terminal_reached`, `run_succeeded`, `run_failed`, `run_cleanup_failed`)
  with contiguous sequence numbers and timestamps; events carry the
  `state_id`, `activation_index`, `session_id`, or transition fields where
  they are relevant to the event.

It never records credentials, environment values, prompt or input bodies, the
OpenCode configuration, result summaries, or worker output; worker-visible
content is referenced by digests and paths only.

Every mutation is a pure reducer command applied to the previous snapshot.
The reducer enforces the run's shape: event successor rules, contiguous
sequences, cursor/transition/activation coherence (contiguous activation
indexes from 1, exactly one active activation, a new activation only after
the previous activation's cleanup and committed transition, a transition
referencing the accepted-and-cleaned activation whose `from` equals the
cursor, a transition that would exceed `pipeline.max_transitions` rejected,
the terminal recorded only when the cursor reaches a terminal state, and a
terminal never reachable after a cleaned activation whose transition was
never committed), and terminal-status
immutability (once the run status is `success`, `failed`, or
`cleanup_failed`, no command can overwrite it — there is no
`success → failed` rewrite and no other post-terminal mutation). The loader
enforces the same coherence fail-closed: a document with more committed
transitions than the pipeline's `max_transitions` is rejected, and every
event payload must name exactly the activation, session, transition, or
terminal record it belongs to (a mutated `state_id`, `activation_index`,
`session_id`, transition field, or terminal state id fails validation);
an entry-terminal run with zero activations and transitions stays
representable, and a failed run after a cleaned activation whose transition
was not committed due to a persistence error remains representable.

Write algorithm — one commit per state change, always in this order: create
the run directory (mode 0700, symlinked directories rejected), create the
temporary file with `O_EXCL` (mode 0600), write and `fsync` the file, rename
it atomically over `state.json`, `fsync` the directory, and unlink the
temporary file when any step before the rename failed. Before every commit
the on-disk revision must equal the revision the new snapshot was derived
from, and the new revision must be exactly one higher; the first write
refuses to overwrite an existing state. A loaded state must be a regular,
non-symlink file. The state change and the event describing it are committed
as one authoritative operation: a committed revision either contains both or
does not exist.

Commit failures are typed. A `not_committed` failure (`PipelineStateStoreError`)
happens at or before the rename: the temporary file is removed and the
previous snapshot remains authoritative byte-for-byte, so the sink can still
record a normalized failure durably at finalize. A `durability_unknown`
failure (`PipelineStateDurabilityError`, carrying the candidate revision and
snapshot) happens after a successful rename, when the post-rename directory
`fsync` fails: the candidate revision is already visible at `state.json`,
but whether the previous or the candidate revision survives a crash is not
guaranteed. The rename is never rolled back and no automatic recovery is
implemented: the sink adopts the visible candidate, poisons itself (no
further commits or finalize for that run), and the run fails with exit 1
while the Session is still cleaned up exactly once. The previous snapshot is
never claimed to have survived in that case.

Not implemented yet: resume (a fresh process cannot continue an existing
run), multi-process coordination (concurrent writers of the same run in the
same state root are not serialized across process boundaries), migration
between schema versions, post-rename durability recovery, and run listing or
inspection commands.

Files such as `STATE.md` may be generated for compatibility or human
inspection in the future, but an agent cannot advance the run by modifying
them.

### Pipeline v2 run state (state schema version 6, the production run state of pipeline v2)

`orchestrator/src/pipeline_v2_state.ts` already defines the durable run state
for pipeline schema v2 as a pure substrate (state schema version 6, with the
nested pipeline identity carrying `schema_version: 2`): logical run inputs
(id, port type, protected flag, snapshot digest), a shared contiguous
execution index covering both agent and decision executions (agent
executions reuse it as their activation index; a decision occupies an index
without an activation directory), per-execution phase records, committed
transitions that bind exactly one settled execution, the terminal, published
run outputs (present/absent variants with digests only), a normalized
failure reason, and the required wait journal `waits` — the ordered,
content-free record list of every user wait and accepted response.

One wait record (`PipelineV2WaitRecord`) carries its journal `index`
(contiguous from 1), the `transition_count` of committed graph transitions
at the moment the run entered the wait, the waiting graph `state_id`, a
normalized policy `reason` (for example the P01 reason
`stage_iteration_limit_exhausted`), the SHA-256 of the future
orchestrator-owned user request manifest, the declared actions
(`{id, to}` in declaration order; several action ids may target the same
state), and — once answered — a content-free `response` record
(`{action_id, response_sha256}`) referencing a declared action of the same
record. Both digests reserve the link to future orchestrator-owned
manifests; validating user intent (for the P01 reasons: the TASK revision,
the budget grant, and the model-profile replacements) belongs to a future
policy/controller layer that dispatches an already validated command — the
reducer itself records no user intent payload. No response body, evidence,
TASK/PLAN content, facts, paths, environment values, profiles or
credentials ever enter the document.

The 20-command union keeps the v5 successor rules and adds the explicit
user-response successor: `run_waiting {stateId, reason, requestSha256,
actions}` appends a new open wait record on a clean boundary (active
`running` run at the cursor, no terminal/outputs/failure, every execution
settled and every transition committed, no open wait) and sets
`status`/`phase` to `waiting`; `wait_response_recorded {waitIndex,
expectedRequestSha256, actionId, responseSha256}` is accepted only on a
waiting run whose last wait record is open, is exactly `waitIndex`, carries
the matching request digest and declares the action — it atomically closes
that record with the response, returns the run to `active`/`running`, and
moves `cursor.current_state` to the declared action target without touching
`cursor.transition_count`, `pipeline.max_transitions`, executions,
transitions or any prior history (no execution, transition or session is
created). After the response ordinary commands apply again under the
existing successor rules; a new execution receives the next global index,
identities are never reused, and repeated wait/response cycles — including
several waits at one `transition_count` — are valid. The loader restores
the cursor by a joint replay of both authoritative streams: starting at the
entry state, before every graph transition with the next ordinal it applies
in order all waits bound to that boundary; each wait must name the replay
cursor, a response moves the replay cursor to the declared action target,
and an open record may only end the journal (no later waits, executions or
transitions, a fully settled and committed history). The persisted cursor
must equal the replayed cursor, and wait/response never consume the graph
transition budget. The triple biconditional
(`status === "waiting"` ⇔ `phase === "waiting"` ⇔ the last wait record
exists and is open) is enforced in both directions; waiting forbids
terminal/run_outputs/failure, and a final state with an open wait is
rejected.

One agent execution records two independent, durable, non-secret session
ids following the two-session capability model: `execution_session_id`
(the orchestrator-owned Execution Session, scope `run_root`, never handed
to the worker) and `tool_session_id` (the Tool Session, scope `project`,
its bearer is the worker's only authority), plus an independent cleanup
pair `{execution, tool}` of `not_required`/`completed`/`failed` outcomes.
Global session-id uniqueness spans both fields of every execution, so an
id can never be reused — not even once as an Execution and once as a Tool
session. Bearers, endpoints, and credentials never enter the document.

The pure reducer applies the 20 commands and enforces the same shape as the
v1 state (execution only at the cursor, one in-flight execution, a new
execution only after the previous transition commit, two-slot session-id
uniqueness, the phase successor chain `started` → `data_prepared` →
`execution_session_created` → `sessions_created` → `running` →
`outputs_accepted` → `cleanup_completed`, the tool session only after the
execution session, transition only after agent cleanup or a matching
evaluated decision outcome, the transition budget, `run_waiting` only on an
active running run at the cursor with a settled, fully committed history
and no terminal/outputs/failure/open wait, one terminal at the cursor,
publication exactly once after the terminal, `run_succeeded` requiring
published outputs, a published failed terminal finalizing as `run_failed`
with `terminal_failed`, cleanup failures finalizing only as
`cleanup_failed`, and an immutable final status); the loader re-derives the
same invariants from those records in both directions. There is no
`events[]` by design: `executions`, `transitions`, `waits`, `terminal`, and
`run_outputs` are the single authoritative journal, so a later
audit/observation layer must never duplicate them as a second source of
truth. State schema versions 1, 2, 3, 4, and 5 are explicitly rejected (no
migration); the schema v2 document stays the production state of pipeline
v1, and production v2 resume is still not implemented.

P01 boundary: this increment implements only the generic durable
request/response pair and the routing to a pre-declared action. It does
not implement TASK-revision checks, additional stage iterations or budget
epochs, model-profile replacements, trusted-profile/capability validation,
architect-owned PLAN/STAGE updates, request/response manifest publication
or validation, or production resume — the corresponding P01-S05…S12 policy
validation stays unconnected, and no TASK/stage/profile/budget fields are
added to the durable state to imitate a policy owner that does not exist
yet.

### Run-owned project copy and the production-neutral coordinator (implemented, driven by the production runner)

The v2 data plane also implements the run-owned project copy:
`prepareRunProject(projectSourcePath, runRoot)` publishes an
orchestrator-owned `<runRoot>/project` copy from the caller's source
directory with one atomic rename (see the data-plane section above). The
production-neutral coordinator `coordinatePipelineV2Run` assembles the
whole v2 substrate — graph execution, the data plane, the decision
evaluator, durable state v4, the state sink and the two-session runtime —
and opens with the provenance gate: `requireResolvedPipelineV2Provenance`
is the first statement, before the sink getters, the runtime callback
capture, any filesystem operation, any state write and any Session; a
forged/cast/spread/`structuredClone`/Proxy pipeline returns
`{ok:false, reason:"internal_error", state:null}` with no side effects at
all. The coordinator takes the caller's project source directory as an
explicit `projectSourcePath` parameter: it prepares the run-owned copy
itself before the run-input snapshot (validate/capture runtime contract →
`prepareRunProject` → `snapshotRunInputs` → `create_run` → graph
execution), never accepts a ready-made `PreparedRunProject`, and keeps the
source untouched. A project preparation failure returns
`{ok:false, reason:"run_input_invalid", state:null}` with zero sink
commands and zero sessions; a copy published before a later snapshot or
`create_run` failure stays in the run root for diagnostics. The
production runner drives this coordinator from `orchestrator run`;
`agent-smoke` and the default pipeline remain on v1.

### Docker Helper 2.1.1 runtime adapter (wired through the production runner)

`orchestrator/src/pipeline_v2_docker_runtime.ts` is a real implementation of
the coordinator's two-session runtime boundary on the official
docker-helper CLI 2.1.1 — wired through the production runner (driven from
`orchestrator run`), not by `agent-smoke`, whose schema version 1 loader
keeps rejecting pipeline schema v2 before Launcher auth and before any
Session.

The factory `createDockerHelperPipelineV2Runtime` validates everything
before the first helper/filesystem side effect: the trusted pipeline
snapshot, one loaded profile per agent state, an immutable per-state
execution snapshot (profile name, image, OpenCode config content, and the
destination-sorted profile env bindings), the CLI runner, the helper
config, the minimal launcher operator environment, the launcher id
learned from `/auth` when known, and the trusted run-root projection
(`localRoot` — the canonical run root the orchestrator works on;
`daemonRoot` — the same directory by its daemon-visible absolute path;
host mode passes the same string for both). Mutating the source profile
map, the profile objects or the projection object during activations
changes nothing; user objects are never frozen or modified, and profile
secrets never appear on public objects. Before every `createChildSession`
of an activation the adapter proves the projection fail-closed: the
activation's canonical run root must equal `localRoot`, both roots must
be real non-symlink directories resolving to their declared canonical
paths with identical dev/ino, and the project, activation, data, inputs
and outputs roots plus the `.orchestrator` directory must be real
non-symlink directories and the execution document a real non-symlink
regular file, each canonically resolving to its own declared path on
both sides (a symlinked parent component redirects `realpath` and
fails) with identical dev/ino — same-kind substitutions such as a
symlinked, swapped file/directory or FIFO/socket pair are rejected. Any
divergence throws the typed `PipelineV2ProjectionError` (stable reason)
before any Session and before any helper CLI side effect. Session
workspaces are only `daemonRoot + relative(localRoot, localPath)`
translations; mount sources stay workspace-relative, so neither root path
appears in worker argv or environment, the execution document or durable
state.

Two sessions per activation, both created through the official CLI with
the launcher operator env only: the Execution Session with workspace =
canonical run root (its bearer authorizes every helper CLI call and is
never handed to the worker) and the Tool Session with workspace = the
canonical project directory (its bearer is the worker's only authority).
A mismatch between the created session's `launcher_id` and the expected
launcher id makes the adapter delete the known session itself before
throwing, with exactly one delete attempt: a confirmed delete reports
`wrong_authority` with the confirmed cleanup; a failed delete is never
swallowed — the adapter reports `cli_failure` naming the mismatch, the
session id and the explicit "cleanup could not be confirmed" fact, so a
mismatched session that could not be removed stays observable and is never
claimed to be gone. A Tool Session additionally requires the uncleaned
Execution Session of the same activation. Bearer tokens live
only in an instance-private registry keyed by the exact handle object;
handles carry the session id and lifecycle methods only.

The worker launch follows the fixed CLI 2.1.1 contract: a `pull` through
the Execution Session (a failed pull is a worker failure and never falls
back to a cached image), then one `run` with the fixed mount order
(project `/workspace` RW, activation inputs `/pipeline/inputs` RO,
activation outputs `/pipeline/outputs` RW), `--helper-socket` exactly
once, workspace-relative mount sources cross-checked against the prepared
activation, and no secret value in argv — the Tool bearer, the OpenCode
config content, and every profile env value travel only through
`--env-from` from deterministic private source variables of the run
subprocess environment. The prompt travels in the orchestrator-owned
execution document at the fixed container path
`/pipeline/inputs/.orchestrator/execution.md`; argv carries only the
static instruction to read it. A failed pull reports `worker_failed`, a
timed-out run `worker_timeout`, a nonzero exit `worker_failed`, and exit 0
`completed`; stdout never participates, and signal delivery plus 130/143
classification are owned by the coordinator signal control. Cleanup is a memoized Launcher-authority
delete — the physical delete runs at most once per session, and Tool and
Execution cleanups remain separate operations.

Known boundary: while `--env-from` keeps secret values out of the
adapter's argv, the legacy daemon-side Docker CLI may still see resolved
values in its own argv; that risk is documented, not eliminated. The
UID/GID contract (orchestrator and shipped agent images as `opencode`
1000:1000; external profile images only as operator-approved, filesystem-
compatible images) is a runtime compatibility requirement of the profile,
not a verified guarantee — the adapter cannot prove a default image UID
through CLI 2.1.1.

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
machine. The multi-state substrate is implemented today: `agent-smoke`
executes the supported multi-state shape (sequential states, revisits, and
cycles within the transition budget) through the graph engine, one child
Session per activation, per-activation result identity, and run state schema
version 2. Generalization to content-based branching, arbitrary JSON
Schemas, retries, and resume has not started.
