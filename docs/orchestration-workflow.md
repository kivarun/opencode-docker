# Orchestrator and Agent Workflow — Design Sketch

## Status

This document records the intended working model for the opencode-docker
orchestrator, agent workers, pipeline authors, and users.

It is a design sketch, not a claim that every described interface is already
implemented. The current implementation is the tested `smoke` and
`agent-smoke` baseline with the first trusted execution profile increment,
the durable per-run pipeline state, and the multi-state execution
substrate (one child Session per agent-state activation, per-activation
result identity, run state schema version 2).
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

`orchestrator/src/pipeline_engine.ts` implements `executePipelineGraph`, a
pure, deterministic graph execution core. Before the first callback it
compiles an immutable, engine-owned snapshot of exactly the graph data it
needs (`entry_state`, `max_transitions`, state ids/types, terminal results,
ordered transitions with original indices) and validates it fail-closed;
during execution it reads transitions, terminal results, and the budget only
from that snapshot, so mutations of the source `ResolvedPipeline` —
synchronous inside the callback or external while a callback is pending —
cannot redirect the graph. The callback receives a separate frozen, deeply
isolated, transition-free execution view (state id, profile, prompt, inputs,
and a deep-cloned, recursively frozen JSON result schema — a corrupted
non-JSON schema in an artificially damaged resolved pipeline is rejected as
`invalid_graph` before the callback) and returns only a validated outcome: it
can never select the next state. The engine starts strictly at `entry_state`, resolves
the declared transition by outcome itself, enforces `max_transitions` with a
single reachable contract — an agent state cursor with an exhausted budget
fails before the callback, and reaching a terminal exactly at the boundary
succeeds — and finishes at a terminal state with `result: success|failed`. It
returns the terminal state id, the terminal result, the applied transition
count, and an ordered transition trace (`from`, `outcome`, `to`,
`transition_index`); it contains no timestamps or randomness. At the core
level it supports sequential states, branching by outcome, and cycles bounded
by `max_transitions`. It fails closed with `PipelineExecutionError` (stable
reasons: `invalid_graph`, `missing_state`, `unknown_outcome`,
`invalid_outcome`, `transition_budget_exhausted`) when the cursor is missing,
an outcome is not declared by the current agent state, an agent state would
run with an exhausted transition budget, an outcome is empty or not a string,
or the resolved graph is internally inconsistent. A callback failure
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
compile branch as pure substrate. v2 replaces workspace paths with named
ports: run-level `inputs` are logical run inputs whose host paths are bound
later by the CLI/API (never in the document); run-level `outputs` are the
data handed back to the user after completion (`required: true` outputs
fail the run if absent at the terminal; optional outputs may be absent —
both are compile-declared); each agent state declares local input and
output ports. Port sources have exactly one form
(`{pipeline_input: ID}` or `{state_output: {state, output}}`); port types
are `file`, `directory`, or `json` (json ports require a bundle-relative
JSON schema file loaded through the same realpath containment; parsed as a
JSON object, no generic JSON Schema validation). Pipeline inputs and agent
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
a built plan. The planner is fail-closed against forged or corrupted
input: it re-validates the resolved object (exact port shapes, safe and
unique ids, known port types, intact source unions, recursively
plain-JSON schema snapshots) before any target path is built, never
trusting a TypeScript cast or a past loader pass.

Session capability contracts are fixed next to the planner, unwired:
Execution Session (`type: "execution"`, scope run root; orchestrator-only
bearer, used only to launch workers, never passed to a worker in any
form), Tool Session (`type: "tool"`, scope project only; bearer handed to
the worker; nested containers cannot reach pipeline inputs/outputs through
helper), and worker mounts (project `/workspace` RW, prepared activation
inputs `/pipeline/inputs` RO, activation outputs `/pipeline/outputs` RW).
The helper socket projection is not a session capability: it lives in the
separate immutable Worker Launch contract (`WORKER_LAUNCH_CONTRACT`)
because it happens once, at worker launch through the Execution Session —
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
(forged port ids, corrupted resolved objects, invalid schema snapshots),
socket-transport/Tool-authority separation, and the production rejection
before auth/session. The decision evaluator, durable state, lifecycle, signal
handling, helper transport, and the default bundle are untouched; wiring
v2 execution into `agent-smoke` is a later increment.

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
