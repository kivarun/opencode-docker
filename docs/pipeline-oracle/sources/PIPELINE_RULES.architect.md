# PIPELINE RULES

## 0

### Rule sources

Use:
- PIPELINE_RULES.shared.md
- PIPELINE_RULES.architect.md

Treat both as one rule set.

Merge rules by meaning.

Priority:
- specific rule > general rule
- architect rules > shared rules

### Modification scope

Architect MUST treat the following files as read-only:

- PIPELINE_RULES.md
- PIPELINE_RULES.architect.md
- OUTPUT_RULES.md
- role files

Architect MUST NOT modify or propose changes to these files.

If output contains modifications to any of these files:
- the step is INVALID

---

## 1 Terminology

### Solution architecture

Solution architecture MUST NOT:

- describe pipeline execution
- describe agent behavior
- describe step sequencing

Architect MUST:

- derive solution architecture from TASK.md
- derive pipeline plan from solution architecture
- ensure stage contracts implement parts of the solution architecture


### Full STATE.md validity (VISIBLE ONLY TO: architect)

Full STATE.md validity is the complete validation of STATE.md.

Full STATE.md is valid if:

- all required fields exist
- all fields have allowed values
- all field relationships defined in this document are satisfied


### STAGE_STATUS (VISIBLE ONLY TO: architect)

STAGE_STATUS is a field in STATE.md.

STAGE_STATUS defines the current lifecycle status of the current stage.

STAGE_STATUS:

- MUST describe the actual state of the current stage
- MUST use only allowed values

---

### CURRENT_STAGE (VISIBLE ONLY TO: architect)

CURRENT_STAGE is a field in STATE.md.

CURRENT_STAGE identifies the stage currently being executed.

CURRENT_STAGE:
- MUST match a value from STAGE_SEQUENCE
- MUST match STAGE.md

CURRENT_STAGE may change ONLY:
- when opening a first stage
- after completing the current stage
- during rework_change_pipeline_plan

---

### STAGE_SEQUENCE (VISIBLE ONLY TO: architect)

STAGE_SEQUENCE is a field in STATE.md.


STAGE_SEQUENCE:

- MUST contain all stages of the pipeline
- MUST define the execution order of stages

----

### CURRENT_STAGE_INDEX (VISIBLE ONLY TO: architect)

CURRENT_STAGE_INDEX is a field in STATE.md.

CURRENT_STAGE_INDEX identifies the position of CURRENT_STAGE in STAGE_SEQUENCE.

CURRENT_STAGE_INDEX:

- MUST match the position of CURRENT_STAGE in STAGE_SEQUENCE
- MUST be within bounds of STAGE_SEQUENCE
- MUST use 1-based indexing

---

### TOTAL_STAGES (VISIBLE ONLY TO: architect)

TOTAL_STAGES is a field in STATE.md.

TOTAL_STAGES defines the total number of stages in STAGE_SEQUENCE.

TOTAL_STAGES:

- MUST equal the number of stages in STAGE_SEQUENCE
- MUST match the last valid value of CURRENT_STAGE_INDEX

---
### STAGE_ITERATION (VISIBLE ONLY TO: architect)

STAGE_ITERATION is a field in STATE.md.

STAGE_ITERATION tracks how many times the current stage has been executed.

STAGE_ITERATION:

- MUST start from 1 when a stage is opened
- MUST increase by 1 on each stage rework

---
### RECOVERY_MODE (VISIBLE ONLY TO: architect)

RECOVERY_MODE is a field in STATE.md.

RECOVERY_MODE indicates that the pipeline is in recovery state.

RECOVERY_MODE:

- MUST be set to true when pipeline state is invalid or blocked
- MUST be set to false only after recovery is completed

---

### ARCHITECT_DECISION (VISIBLE ONLY TO: architect)

ARCHITECT_DECISION is a field in STATE.md.

ARCHITECT_DECISION defines the current decision state for the current stage.

ARCHITECT_DECISION:

- MUST use only allowed values
- MUST reflect the actual architect decision for the current stage

---

### 2.4 Architect steps

The architect step, architect decision step, and architect progression rules are defined in Sections 7 and 8.

---
## 4.1 Recovery mode update rule

RECOVERY_MODE is controlled by architect.

Architect MUST set RECOVERY_MODE to true if:

- STAGE_STATUS = blocked
- OR STATE.md is invalid

While RECOVERY_MODE = true:

- architect MAY act only to validate current state
- architect MUST NOT advance pipeline execution

Architect MUST set RECOVERY_MODE to false only if:

- STATE.md is valid
- STAGE_STATUS != blocked

If RECOVERY_MODE = true and recovery is completed:

- architect MUST:
  - set STAGE_STATUS to in_progress
  - set RECOVERY_MODE to false
  - set NEXT_AGENT according to transition rules
  - validate full STATE.md validity

---

### 6.1 ARCHITECTURE_WARNING.md

ARCHITECTURE_WARNING.md is produced when the current implementation violates the pipeline plan.

ARCHITECTURE_WARNING.md MUST:

- describe violations of TASK.md
- be based on:
  - TASK.md
  - STAGE.md
  - stage artifacts

ARCHITECTURE_WARNING.md MUST contain:

- Scope
- Violations
- Impact
- Recommendation

Scope MUST:

- confirm that evaluation is against TASK.md

Violations MUST:

- describe mismatches with TASK.md

Impact MUST:

- describe why the violation matters

Recommendation MUST:

- describe what needs to be fixed

---

### 6.2 ARCHITECTURE_PROPOSAL.md

ARCHITECTURE_PROPOSAL.md is produced when changes to the pipeline plan are required.

ARCHITECTURE_PROPOSAL.md MUST:

- propose changes to TASK.md
- be based on current implementation and constraints

ARCHITECTURE_PROPOSAL.md MUST contain:

- Scope
- Problem
- Proposed change
- Rationale

Scope MUST:

- confirm that the proposal affects TASK.md

Problem MUST:

- describe the limitation or conflict

Proposed change MUST:

- describe exact modification to TASK.md

Rationale MUST:

- justify the change

---

### 6.3 Output rules

Architect MAY produce:

- ARCHITECTURE_WARNING.md
- ARCHITECTURE_PROPOSAL.md

Architect MUST:

- produce STATE.md in every step

All architect outputs MUST:

- reflect the actual state of the system
- be consistent with STATE.md

---
## 7 Architect section (VISIBLE ONLY TO: architect)


### 7.0 Architect step

#### Input:

- TASK.md
- STATE.md (optional)
- PLAN.md (optional)
- STAGE.md (optional)
- ARCHITECTURE_PROPOSAL.md (optional)
- ARCHITECTURE_WARNING.md (optional)

---
#### Input validation action

Architect MUST:

- verify that TASK.md exists

If STATE.md does not exist:

- architect MUST verify that the pipeline has not already been started

If STATE.md does not exist and any of the following files exist:

- PLAN.md
- STAGE.md
- REVIEW.md
- SECURITY_REVIEW.md
- ARCHITECTURE_PROPOSAL.md
- ARCHITECTURE_WARNING.md

Then:

- Enter recovery mode

If STATE.md exists:

- architect MUST:
  - validate full STATE.md validity
  - verify that NEXT_AGENT = architect
  - verify that ARCHITECT_DECISION exists

If any of these conditions is not satisfied:

- Enter recovery mode

If STATE.md exists and PLAN.md does not exist:

- Enter recovery mode

If ARCHITECT_DECISION = rework_change_stage_contract and STAGE.md does not exist:

- Enter recovery mode

If ARCHITECT_DECISION = architectural_proposal and ARCHITECTURE_PROPOSAL.md does not exist:

- Enter recovery mode

---

#### Actions

##### Common actions

Architect MUST:

- read STATE.md if it exists
- update PLAN.md only if required by the current execution path
- update STAGE.md only if required by the current execution path
- update architect-owned STATE.md fields required for execution

---

##### Pipeline initialization path

If STATE.md does not exist:

- Architect MUST:
  - Initialize pipeline

---

##### Rework paths

If STATE.md exists and ARCHITECT_DECISION = rework_same_stage:

- Architect MUST:
  - Start same-stage rework

If STATE.md exists and ARCHITECT_DECISION = rework_change_stage_contract:

- Architect MUST:
  - Start changed-stage rework

If STATE.md exists and ARCHITECT_DECISION = rework_change_pipeline_plan:

- Architect MUST:
  - Apply pipeline plan change

---

##### Close paths

If STATE.md exists and ARCHITECT_DECISION is one of:

- close_stage
- close_stage_ignore_minor

Then:

- If a next stage exists:
  - Architect MUST:
    - Open next stage
- If no next stage exists:
  - Architect MUST:
    - Complete final stage

---

##### External-block paths

If STATE.md exists and ARCHITECT_DECISION = architectural_warning:

- Architect MUST:
  - Keep pipeline blocked

If STATE.md exists and ARCHITECT_DECISION = architectural_proposal:

- If proposed TASK.md changes are not yet applied:
  - Architect MUST:
    - Keep pipeline blocked

- If proposed TASK.md changes are applied:
  - Architect MUST:
    - Apply proposal changes

---

#### Output

Architect MUST produce:

- STATE.md

Architect MAY produce:

- PLAN.md
- STAGE.md

PLAN.md and STAGE.md MUST be produced if they are modified by the step

Architect MUST NOT produce:

- REVIEW.md
- SECURITY_REVIEW.md
- implementation files

---

#### Output validation action

Architect MUST ensure:

- STATE.md is present in output
- ARCHITECT_STATUS = done if the step completed normally
- NEXT_AGENT is set according to the executed path

If a procedure from Section 8.2 was executed:

- all required state changes defined by that procedure MUST be applied

If Open next stage was executed:

- CURRENT_STAGE MUST match STAGE.md
- CURRENT_STAGE_INDEX MUST be valid
- STAGE_ITERATION MUST be 1

If Start same-stage rework or Start changed-stage rework was executed:

- CURRENT_STAGE MUST remain unchanged
- STAGE_ITERATION MUST be increased by 1

If Complete final stage was executed:

- NEXT_AGENT MUST be set to user

If Keep pipeline blocked was executed:

- pipeline MUST NOT be advanced

Output validation MUST satisfy the Valid step definition (Section 1)

---

## 7.1 Architect decision step

#### Input:

- TASK.md
- PLAN.md
- STAGE.md
- STATE.md
- REVIEW.md
- SECURITY_REVIEW.md

---

#### Input validation action

Architect MUST:

- validate full STATE.md validity
- verify that all required input files exist

Architect MUST verify that the iteration is complete:

- NEXT_AGENT = architect
- CODER_STATUS = done
- REVIEWER_STATUS = done
- SEC_REVIEWER_STATUS = done
- STAGE_STATUS = in_progress

If any condition is not satisfied:

- Enter recovery mode

---

#### Actions

Architect MUST:

1. compute decision flags (Section 7.5)
2. apply hard constraints (Section 7.6)
3. select decision using priority order (Section 7.7)

Architect MUST:

- set ARCHITECT_DECISION
- set STAGE_STATUS according to Section 7.3

If ARCHITECT_DECISION is one of:

- architectural_warning
- architectural_proposal

Then Architect MAY additionally produce:

- ARCHITECTURE_WARNING.md
- ARCHITECTURE_PROPOSAL.md

---

#### Output

- STATE.md

Architect MAY produce:

- ARCHITECTURE_WARNING.md
- ARCHITECTURE_PROPOSAL.md

---

#### Output validation action

Architect MUST ensure:

- ARCHITECT_DECISION is set
- STAGE_STATUS is set

- ARCHITECT_DECISION and STAGE_STATUS MUST be consistent (Section 7.3)

- no state changes are performed except:
  - setting ARCHITECT_DECISION
  - setting STAGE_STATUS

If ARCHITECT_DECISION is one of:

- close_stage
- close_stage_ignore_minor

Then:

- Archive stage contract

Output validation MUST satisfy the Valid step definition (Section 1)

---

### 7.2 Allowed state fields values

This section defines allowed values for architect-only state fields.

---

STAGE_STATUS:

- pending
- in_progress
- completed
- failed
- blocked

---

ARCHITECT_DECISION:

- architectural_proposal
- architectural_warning
- rework_change_pipeline_plan
- rework_change_stage_contract
- rework_same_stage
- close_stage_ignore_minor
- close_stage
- none

---

### 7.3 State consistency rules

This section defines consistency rules for architect-only state fields.

---

STAGE_STATUS:

- MUST reflect the actual lifecycle state of the current stage
- MUST be consistent with ARCHITECT_DECISION

---

ARCHITECT_DECISION:

- MUST reflect the actual decision taken for the current stage
- MUST be consistent with STAGE_STATUS

---

Consistency constraints:

If ARCHITECT_DECISION is:

- close_stage
- close_stage_ignore_minor

Then:

- STAGE_STATUS MUST be set to completed

---

If ARCHITECT_DECISION is:

- architectural_proposal
- architectural_warning

Then:

- STAGE_STATUS MUST be set to blocked

---

If ARCHITECT_DECISION is:

- rework_same_stage
- rework_change_stage_contract
- rework_change_pipeline_plan

Then:

- STAGE_STATUS MUST NOT be completed

---

### 7.4 Decision model

Architect MUST NOT select a decision directly.

Architect MUST:

1. compute decision flags
2. apply hard constraints
3. select decision using strict priority order

Any deviation from this model makes the step INVALID.

---

### 7.5 Decision flags

Issue classification is performed by architect during decision flag computation.

---

Architect MUST compute the following boolean flags:

---

requires_task_change:

- true if resolving the issue requires modifying TASK.md

---

iteration_limit_reached:

- true if STAGE_ITERATION >= MAX_STAGE_ITERATIONS

---

issues_exist:

- true iff at least one issue exists

---

has_major_issue:

- true iff at least one non-minor issue exists

---

has_only_minor_issues:

- true iff:
  - issues_exist = true
  - has_major_issue = false

---

needs_stage_contract_change:

- true if the issue can be resolved by modifying the current STAGE.md
- does NOT require changing pipeline structure or execution path

---

needs_pipeline_plan_change:

- true if the issue cannot be correctly resolved within the current pipeline execution path
- requires modification of PLAN.md and/or STAGE_SEQUENCE
- may involve:
  - inserting new stages
  - removing or restructuring future stages
  - retargeting CURRENT_STAGE (including rollback)

---

stage_cannot_continue_without_external_input:

- true if stage cannot continue or complete without external input
- MUST NOT require TASK.md change

---

no_blocking_issues:

- true iff:
  - has_major_issue = false
  - stage_cannot_continue_without_external_input = false
  - requires_task_change = false

---

can_close_normally:

- true iff:
  - stage acceptance criteria are satisfied
  - issues_exist = false

---

### Flag consistency rules

- can_close_normally = true ⇒ issues_exist = false
- has_only_minor_issues = true ⇒ can_close_normally = false
- has_major_issue = true ⇒ has_only_minor_issues = false
- if stage acceptance criteria are not satisfied ⇒ has_major_issue = true
- needs_pipeline_plan_change = true ⇒ stage-level rework decisions are invalid

---


### 7.6 Hard constraints

Hard constraints MUST be applied before decision selection.

---

#### Constraint 1 — TASK.md change dominates

If:

- requires_task_change = true

Then:

- ONLY valid decision is:
  - architectural_proposal

All other decisions are forbidden.

---

#### Constraint 2 — Iteration limit override

If:

- iteration_limit_reached = true

Then:

Forbidden decisions:

- rework_same_stage
- rework_change_stage_contract

Allowed decisions:


- close_stage
- close_stage_ignore_minor
- rework_change_pipeline_plan
- architectural_proposal
- architectural_warning

---

#### Constraint 3 — Major issues forbid ignore-minor close

If:

- has_major_issue = true

Then:

Forbidden decision:

- close_stage_ignore_minor

---

#### Constraint 4 — External input without TASK change

If:

- stage_cannot_continue_without_external_input = true
- requires_task_change = false
- needs_pipeline_plan_change = false
- needs_stage_contract_change = false

Then:

- ONLY valid decision is:
  - architectural_warning

---

#### Constraint 5 — Pipeline plan change dominates stage-level rework

If:

- requires_task_change = false
- needs_pipeline_plan_change = true

Then:

Forbidden decisions:

- rework_same_stage
- rework_change_stage_contract

---

### 7.7 Decision selection

Architect MUST evaluate decisions in the following order:

1. close_stage
2. close_stage_ignore_minor
3. rework_same_stage
4. rework_change_stage_contract
5. rework_change_pipeline_plan
6. architectural_proposal
7. architectural_warning

The FIRST applicable decision MUST be selected.

Architect MUST NOT:

- skip higher-priority applicable decisions
- select multiple decisions
- invent new decisions

---

### 7.8 Decision rules

---

#### Rule A — TASK.md change

If:

- requires_task_change = true

Then:

- decision = architectural_proposal

---

#### Rule B — Rework same stage

If:

- requires_task_change = false
- stage_cannot_continue_without_external_input = false
- can_close_normally = false
- needs_pipeline_plan_change = false
- needs_stage_contract_change = false
- iteration_limit_reached = false

Then:

- decision = rework_same_stage

---

#### Rule C — Rework with stage contract change

If:

- requires_task_change = false
- stage_cannot_continue_without_external_input = false
- has_major_issue = true
- needs_pipeline_plan_change = false
- needs_stage_contract_change = true
- iteration_limit_reached = false

Then:

- decision = rework_change_stage_contract

---

#### Rule D — Rework with pipeline plan change

If:

- requires_task_change = false
- has_major_issue = true
- needs_pipeline_plan_change = true

Then:

- decision = rework_change_pipeline_plan

---

#### Rule E — Close ignoring minor issues

If:

- iteration_limit_reached = true
- requires_task_change = false
- has_only_minor_issues = true
- can_close_normally = false

Then:

- decision = close_stage_ignore_minor

---

#### Rule F — Normal close

If:

- can_close_normally = true

Then:

- decision = close_stage

---

### 7.9 Decision scope

Architect decision applies to the current stage only.

Architect decision MUST:

- define the outcome of the current stage

Architect decision MUST NOT:

- open the next stage
- modify STAGE_SEQUENCE
- change CURRENT_STAGE
- create the next STAGE.md

Stage transition and next stage initialization are performed in a separate architect step.

---

### 7.10 Stage close semantics

If decision is:

- close_stage
- close_stage_ignore_minor

Then:

- STAGE_STATUS MUST be set to completed

No other state transitions are allowed in the same step.

---

### 7.11 Closed stage

If STAGE_STATUS is completed:

- the current stage is considered finished

---

## 8 Pipeline execution model (VISIBLE ONLY TO: architect)

### 8.1 Definitions

#### Decision

Decision is the value of ARCHITECT_DECISION selected by the architect decision step.

Decision:

- defines the outcome of the current stage
- does NOT perform any actions by itself
- MUST be interpreted and executed in the architect step

---

#### Decision class

Decision class is a classification of decisions by their effect on the pipeline.

Decision classes:

- close
- stage_rework
- pipeline_rework
- external_block

Mapping:

- close:
  - close_stage
  - close_stage_ignore_minor

- stage_rework:
  - rework_same_stage
  - rework_change_stage_contract

- pipeline_rework:
  - rework_change_pipeline_plan

- external_block:
  - architectural_proposal
  - architectural_warning

---

#### Stage rework

Stage rework is a repeated execution of the current stage without changing its position in the pipeline.

Stage rework:

- MUST NOT modify STAGE_SEQUENCE
- MUST NOT change CURRENT_STAGE
- MUST increase STAGE_ITERATION

---

#### Stage contract change

Stage contract change is a modification of the requirements of the current stage.

Stage contract change:

- MUST modify STAGE.md
- MAY modify PLAN.md
- MUST NOT modify STAGE_SEQUENCE
- MUST NOT change CURRENT_STAGE

---

#### Pipeline plan change

Pipeline plan change is a modification of the pipeline structure.

Pipeline plan change:

- MUST modify PLAN.md
- MAY modify STAGE_SEQUENCE
- MAY change CURRENT_STAGE
- MAY change CURRENT_STAGE_INDEX
- MAY introduce new stages or remove existing ones

---

#### Close decision

Close decision is a decision that completes the current stage.

Close decision:

- MUST set STAGE_STATUS to completed
- marks the current stage as finished
- does NOT define how the next stage is opened

---

#### External block

External block is a situation where the pipeline cannot continue without external input.

External block:

- cannot be resolved within the current pipeline
- requires external action or clarification
- may result in:
  - architectural_proposal
  - architectural_warning

--- 

#### Iteration boundary

Iteration boundary is the point at which the current stage iteration is completed and a decision is fixed.

Iteration boundary:

- occurs at the architect decision step
- defines the outcome of the current stage iteration
- separates consecutive executions of the same stage

---

### 8.2 Procedures

#### Enter recovery mode

Enter recovery mode is a procedure used when pipeline state is INVALID.

Procedure result:

- RECOVERY_MODE MUST be set to true
- NEXT_AGENT MUST be set to architect
- architect MUST stop immediately
- architect MUST NOT perform further actions

---
#### Initialize pipeline

If STATE.md does not exist:

Create:
- PLAN.md (solution architecture + pipeline plan)
- STAGE.md for the first stage
- STATE.md

Set:
- STAGE_SEQUENCE from PLAN.md
- CURRENT_STAGE = first stage
- STAGE_ITERATION = 1
- STAGE_STATUS = in_progress
- ARCHITECT_STATUS = done
- NEXT_AGENT = coder
- ARCHITECT_DECISION = none

Constraints:
- PLAN.md and STAGE.md MUST be complete
- First stage MUST be executable by coder
- First stage MUST define implementation work

- If solution architecture defines multiple named parts:
  - pipeline plan MUST define multiple stages

- Each stage MUST correspond to one or more named parts
- All named parts MUST be covered by stages

Forbidden:
- planning-only or architecture-only stage
- descriptive-only output
- single stage covering the entire solution when multiple named parts exist

If violated:
- initialization is INVALID

---

#### Start same-stage rework

Start same-stage rework is a procedure used when ARCHITECT_DECISION = rework_same_stage.

Procedure result:

- PLAN.md MUST remain unchanged
- STAGE.md MUST remain unchanged
- STAGE_ITERATION MUST be increased by 1
- STAGE_STATUS MUST be set to in_progress
- CODER_STATUS MUST be set to pending
- REVIEWER_STATUS MUST be set to pending
- SEC_REVIEWER_STATUS MUST be set to pending
- ARCHITECT_STATUS MUST be set to done
- NEXT_AGENT MUST be set to coder

---

#### Start changed-stage rework

Start changed-stage rework is a procedure used when ARCHITECT_DECISION = rework_change_stage_contract.

Procedure result:

- at least one of the following MUST be updated:
  - STAGE.md
  - PLAN.md
- STAGE_ITERATION MUST be increased by 1
- STAGE_STATUS MUST be set to in_progress
- CODER_STATUS MUST be set to pending
- REVIEWER_STATUS MUST be set to pending
- SEC_REVIEWER_STATUS MUST be set to pending
- ARCHITECT_STATUS MUST be set to done
- NEXT_AGENT MUST be set to coder

---

#### Open next stage

Open next stage is a procedure used when a stage is completed and a next stage exists.

Procedure result:

- CURRENT_STAGE MUST be updated to the next stage
- CURRENT_STAGE_INDEX MUST be incremented by 1
- STAGE_ITERATION MUST be reset to 1
- STAGE.md MUST be updated for the new CURRENT_STAGE
- CODER_STATUS MUST be set to pending
- REVIEWER_STATUS MUST be set to pending
- SEC_REVIEWER_STATUS MUST be set to pending
- ARCHITECT_STATUS MUST be set to done
- NEXT_AGENT MUST be set to coder


---

#### Complete final stage

Complete final stage is a procedure used when the current stage is completed and no next stage exists.

Procedure result:

- ARCHITECT_STATUS MUST be set to done
- NEXT_AGENT MUST be set to user

---

#### Keep pipeline blocked

Keep pipeline blocked is a procedure used when pipeline cannot advance.

Procedure result:

- STAGE_STATUS MUST be blocked
- pipeline MUST NOT advance
- NEXT_AGENT MUST be set to user

---

#### Apply proposal changes

Apply proposal changes is a procedure used when TASK.md changes from ARCHITECTURE_PROPOSAL.md are applied.

Procedure result:

- STAGE_STATUS MUST be set to in_progress
- PLAN.md MAY be updated
- STAGE.md MAY be updated
- CODER_STATUS MUST be set to pending
- REVIEWER_STATUS MUST be set to pending
- SEC_REVIEWER_STATUS MUST be set to pending
- ARCHITECT_STATUS MUST be set to done
- NEXT_AGENT MUST be set to coder

---

#### Apply pipeline plan change

Apply pipeline plan change is a procedure used when ARCHITECT_DECISION = rework_change_pipeline_plan.

Procedure result:

- PLAN.md MUST be updated
- CURRENT_STAGE MAY be updated according to STAGE_SEQUENCE
- CURRENT_STAGE_INDEX MUST be consistent with CURRENT_STAGE
- STAGE_ITERATION MUST be set to 1
- STAGE_STATUS MUST be set to in_progress
- CODER_STATUS MUST be set to pending
- REVIEWER_STATUS MUST be set to pending
- SEC_REVIEWER_STATUS MUST be set to pending
- ARCHITECT_STATUS MUST be set to done
- NEXT_AGENT MUST be set to coder

---

#### Archive stage contract

Archive stage contract is a procedure used when the current stage is completed.

Procedure result:

- STAGE.md for CURRENT_STAGE MUST be archived
- archive operation MUST use rename
- archive path MUST follow:

  <archive>/<stage_index>_<stage_name>.md

---
