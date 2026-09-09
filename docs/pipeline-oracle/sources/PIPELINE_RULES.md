# PIPELINE RULES

## 1 Terminology

### Pipeline rules

Pipeline rules are defined in PIPELINE_RULES.md.

Pipeline rules are the authoritative source of pipeline behavior.

All agents MUST follow pipeline rules.

Any action not defined by pipeline rules is forbidden.

---

### State

State is the complete representation of the pipeline at a given moment.

State is stored in:
- STATE.md

State includes:

- current stage information
- stage sequence
- agent statuses
- control fields (e.g. NEXT_AGENT, STAGE_STATUS, ARCHITECT_DECISION)

---

### Action

An action is a required operation that an agent MUST perform during its step.

Actions are defined by:

- the agent role rules
- the current stage requirements
- the transition rules

An action may include:

- modifying files
- creating required artifacts
- updating STATE.md
- validating required files

A step is complete ONLY if:
- all required actions are performed
---

### Step

A step is a single agent action that produces output.

A step is atomic:
- it MUST NOT be split across multiple responses
- it MUST contain all required actions for the current agent

A step belongs to exactly one agent.

---

### Valid step

A step is valid ONLY if:

- the active agent performs it
- all required actions of the step are completed
- all required state changes are applied
- all input required files were present before the step
- all output required files are produced by the step
- STATE.md is updated

If any of these conditions is not satisfied:
- the step is INVALID

---

### Stage

A stage is a unit of solution implementation.

A stage:
- represents a distinct part of the solution architecture
- has an objective in PLAN.md
- is specified for execution in STAGE.md
- produces concrete implementation results

A stage MUST:
- implement only part of the solution
- be directly executable by coder
- result in creation or modification of implementation artifacts

A stage MUST NOT:
- represent planning or analysis only
- produce only descriptive artifacts

---

### Agent-visible STATE.md validity

Agent-visible STATE.md validity is the subset of STATE.md validation that can be performed by a non-architect agent.

Agent-visible STATE.md is valid if:

- all required visible fields exist
- all visible fields have allowed values
- NEXT_AGENT matches the active agent

---

### Full STATE.md validity (VISIBLE ONLY TO: architect)

Full STATE.md validity is the complete validation of STATE.md.

Full STATE.md is valid if:

- all required fields exist
- all fields have allowed values
- all field relationships defined in this document are satisfied

---

### Stage transition

Stage transition is the process of moving from a completed stage to the next stage in the pipeline.

---

### Stage status

Stage status is the status of the current stage.

The stage status is defined by:
- STAGE_STATUS field in STATE.md

---

### Current stage

The current stage is the stage currently being executed.
The current stage is defined by:
- CURRENT_STAGE field in STATE.md

---

### Current stage objective

Current stage objective is the goal that must be achieved in the current stage.

Current stage objective is defined by:
- PLAN.md

Current stage objective MUST:

- correspond to CURRENT_STAGE
- describe the intended outcome of the stage
- NOT define implementation details

---

### Stage contract

Stage contract is the set of requirements that define what must be done in the current stage.

Stage contract is defined by:
- STAGE.md


Stage contract MUST:

- implement the current stage objective
- implement a part of the solution architecture
- remain consistent with the pipeline plan
- define exact implementation requirements for coder

---

### Stage contract validity (CRITICAL)

A stage contract is INVALID if:

- it defines only analysis, design, planning, or documentation work
- it does not require creation or modification of implementation artifacts
- it cannot be directly executed by coder to produce concrete results

A stage MUST NOT:

- exist solely to design or describe solution architecture
- require producing only PLAN.md, STAGE.md, or descriptive artifacts
- defer implementation work to future stages if it can be defined now

If STAGE.md does not define concrete implementation work:

- the stage is INVALID

---

### CURRENT_STAGE

CURRENT_STAGE is the active stage from STAGE_SEQUENCE.

CURRENT_STAGE MUST match STAGE.md.

CURRENT_STAGE may change only:
- on stage open
- after stage completion
- on pipeline plan change

---

### Stage sequence

Stage sequence is the comma separated ordered list of stages in the pipeline.

The stage sequence is defined by:
- STAGE_SEQUENCE field in STATE.md

---

### STAGE_SEQUENCE validity (CRITICAL)

STAGE_SEQUENCE MUST contain only stage names representing units of solution work.

STAGE_SEQUENCE is INVALID if it contains:

- agent names
- control values
- transition markers
- duplicated execution-role entries used to represent agent order

---

### Current stage index

Current stage index is the position of the current stage in the stage sequence.

The current stage index is defined by:
- CURRENT_STAGE_INDEX field in STATE.md

----

### Total stages

Total stages is the total number of stages in the pipeline.

The total stages is defined by:
- TOTAL_STAGES field in STATE.md

---

### Stage iteration

Stage iteration is the number of attempts to complete the current stage.

The stage iteration is defined by:
- STAGE_ITERATION field in STATE.md

---

### Iteration limit semantics

STAGE_ITERATION counts the current execution attempt of the stage.

If:

- STAGE_ITERATION = MAX_STAGE_ITERATIONS

Then:

- iteration_limit_reached = true

A new rework MUST NOT be started if iteration_limit_reached = true.

---

### Stage rework

Stage rework is a repeated execution of the current stage.

---

### Stage artifact

A stage artifact is any file produced or modified during a stage.

Stage artifacts may include:
- implementation files

---
### Stage acceptance criteria

Stage acceptance criteria are the set of conditions that define when a stage is considered complete.

Stage acceptance criteria are defined by:
- STAGE.md

---

### Pipeline
A pipeline is a structured process for executing stages using agents.

The pipeline includes:

- stage sequence (STAGE_SEQUENCE)
- stage execution flow
- state management (STATE.md)
- agent coordination

The pipeline defines how stages are executed and controlled.

---
### Solution contract
Solution contract is the set of requirements that define:

- what system must be built
- required behavior and constraints
- external interfaces and expectations

Solution contract is defined by:
- TASK.md

---

### Solution architecture

Solution architecture is the decomposition of the solution into named parts.

It MUST:
- be derived from TASK.md
- describe the target system, not pipeline execution
- define parts that can be implemented through pipeline stages

It is defined in PLAN.md.

---

### Pipeline plan

Pipeline plan is the implementation plan of the solution architecture.

It MUST:
- be derived from solution architecture
- decompose solution architecture into stages
- define stage sequence
- define a stage objective for each stage
- remain consistent with solution architecture

PLAN.md MUST contain:
- solution architecture
- pipeline plan
---

### Execution

Execution is the process of performing steps in the pipeline.

Execution is carried out by agents according to the pipeline rules.

---

### Next step

The next step is the step that must be performed next in the pipeline.

The next step is determined by:
- transition rules
- current pipeline state

---

### Agent
An agent is one of:
- architect
- coder
- reviewer
- sec_reviewer

Only one agent may act at a time.

---

### Next agent

The next agent is the agent that must perform the next step.

The next agent is defined by:
- NEXT_AGENT field in STATE.md

An agent may act ONLY if:
- NEXT_AGENT matches the agent

NEXT_AGENT:

- MUST match one of the allowed values defined in STATE.md
- MUST follow the transition rules

---

### NEXT_AGENT

NEXT_AGENT identifies which actor MUST perform the next step.

NEXT_AGENT:
- MUST match one of the allowed values defined in STATE.md
- MUST be set according to transition rules

---

### Agent status

Agent status is the execution status of an agent for the current stage.

Agent status is defined by the following fields in STATE.md:
- CODER_STATUS
- REVIEWER_STATUS
- SEC_REVIEWER_STATUS
- ARCHITECT_STATUS

Agent status:
- MUST reflect whether the agent has completed its step
- MUST use only allowed values

Agent status may be changed ONLY by the corresponding agent or by architect during stage control.

---

### Recovery mode

Recovery mode is a special pipeline state.

Recovery mode is active when:
- RECOVERY_MODE field in STATE.md is set to true

When recovery mode is active:

- non-architect agents MUST NOT perform any step
- agent steps are restricted according to NEXT_AGENT and state validity rules

---

### Architect decision

Architect decision is the decision made by architect for the current stage.

Architect decision is defined by:
- ARCHITECT_DECISION field in STATE.md

---

### Required file

A required file is a file that MUST exist for a valid step.

There are two types of required files:

Input required files:
- files that MUST exist before the step starts
- MUST be validated as the FIRST action of the step

Output required files:
- files that MUST be produced by the step
- MUST be validated as the LAST action of the step

Required files are defined by:
- agent rules
- current stage requirements
- transition rules

If any input required file is missing:
- the agent MUST stop

If any output required file is missing:
- the step is INVALID

---

### Optional input file

An optional input file is a file that may be provided to an agent as input for a step.

Optional input files:

- may or may not be present
- MUST NOT be required for the step to start
- MUST be processed by the agent if present

Optional input files are defined by:

- agent role rules
- current stage context

----

### user:
- human interacting via chat

---

### orchestrator:
- external system controlling pipeline execution (if present)
---

### external:
- user or orchestrator

---

### external input:
- any change to TASK.md
- explicit instruction from user or orchestrator

---

### external actor:
- an entity that provides external input to the pipeline
- interacts with architect
---

### external communication:
- interaction between architect and external actors
- used to clarify requirements or provide decisions
---

## 2 Agent steps

Each agent step is a strictly defined execution unit.

A step consists of actions executed in a strict order:

1. Input validation action
2. Actions
3. Output validation action

---

### Step execution rules

Agent activation MUST follow the Next agent definition (Section 1).

Agent MUST derive the ability to act ONLY from:

- NEXT_AGENT
- STATE.md validity
- required inputs

Agent MUST NOT justify actions using general statements about rules.

A new agent step is valid only if:

- agent-visible STATE.md is valid
- all required input files exist

---

### Input validation action

Input validation action is the first action of the step.

Agent MUST:

- validate agent-visible STATE.md validity
- verify that all required input files exist

If input validation fails:

- agent MUST stop immediately
- agent MUST NOT perform further actions
- agent MUST NOT modify any files

If NEXT_AGENT does not match the active agent:

- agent MUST stop immediately
- agent MUST NOT perform further actions
- agent MUST NOT modify any files

---

### Actions

Actions are defined by:

- agent role rules
- current stage contract (STAGE.md)
- transition rules

Agent MUST NOT:

- perform actions outside of its role
- modify files not allowed by its role

---

### Output validation action

Output validation action is the last action of the step.

Agent MUST:

- produce all required output files
- include STATE.md in output

Output validation MUST satisfy the Valid step definition (Section 1).

---

### 2.1 Coder step

#### Input:

- STAGE.md
- STATE.md
- REVIEW.md (optional)
- SECURITY_REVIEW.md (optional)

---

#### Actions

Coder MUST:

- implement ONLY what is defined in STAGE.md

---

#### Output

- files defined by the stage contract (STAGE.md)
- STATE.md

---

#### Output validation action

Coder MUST:

- set:
  - CODER_STATUS=done
  - NEXT_AGENT=reviewer

Output validation MUST satisfy the Valid step definition (Section 1).

---

### 2.2 Reviewer step

Input:

- STAGE.md
- STATE.md
- stage artifacts (implementation files)

---

#### Actions

Reviewer MUST:

- review the implementation against STAGE.md
- check that all requirements from STAGE.md are satisfied

---

#### Output

- REVIEW.md
- STATE.md

---

#### Output validation action

Reviewer MUST:

- set:
  - REVIEWER_STATUS=done
  - NEXT_AGENT=sec_reviewer

Output validation MUST satisfy the Valid step definition (Section 1).

----
### 2.3 Sec reviewer step

Input:

- PLAN.md
- STAGE.md
- STATE.md
- stage artifacts (implementation files)

---

#### Actions

Sec reviewer MUST:

- review the implementation for security risks
- identify potential vulnerabilities or unsafe behavior

---

#### Output

- SECURITY_REVIEW.md
- STATE.md

---

#### Output validation action

Sec reviewer MUST:

- set:
  - SEC_REVIEWER_STATUS=done
  - NEXT_AGENT=architect

Output validation MUST satisfy the Valid step definition (Section 1).

-- 

## 3 STATE.md

STATE.md is the single source of pipeline state.

---

### Naming convention

AMY name MUST match:
^[a-z][a-z0-9_]*$

Solution architecture MUST define named parts.

Names MUST be pure and MUST NOT contain:
- stage prefixes (stage_, step_, phase_)
- numeric ordering (1_, 2_, etc.)
- pipeline or execution metadata

Pipeline stages MUST be defined as:

- STAGE_SEQUENCE contains exactly one stage per solution part
- number of stages = number of solution parts
- each stage name MUST equal the corresponding solution part name

Invalid naming or mismatch → step is INVALID

---

### Format

STATE.md MUST contain the following fields:

- STAGE_SEQUENCE
- CURRENT_STAGE
- CURRENT_STAGE_INDEX
- TOTAL_STAGES
- STAGE_STATUS
- STAGE_ITERATION
- MAX_STAGE_ITERATIONS
- ARCHITECT_STATUS
- CODER_STATUS
- REVIEWER_STATUS
- SEC_REVIEWER_STATUS
- ARCHITECT_DECISION
- NEXT_AGENT
- RECOVERY_MODE

Field order MUST match this section.
---

### Ownership semantics

Field ownership defines which agent is allowed to modify a STATE.md field.

Rules:

- Only the owning agent MAY modify a field
- Other agents MUST NOT modify fields they do not own

Exception:

- it MUST be set exactly as defined by transition rules
- ownership does NOT grant freedom to set arbitrary NEXT_AGENT values

Violation of ownership rules makes the step INVALID.

---
### Field format

STAGE_SEQUENCE:
- ownership: architect
- value: comma-separated list of stages

CURRENT_STAGE:
- ownership: architect
- value: one of STAGE_SEQUENCE

CURRENT_STAGE_INDEX:
- ownership: architect
- value: integer
- MUST match position of CURRENT_STAGE in STAGE_SEQUENCE

TOTAL_STAGES:
- ownership: architect
- value: integer
- MUST equal number of stages in STAGE_SEQUENCE

STAGE_STATUS:
- ownership: architect
- allowed values: defined in architect-only rules

STAGE_ITERATION:
- ownership: architect
- value: integer
- MUST start from 1


MAX_STAGE_ITERATIONS:

- defines the maximum number of attempts to complete a stage
- value: integer
- MUST be > 1

ARCHITECT_STATUS:
- ownership: architect
- allowed values:
  - pending
  - done
  - failed

CODER_STATUS:
- ownership: coder
- allowed values:
  - pending
  - done
  - failed

REVIEWER_STATUS:
- ownership: reviewer
- allowed values:
  - pending
  - done
  - failed

SEC_REVIEWER_STATUS:
- ownership: sec_reviewer
- allowed values:
  - pending
  - done
  - failed

ARCHITECT_DECISION:
- ownership: architect
- allowed values: defined in architect-only rules

NEXT_AGENT:
- ownership: transition-defined
- value:
  - one of pipeline agents:
    - architect
    - coder
    - reviewer
    - sec_reviewer
  - user (external actor)

RECOVERY_MODE:
- ownership: architect
- allowed values:
  - true
  - false

---

## 4 STATE update rules

STATE.md is the only source of truth for pipeline state.

STATE.md is invalid if:

- any required field is missing
- any field has a value outside allowed values
- field relationships defined in this document are violated

---

### Update rules

- State changes MUST be applied only via STATE.md
- Describing a state change in text does NOT modify state
- Only the final version of STATE.md in output is considered

---

### Persistence rules

- If STATE.md is not present in output:
  - state is NOT updated

- If STATE.md is present:
  - all fields in STATE.md MUST be treated as the new state

---

## 5 Review outputs

Review outputs are review-owned files produced by reviewer and sec_reviewer steps.

---

### 5.1 REVIEW.md

REVIEW.md is the reviewer output file.

REVIEW.md MUST:

- describe findings for the current stage only
- be based on STAGE.md and stage artifacts
- reflect the actual implementation state

REVIEW.md MUST contain:

- Scope
- Checked items
- Findings
- Violations
- Conclusion

Scope MUST:

- confirm that only the current stage is reviewed

Checked items MUST:

- list requirements from STAGE.md
- give status for each requirement

Allowed item statuses:

- implemented
- not implemented
- unclear

Findings MUST:

- include all detected issues
- reference the relevant STAGE.md requirement
- describe the observed mismatch
- describe the impact

Violations MUST:

- list every mismatch with STAGE.md

Conclusion MUST be one of:

- pass
- fail

Reviewer MUST NOT:

- review outside the current stage
- skip findings
- classify issues as minor or major

---

### 5.2 SECURITY_REVIEW.md

SECURITY_REVIEW.md is the sec_reviewer output file.

SECURITY_REVIEW.md MUST:

- describe security findings for the current implementation
- use PLAN.md as security context
- reflect the actual implementation state

SECURITY_REVIEW.md MUST contain:

- Scope
- Findings
- Conclusion

Scope MUST:

- confirm that security review is performed for the current implementation
- state that focus is on risks introduced or affected by the current stage

Findings MUST:

- include all detected issues
- identify affected component or file
- describe the risk

Conclusion MUST be one of:

- safe
- unsafe to proceed

Sec_reviewer MUST NOT:

- skip potential risks
- classify issues as minor or major

---

