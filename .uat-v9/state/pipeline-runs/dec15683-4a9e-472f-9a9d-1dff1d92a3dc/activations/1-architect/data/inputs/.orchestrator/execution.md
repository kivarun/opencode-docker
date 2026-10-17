# Pipeline execution document

state_id: "architect"
execution_index: 1

## Instruction

# Plan the run

You are the planning agent for this pipeline run, working in the shared
project workspace mounted at `/workspace`.

- Read the protected task file mounted at `/pipeline/inputs/task`. Treat
  it as read-only: never modify, replace, or delete it.
- Use its content as the body of the single task of the run plan.

Your single declared output port is the JSON document at
`/pipeline/outputs/plan`. You MUST create that file before you finish, and
it is the only artifact the orchestrator accepts from this activation.

The file must contain exactly this JSON document shape (schema_version 1,
kind `run_plan_proposal`):

```json
{
  "schema_version": 1,
  "kind": "run_plan_proposal",
  "stages": [
    {
      "id": "stage-1",
      "template": "development",
      "tasks": [ { "id": "task-1", "depends_on": [] } ]
    }
  ],
  "new_tasks": [
    { "id": "task-1", "body": "<the exact content of /pipeline/inputs/task>" }
  ]
}
```

Rules:

- The stage id must be exactly `stage-1` and the template exactly
  `development`: the documented next operator command for this pipeline is
  `orchestrator resume-plan --stage-id stage-1 --initial-budget 1`.
- Declare exactly one new task with the id `task-1`; its body is the
  non-empty content of the protected task file.
- The stage task pointer references that task with an empty `depends_on`.
- Do not write any other file into `/pipeline/outputs`.
- Do not write `result.json` or any other structured result file.
- Do not attempt to change pipeline, profile, or runtime configuration.


## Workspace

The shared project directory is mounted at `/workspace` read-write.
The project sources at `/workspace` are the working product of this activation.
They are not part of the pipeline output-port namespace; do not place
declared outputs anywhere but their declared paths below.

## Input ports (declaration order)

1. `task` — type `file`, path `/pipeline/inputs/task` (read-only)

## Output ports (declaration order)

1. `plan` — type `json`, path `/pipeline/outputs/plan`
   JSON schema (canonical JSON):
   ```json
   {"$schema":"https://json-schema.org/draft/2020-12/schema","title":"Run plan proposal envelope","type":"object"}
   ```

## Rules

- Create only the declared top-level outputs under `/pipeline/outputs`.
- Finish only after all declared outputs have been written.
- The project sources at `/workspace` are the working product of this
  activation; they are not part of the pipeline output-port namespace.
