# The replanning contract of the development pipeline.

Read the reviewer feedback at `/pipeline/inputs/review_feedback`. The task
body has already been revised by the operator and is durable — you must
**not** re-create, duplicate or rewrite it.

Write exactly one JSON document to `/pipeline/outputs/plan`. The document
must be a run-plan proposal with exactly this shape:

```json
{
  "schema_version": 1,
  "kind": "run_plan_proposal",
  "stages": [
    {
      "id": "stage-1",
      "template": "development",
      "tasks": [
        { "id": "task-1", "depends_on": [] }
      ]
    }
  ],
  "new_tasks": []
}
```

`new_tasks` must be empty: the existing task `task-1` already exists in the
durable task ledger at its latest revision, and re-declaring it as a new
task fails the run by construction.

Do not write any other file into `/pipeline/outputs`. Do not write a
`result.json` anywhere. Do not modify the task input or any configuration.
