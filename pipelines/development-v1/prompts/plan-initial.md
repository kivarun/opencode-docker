# The initial planning contract of the development pipeline.

You are the initial planner of this pipeline. Read the protected task at
`/pipeline/inputs/task`. That file is the run's task description.

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
  "new_tasks": [
    { "id": "task-1", "body": "<the exact content of /pipeline/inputs/task>" }
  ]
}
```

The `body` of `task-1` must be the exact content of `/pipeline/inputs/task`,
byte for byte.

Do not write any other file into `/pipeline/outputs`. Do not write a
`result.json` anywhere. Do not modify the task input or any configuration.
