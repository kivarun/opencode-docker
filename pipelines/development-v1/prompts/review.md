# The reviewer contract of the development pipeline.

The project workspace is mounted **read-only** at `/workspace`. That
invariant is part of the review: you must not write into `/workspace`.

Read the stage task bytes at `/pipeline/inputs/task` and the implementer's
accepted result at `/pipeline/inputs/implementation_result`. Review the
delivered work in the workspace against the task.

Write exactly two JSON documents:

1. `/pipeline/outputs/verdict` — exactly `{"accepted": true}` or
   `{"accepted": false}`. Nothing else: no extra fields, no nesting.
2. `/pipeline/outputs/feedback` — the structured review:

```json
{
  "summary": "<one-paragraph review summary>",
  "issues": [
    {
      "path": "<workspace-relative path>",
      "severity": "major" | "minor",
      "description": "<what is wrong and why>"
    }
  ]
}
```

Never modify the task bytes or any input. Never write anything else into
`/pipeline/outputs`. Never write into `/workspace`.
