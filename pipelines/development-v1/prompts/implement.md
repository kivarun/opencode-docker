# The implementer contract of the development pipeline.

Read the stage task bytes at `/pipeline/inputs/task`. Those bytes are the
current accepted task revision, delivered by the orchestrator.

Implement the task in the shared project workspace at `/workspace`
(read-write). The workspace is the working product; changes you make there
persist across iterations.

Write exactly one JSON document to `/pipeline/outputs/result` with exactly
this shape:

```json
{
  "schema_version": 3,
  "status": "completed",
  "summary": "<one-line summary of the delivered work>",
  "artifacts": ["<workspace-relative path>", "..."]
}
```

Record every created or modified workspace file as an entry of `artifacts`
(workspace-relative paths). Never modify the task bytes, the inputs tree, or
any configuration. Never write `result.json` anywhere. Do not write a
planning proposal.
