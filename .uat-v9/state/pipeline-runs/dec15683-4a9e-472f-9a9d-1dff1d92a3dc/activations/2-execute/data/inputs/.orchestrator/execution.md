# Pipeline execution document

state_id: "execute"
execution_index: 2

## Instruction

# Execute the stage

You are the stage worker for this pipeline run, working in the shared
project workspace mounted at `/workspace`.

- Read the task file mounted at `/pipeline/inputs/task` and treat it as
  the contract for this run. The task file is protected: never modify,
  replace, or delete it.
- Perform the work the task asks for and produce the requested work
  product in the workspace. Keep the work product self-contained and
  reproducible; prefer writing files over printing results only to the
  console.
- Your activation has exactly one declared output port: the JSON document
  at `/pipeline/outputs/result`. You MUST create that file before you
  finish, and it must conform to the JSON schema embedded in the
  execution document (`/pipeline/inputs/.orchestrator/execution.md`).

The result document carries exactly these fields:

```json
{
  "schema_version": 3,
  "status": "completed",
  "summary": "<one-line description of the finished work>",
  "artifacts": ["<workspace-relative path>"]
}
```

Rules:

- Use `status: "completed"` only when the work described by the task is
  actually finished; never create or fake a successful result for
  unfinished work.
- Record every file you created or modified as an artifact, using clean
  workspace-relative paths.
- Do not fabricate results, artifacts, or paths that do not exist.
- Do not write any other file into `/pipeline/outputs`.
- Do not write `result.json` anywhere: this pipeline does not use a
  legacy structured result file.
- Do not write a planning proposal.
- Never change pipeline, profile, or runtime configuration.


## Workspace

The shared project directory is mounted at `/workspace` read-write.
The project sources at `/workspace` are the working product of this activation.
They are not part of the pipeline output-port namespace; do not place
declared outputs anywhere but their declared paths below.

## Input ports (declaration order)

1. `task` — type `file`, path `/pipeline/inputs/task` (read-only)

## Output ports (declaration order)

1. `result` — type `json`, path `/pipeline/outputs/result`
   JSON schema (canonical JSON):
   ```json
   {"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"artifacts":{"items":{"type":"string"},"type":"array"},"schema_version":{"const":3},"status":{"const":"completed"},"summary":{"minLength":1,"pattern":"\\S","type":"string"}},"required":["schema_version","status","summary","artifacts"],"title":"Pipeline V2 agent result","type":"object"}
   ```

## Rules

- Create only the declared top-level outputs under `/pipeline/outputs`.
- Finish only after all declared outputs have been written.
- The project sources at `/workspace` are the working product of this
  activation; they are not part of the pipeline output-port namespace.
