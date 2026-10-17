/**
 * The minimal agent bundle of the 66-command lifecycle E2E; shared with
 * the browser UI events integration suite. Not a test file — test suites
 * never import other test files (transitive re-evaluation would duplicate
 * test registration under the file-level isolated parallel runner).
 */
/** The minimal agent bundle of the 66-command lifecycle E2E; shared
 * with the browser UI events integration suite. */
export const E2E_YAML = `
schema_version: 2
entry_state: architect
max_transitions: 8
inputs: []
outputs: []
states:
  - id: architect
    type: agent
    profile: architect
    prompt: prompts/architect.md
    inputs: []
    outputs:
      - id: plan
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: dev_entry
  - id: dev_entry
    type: agent
    profile: coder
    prompt: prompts/coder.md
    inputs: []
    outputs:
      - id: report
        type: json
        schema: schemas/plan.schema.json
    timeout_seconds: 60
    max_attempts: 1
    transitions:
      - outcome: completed
        to: done
  - id: done
    type: terminal
    result: success
`;
