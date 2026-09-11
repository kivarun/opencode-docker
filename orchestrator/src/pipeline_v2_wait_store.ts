import {
  realWaitStoreIo,
  publishWaitRequestWithIo,
  publishWaitResponseWithIo,
  PipelineV2WaitStoreError,
  type PipelineV2WaitStoreFailureReason,
  type PipelineV2WaitStoreCandidate,
  type PublishedPipelineV2WaitRequest,
  type PublishedPipelineV2WaitResponse,
} from "./pipeline_v2_wait_store_internal.ts";

/**
 * Filesystem publication of the wait request/response manifests (unwired).
 *
 * This module publishes the orchestrator-owned canonical manifests of the
 * pure `pipeline_v2_wait_manifest.ts` substrate under the fixed flat
 * layout
 *
 *   <runRoot>/waits/<waitIndex>.request.json
 *   <runRoot>/waits/<waitIndex>.response.json
 *
 * as 0600 regular files whose content is exactly the manifest's canonical
 * JSON (no trailing newline), inside a 0700 real non-symlink `waits/`
 * directory of the canonical run root. Publication is atomic (temp file
 * with `O_CREAT|O_EXCL|O_NOFOLLOW`, full write-all, file fsync, close,
 * exclusive `link()` — never a replace-capable `rename()` — then
 * ownership-checked temp removal and a waits-directory fsync) and
 * idempotent: a repeat with the same canonical bytes adopts the existing
 * file without touching its inode, mode, mtime or content and re-fsyncs
 * the directory, so a retry after a former durability-unknown outcome
 * confirms it. A different manifest on a busy path is a typed conflict;
 * nothing is ever overwritten, repaired or removed except the caller's own
 * temp file.
 *
 * Errors are a closed typed contract (`PipelineV2WaitStoreError` with
 * `outcome` `not_published`|`durability_unknown`, `reason`
 * `invalid_layout`|`conflict`|`io_failure`, and an immutable candidate on
 * durability-unknown outcomes); manifest validation failures keep their
 * `PipelineV2WaitManifestError` class. Diagnostics are content-free.
 *
 * Semantic boundary with the durable state (future, not wired here):
 * `publish request → dispatch run_waiting(request_sha256)` and
 * `publish response → dispatch wait_response_recorded(response_sha256)`.
 * A manifest file without the corresponding durable commit is an orphan,
 * not part of the history; this module never dispatches reducer commands
 * and never touches the durable state, the project copy, inputs or
 * outputs. The coordinator, runner, CLI and resume wiring are later
 * increments.
 *
 * The full algorithm, concurrency semantics, ownership rules and honest
 * boundaries are documented in `pipeline_v2_wait_store_internal.ts`.
 */
export {
  PipelineV2WaitStoreError,
  type PipelineV2WaitStoreFailureReason,
  type PipelineV2WaitStoreCandidate,
  type PublishedPipelineV2WaitRequest,
  type PublishedPipelineV2WaitResponse,
};

/**
 * Validates, binds and publishes one wait request manifest under
 * `<runRoot>/waits/<wait_index>.request.json`. The run root must be an
 * existing absolute canonical real non-symlink directory whose basename is
 * the manifest's run id; it is never created or removed.
 */
export async function publishPipelineV2WaitRequest(
  runRoot: string,
  value: unknown,
): Promise<PublishedPipelineV2WaitRequest> {
  return await publishWaitRequestWithIo(realWaitStoreIo, runRoot, value);
}

/**
 * Validates the wait index, loads and verifies the stored request file
 * `<runRoot>/waits/<waitIndex>.request.json` itself (the caller never
 * passes a prepared request), accepts the user's response against it, and
 * publishes `<runRoot>/waits/<waitIndex>.response.json`.
 */
export async function publishPipelineV2WaitResponse(
  runRoot: string,
  waitIndex: number,
  raw: string,
): Promise<PublishedPipelineV2WaitResponse> {
  return await publishWaitResponseWithIo(realWaitStoreIo, runRoot, waitIndex, raw);
}
