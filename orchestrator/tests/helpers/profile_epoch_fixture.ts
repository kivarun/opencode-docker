/**
 * Shared profile-epoch fixtures for the test suites. Not a test file: the
 * suites import these helpers so no test file imports another test file.
 */
import {
  preparePipelineV2ProfileEpoch,
  type PipelineV2ProfileEpochBinding,
  type PreparedPipelineV2ProfileEpoch,
  type ProfileEpochProfileEntry,
} from "../../src/pipeline_v2_profile_epoch.ts";

const EXAMPLE_ENTRY: ProfileEpochProfileEntry = {
  id: "default",
  image_ref: "ghcr.io/example/worker@sha256:" + "a".repeat(64),
  config_policy: "env_only",
  config_sha256: "b".repeat(64),
  env_bindings: [
    {
      destination: "LLM_SERVER",
      from_env: "LLM_SERVER",
      required: true,
      secret: false,
      value_sha256: "c".repeat(64),
    },
  ],
};

/** One prepared single-profile epoch manifest shared by coordinator-level tests. */
export const V2_PREPARED_PROFILE_EPOCH: PreparedPipelineV2ProfileEpoch =
  preparePipelineV2ProfileEpoch([EXAMPLE_ENTRY]);

/** The binding matching `V2_PREPARED_PROFILE_EPOCH`. */
export const V2_PREPARED_PROFILE_EPOCH_BINDING: PipelineV2ProfileEpochBinding = {
  digest: V2_PREPARED_PROFILE_EPOCH.sha256,
  profile_count: V2_PREPARED_PROFILE_EPOCH.manifest.profiles.length,
};

import { loadProfile } from "../../src/profile.ts";
import { uniqueProfileNames } from "../../src/pipeline_v2_runner.ts";
import type { ResolvedPipelineV2 } from "../../src/pipeline_v2.ts";
import { publishPipelineV2ProfileEpoch } from "../../src/pipeline_v2_profile_epoch_store.ts";
import type { ResolvedProfile } from "../../src/profile.ts";

/**
 * Loads the pipeline's unique agent-state profiles exactly like the fresh
 * runner does, prepares the epoch manifest from those loaded objects,
 * publishes it at `<runRoot>/profile-epoch.json` and returns the binding
 * for the fixture's `create_run` dispatch. A later runner invocation on
 * the same run recomputes the same manifest from the same configuration
 * root and base environment.
 */
export async function prepareAndPublishRunProfileEpoch(options: {
  readonly runRoot: string;
  readonly pipeline: ResolvedPipelineV2;
  readonly configRoot: string;
  readonly baseEnv: Readonly<Record<string, string | undefined>>;
}): Promise<PreparedPipelineV2ProfileEpoch> {
  const profiles = new Map<string, ResolvedProfile>();
  for (const name of uniqueProfileNames(options.pipeline)) {
    profiles.set(name, await loadProfile(options.configRoot, name, options.baseEnv));
  }
  const prepared = preparePipelineV2ProfileEpoch(
    uniqueProfileNames(options.pipeline).map((name) => {
      const profile = profiles.get(name);
      if (profile === undefined) {
        throw new Error(`profile epoch fixture: profile ${JSON.stringify(name)} was not loaded`);
      }
      return profile.identity;
    }),
  );
  await publishPipelineV2ProfileEpoch(options.runRoot, prepared);
  return prepared;
}

/**
 * Loads and publishes the run epoch and returns only the durable binding
 * for the fixture's `create_run` dispatch.
 */
export async function loadAndPublishRunProfileEpoch(options: {
  readonly runRoot: string;
  readonly pipeline: ResolvedPipelineV2;
  readonly configRoot: string;
  readonly baseEnv: Readonly<Record<string, string | undefined>>;
}): Promise<PipelineV2ProfileEpochBinding> {
  const prepared = await prepareAndPublishRunProfileEpoch(options);
  return { digest: prepared.sha256, profile_count: prepared.manifest.profiles.length };
}
