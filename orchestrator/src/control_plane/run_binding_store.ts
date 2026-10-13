/**
 * Durable control-plane run binding store (public facade).
 *
 * Before the control-plane starts the first `run` for a caller run id it
 * durably binds that run id to one registry project id
 * (`ControlPlaneRunBindingStore.open(root)` + `claim`). After any restart
 * every later operation resolves its project, config and pipeline
 * authority exclusively through `resolveBoundControlPlaneProject` — the
 * binding is loaded from the store and the project is looked up only in
 * the already-loaded registry; a caller-supplied project id is never
 * accepted as ownership evidence and is not even a parameter.
 *
 * The binding document is a versioned immutable canonical JSON record
 * (`{"project_id":"...","run_id":"...","schema_version":1}`) published as
 * a 0600 regular file at `<store-root>/bindings/<runId>.json` inside a
 * store-owned 0700 `bindings/` namespace, through the one shared
 * immutable publication protocol (exclusive temp file, fsync, exclusive
 * `link()`, parent fsync, read-back verification). A published binding is
 * never updated, rewritten or removed; there is no list, delete or update
 * API. The store never touches the Pipeline V2 run root or the durable
 * run state tree, and a CLI-created run without a binding fails closed
 * (`binding_not_found`) — it is never adopted and never guessed from raw
 * state; operator import/adoption is a separate future increment.
 *
 * The typed failure vocabulary, the atomic publication, the crash and
 * concurrency semantics and the content-free diagnostics are documented
 * on the internal core (`run_binding_store_internal.ts`), which this
 * facade always drives with the single fixed production IO.
 */

import { deepFreezeValue } from "../pipeline_v2_freeze_internal.ts";
import {
  ControlPlaneProjectRegistryError,
  type ControlPlaneProjectRegistry,
  type ResolvedControlPlaneProject,
} from "./project_registry.ts";
import {
  ControlPlaneRunBindingError,
  claimRunBindingWithIo,
  loadRunBindingWithIo,
  openRunBindingStoreWithIo,
  realRunBindingStoreIo,
  type ControlPlaneRunBinding,
  type ControlPlaneRunBindingClaim,
  type ControlPlaneRunBindingFailureReason,
  type RunBindingStoreState,
} from "./run_binding_store_internal.ts";

export { ControlPlaneRunBindingError };
export type { ControlPlaneRunBinding, ControlPlaneRunBindingClaim, ControlPlaneRunBindingFailureReason };

/**
 * The durable run binding store. Opened once per use against one
 * operator-configured store root; every instance is frozen and every
 * operation drives the internal core with the single fixed production IO.
 */
export class ControlPlaneRunBindingStore {
  private readonly state: RunBindingStoreState;

  private constructor(state: RunBindingStoreState) {
    this.state = state;
    Object.freeze(this);
  }

  /** Opens the store over one existing real non-symlink directory root. */
  static async open(root: string): Promise<ControlPlaneRunBindingStore> {
    return new ControlPlaneRunBindingStore(await openRunBindingStoreWithIo(realRunBindingStoreIo, root));
  }

  /**
   * Claims the run id for the project id: `created` for the fresh
   * exclusive publication, `existing` for an exactly matching published
   * binding, the typed `binding_conflict` failure for a different
   * project. Invalid identifiers never reserve a run id.
   */
  claim(options: { readonly runId: string; readonly projectId: string }): Promise<ControlPlaneRunBindingClaim> {
    return claimRunBindingWithIo(realRunBindingStoreIo, this.state, options);
  }

  /**
   * Loads one run binding strictly read-only: `binding_not_found` when no
   * binding exists, a typed failure for a defective artifact, never a
   * repair.
   */
  load(runId: string): Promise<ControlPlaneRunBinding> {
    return loadRunBindingWithIo(realRunBindingStoreIo, this.state, runId);
  }
}

/** The authority resolution result: the durable binding plus its registry project. */
export interface ControlPlaneBoundProject {
  readonly binding: ControlPlaneRunBinding;
  readonly project: ResolvedControlPlaneProject;
}

/**
 * The one composition boundary from a durable run binding to its project
 * authority: the binding is loaded from the store by run id, and the
 * project is looked up exclusively in the already-loaded frozen registry.
 * The caller supplies no project id and can never override the resolved
 * authority. A run whose binding references a project that the current
 * registry no longer declares fails closed as `project_unavailable` with
 * zero writes — the binding itself is never deleted and no subprocess is
 * started by this layer.
 */
export async function resolveBoundControlPlaneProject(
  registry: ControlPlaneProjectRegistry,
  store: ControlPlaneRunBindingStore,
  runId: string,
): Promise<ControlPlaneBoundProject> {
  if (
    typeof registry !== "object" ||
    registry === null ||
    Array.isArray(registry) ||
    !Object.isFrozen(registry) ||
    !Array.isArray((registry as unknown as Record<string, unknown>)["projects"])
  ) {
    throw new ControlPlaneProjectRegistryError(
      "invalid_registry",
      "the registry must be a loaded, frozen registry object",
    );
  }
  if (!(store instanceof ControlPlaneRunBindingStore)) {
    throw new ControlPlaneRunBindingError("binding_invalid", "the run binding store must be an opened store");
  }
  const binding = await store.load(runId);
  let project: ResolvedControlPlaneProject | null = null;
  for (const candidate of registry.projects) {
    if (candidate.id === binding.projectId) {
      project = candidate;
      break;
    }
  }
  if (project === null) {
    throw new ControlPlaneRunBindingError(
      "project_unavailable",
      "the run binding references a project that is not declared in the loaded registry",
    );
  }
  return deepFreezeValue({ binding, project });
}
