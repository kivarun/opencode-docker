/**
 * Fixture executable for the control-plane run binding race tests.
 *
 * A real separate OS process that opens the binding store over the given
 * root and performs exactly one store operation, answering with exactly
 * one JSON document plus a newline on stdout. It imports only the public
 * store facade — the same surface a future control-plane server would
 * use — and never touches anything else.
 *
 * usage: bun control_plane_run_binding_claim_child.ts <storeRoot> <runId> <projectId> [load]
 *   (the fourth argument switches the child from claim to load)
 */
import { ControlPlaneRunBindingStore, ControlPlaneRunBindingError } from "../src/control_plane/run_binding_store.ts";

const argv = process.argv.slice(2);
const storeRoot = argv[0];
const runId = argv[1];
const projectId = argv[2] ?? "";
const mode = argv[3];

if (storeRoot === undefined || runId === undefined) {
  process.stdout.write(`${JSON.stringify({ ok: false, reason: "binding_invalid", message: "usage: <storeRoot> <runId> <projectId> [load]" })}\n`);
} else {
  const store = await ControlPlaneRunBindingStore.open(storeRoot);
  try {
    if (mode === "load") {
      const binding = await store.load(runId);
      process.stdout.write(`${JSON.stringify({ ok: true, kind: "loaded", binding })}\n`);
    } else {
      const claim = await store.claim({ runId, projectId });
      process.stdout.write(`${JSON.stringify({ ok: true, kind: claim.kind, binding: claim.binding })}\n`);
    }
  } catch (cause) {
    const reason = cause instanceof ControlPlaneRunBindingError ? cause.reason : "unexpected";
    const message = cause instanceof Error ? cause.message : "unknown failure";
    process.stdout.write(`${JSON.stringify({ ok: false, reason, message })}\n`);
  }
}
