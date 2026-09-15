#!/usr/bin/env python3
"""Fail-closed patch: make litert-torch export_hf honor
ExportConfig.output_logits_on_prefill (default False) on prefill signatures.

Source of truth: the UAT-proven overlay fix (2026-09-15), verified by the
first generic Gemma3-270M conversion + BoxProbe re-UAT (contract PASS).
This script ports EXACTLY that minimal fix — no new variant, no by-memory
reimplementation.

Behaviour (fail closed):
  1. locates litert_torch/generative/export_hf/core/exportable_module.py;
  2. checks the EXPECTED original fragment is present exactly once (the
     upstream hardcoded prefill return); fails if the upstream source does
     not match this expectation (upstream changed -> re-derive the fix,
     never guess);
  3. if the patch marker is already present, verifies the patched fragment
     matches the expected patched state exactly (idempotent re-run OK);
  4. applies the replacement (exactly one fragment, exactly once);
  5. re-reads the file and verifies the patched state is in place and the
     original fragment is gone.

Exit codes: 0 = patched (or already patched & verified), 1 = FAIL.
"""
import sys
from pathlib import Path

FILE_REL = Path("litert_torch") / "generative" / "export_hf" / "core" / "exportable_module.py"

# Verbatim original upstream fragment (checked against litert-torch-nightly
# 0.10.0.dev20260909 exactly once; must match, otherwise FAIL).
ORIG = '''    inputs |= self.attention_kwargs()
    inputs["logits_to_keep"] = 1
    output = self.model(**inputs)
    return {
        "kv_cache": output.past_key_values,
        "logits": output.logits,
    }'''

# Verbatim patched fragment, identical to the proven overlay:
# /exchange/scratch/conv2/overlay/litert_torch/generative/export_hf/core/exportable_module.py
NEW = '''    inputs |= self.attention_kwargs()
    inputs["logits_to_keep"] = 1
    output = self.model(**inputs)
    # WORKAROUND (upstream exporter defect): litert-torch export_hf ignores its
    # own ExportConfig.output_logits_on_prefill flag and hardcodes "logits" in
    # prefill outputs, which LiteRT-LM <=0.12 runtime rejects at engine creation
    # ("Only kv_cache tensors are supported as outputs to prefill"). This patch
    # restores the flag semantics: prefill emits kv_cache only unless the flag
    # is explicitly set. REMOVE THIS PATCH once upstream export_hf honors the
    # flag again.
    if not getattr(self.export_config, "output_logits_on_prefill", False):
      return {"kv_cache": output.past_key_values}
    return {
        "kv_cache": output.past_key_values,
        "logits": output.logits,
    }'''

MARKER = "WORKAROUND (upstream exporter defect)"


def fail(msgs):
    print("PATCH FAIL:", file=sys.stderr)
    for m in msgs:
        print(f"  - {m}", file=sys.stderr)
    sys.exit(1)


def locate():
    try:
        import litert_torch
    except ImportError as e:
        fail([f"litert_torch not importable: {e}",
              "run with the model-converter venv python "
              "(/opt/model-converter/bin/python)"])
    root = Path(litert_torch.__file__).resolve().parent.parent
    return root / FILE_REL


def main():
    path = locate()
    if not path.is_file():
        fail([f"target file not found: {path}"])
    text = path.read_text()

    n_orig = text.count(ORIG)
    n_new = text.count(NEW)
    patched_marker = MARKER in text

    if patched_marker and n_orig == 0:
        # Already patched: verify the patched state is exactly expected.
        if n_new != 1:
            fail([f"patch marker present but patched fragment count = {n_new} "
                  "(expected exactly 1); manual inspection required"])
        print(f"PATCH OK (already applied, verified): {path}")
        return

    if patched_marker and n_orig > 0:
        fail(["marker present AND original fragment present — inconsistent "
              "state, manual inspection required"])

    if n_orig != 1:
        fail([f"expected original fragment count = {n_orig} (expected exactly 1)",
              "upstream source does not match the known litert-torch-nightly "
              "0.10.0.dev20260909 shape; re-derive the fix from the actual "
              "source, do not guess"])
    if n_new != 0:
        fail([f"patched fragment already present without marker (count {n_new})"])

    text = text.replace(ORIG, NEW, 1)
    path.write_text(text)

    # Post-apply verification (re-read from disk).
    reread = path.read_text()
    if reread.count(NEW) != 1 or reread.count(ORIG) != 0:
        fail(["post-apply verification failed: patched fragment missing or "
              "original fragment still present"])
    print(f"PATCH OK: {path}")
    print("  export_hf prefill now honors ExportConfig.output_logits_on_prefill")
    print("  (default False -> prefill outputs kv_cache only)")


if __name__ == "__main__":
    main()
