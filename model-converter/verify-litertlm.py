#!/opt/model-converter/bin/python3
"""Post-conversion contract verifier for generic LiteRT-LM artifacts.

The TFLite signature contract is the source of truth — the container format
version is NOT used to decide compatibility (the runtime already proved it
reads container 1.6.0; the defect was in the graph contract).

Fails closed (exit 1) unless ALL of the following hold:
  1. `litert-lm-peek` parses the artifact successfully;
  2. every prefill_* signature has ONLY kv-cache outputs
     (kv_cache_{k,v}_N / kv_slice_{k,v}_N — no logits or other non-KV outputs);
  3. the decode signature exists, emits `logits`, and carries kv-cache outputs.

Signatures are read factually via ai_edge_litert Interpreter — the same
mechanism used to compare the working artifacts (generic 1B, container 1.0.0;
1B MT6991 NPU, container 1.1.0) against the broken 270M (container 1.6.0)
during the 2026-09-15 investigation.

Usage: verify-litertlm MODEL.litertlm
Exit codes: 0 = contract PASS, 1 = FAIL (reasons printed), 2 = usage error.
"""
import glob
import os
import re
import shutil
import subprocess
import sys
import tempfile

KV_RE = re.compile(r"^kv_(cache|slice)_[kv]_\d+$")


def fail(reasons):
    print("CONTRACT FAIL:")
    for r in reasons:
        print(f"  - {r}")
    sys.exit(1)


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    artifact = sys.argv[1]
    reasons = []

    if not os.path.isfile(artifact):
        fail([f"artifact not found: {artifact}"])

    # 1. litert-lm-peek must parse the container (fail closed).
    peek = shutil.which("litert-lm-peek")
    if not peek:
        fail(["litert-lm-peek not found on PATH"])
    with tempfile.TemporaryDirectory(prefix="litertlm-verify-") as tmp:
        proc = subprocess.run(
            [peek, "--litertlm_file", artifact, "--dump_files_dir", tmp],
            capture_output=True, text=True)
        if proc.returncode != 0:
            fail([f"litert-lm-peek exit {proc.returncode}",
                  (proc.stderr or proc.stdout).strip()[:800]])
        if "Sections" not in proc.stdout:
            fail(["litert-lm-peek output has no Sections (container unreadable)"])

        # Locate the prefill/decode TFLite section (prefer model.toml entry).
        tflite = None
        toml_path = os.path.join(tmp, "model.toml")
        try:
            with open(toml_path) as f:
                toml = f.read()
            for block in toml.split("[[section]]"):
                if 'model_type = "prefill_decode"' in block:
                    for line in block.splitlines():
                        if line.strip().startswith("data_path"):
                            tflite = os.path.join(
                                tmp, line.split("=", 1)[1].strip().strip('"'))
        except OSError:
            pass
        if not tflite or not os.path.isfile(tflite):
            hits = sorted(glob.glob(os.path.join(tmp, "*PREFILL_DECODE*.tflite"))) \
                or sorted(glob.glob(os.path.join(tmp, "*prefill_decode*.tflite")))
            tflite = hits[0] if hits else None
        if not tflite:
            fail(["no prefill_decode TFLite section found in artifact"])

        # 2/3. Factual signature check via ai_edge_litert Interpreter.
        from ai_edge_litert.interpreter import Interpreter
        interp = Interpreter(model_path=tflite, num_threads=1)
        sl = interp.get_signature_list()
        if not sl:
            fail(["Interpreter returned no signature list"])

        prefill = {k: v for k, v in sl.items() if k.startswith("prefill")}
        decode = sl.get("decode")

        if not prefill:
            reasons.append("no prefill_* signatures found")
        else:
            for name, sig in sorted(prefill.items()):
                bad = [o for o in sig["outputs"] if not KV_RE.match(o)]
                if bad:
                    reasons.append(
                        f"prefill '{name}' has non-kv-cache output(s): "
                        f"{', '.join(sorted(bad))}")
        if decode is None:
            reasons.append("no 'decode' signature found")
        else:
            if "logits" not in decode["outputs"]:
                reasons.append("decode signature has no 'logits' output")
            if not any(KV_RE.match(o) for o in decode["outputs"]):
                reasons.append("decode signature has no kv-cache output")

        if reasons:
            fail(reasons)

    print(f"CONTRACT PASS: {artifact}")
    print(f"  prefill signatures ({len(prefill)}): "
          + ", ".join(sorted(prefill)))
    print("  prefill outputs: kv-cache only")
    print("  decode outputs: kv-cache + logits")


if __name__ == "__main__":
    main()
