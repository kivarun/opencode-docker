#!/usr/bin/env bash
set -e

cd /workspace

if [[ -f mise.toml ]]; then
    mise trust /workspace/mise.toml >/dev/null 2>&1 || true
fi

exec opencode "$@"
