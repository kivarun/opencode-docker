# AGENTS.md

Repo of Dockerfiles for OpenCode agent images (published to GHCR via CI) plus an experimental Bun/TypeScript orchestrator under `orchestrator/`.

## Verification commands

- Orchestrator tests (only test suite in repo): `cd orchestrator && bun test`
  - Tests use fake CLI runners and temp dirs; no docker, network, or docker-helper needed.
- Type checking (from repo root): `tsc --noEmit -p orchestrator`. `bun test` does not type-check TypeScript.
  - Use a prepared development environment with TypeScript (`tsc`) on PATH and `@types/bun` resolvable by `orchestrator/tsconfig.json`; the repo has no package.json/lockfile or dependency-install step for these tools.
- CI (`.github/workflows/`) only plans/builds images — it does **not** run orchestrator tests. Run `bun test` locally before pushing.
- Shell/workflow checks (from repo root): `shellcheck scripts/*.sh gdk/*.sh`, `yamllint .github/`, `actionlint`. Preserve the existing targeted `# yamllint disable-line` markers in workflows.
- If required tools or dependencies are unavailable, report the affected checks as not run, not passed.

## Image build model

- Images are auto-discovered from root `Dockerfile_*` files; never edit a central list.
- `Dockerfile_base` defines the shared base and is exempt from dependent-image validation. Every other image must declare `ARG BASE_IMAGE` + `FROM ${BASE_IMAGE}`, or be marked `# ci: standalone` (currently `Dockerfile_gdk`); `scripts/build-images.sh plan` enforces this.
- `# ci: watch=<glob>` adds watched source paths; the Dockerfile itself is always watched. Changes under `.github/workflows/**`, `.github/actions/**`, `scripts/**`, or to `.dockerignore` trigger a rebuild of **all** images.
- Preserve selective, parallel builds: a changed base selects all dependents, not unchanged standalone images. Standalone jobs do not wait for base; selected dependents run in a matrix after base succeeds or is skipped. Empty selections must skip jobs before matrix expansion.
- PR builds must not log in to GHCR, publish images, or receive `packages: write`. Preserve the caller permission boundary in `.github/workflows/images.yml`; skipped login/push steps alone are not a token permission boundary.
- Local build of a dependent image requires `--build-arg BASE_IMAGE=...` (e.g. `ghcr.io/kivarun/opencode-docker/base:latest`); `build-images.sh build <name>` needs `EVENT_NAME`, `COMMIT_SHA`, and `REGISTRY_PREFIX` set.
- Bumping docker-helper means updating **both** `DOCKER_HELPER_VERSION` and `DOCKER_HELPER_SHA256` in `Dockerfile_base` (and `Dockerfile_gdk`, which pins it separately).

## Orchestrator (`orchestrator/`)

- Plain Bun TypeScript with **no package.json/lockfile**: Bun runs `src/main.ts` directly. Imports use explicit `.ts` extensions; type-only imports must use `import type` (`verbatimModuleSyntax`). Strict mode plus `noUncheckedIndexedAccess` — index access needs guards.
- CLI: `bun src/main.ts smoke|agent-smoke`; `agent-smoke` requires `--image` (explicit opt-in). Needs the docker-helper socket and a **Launcher** credential (never a Principal/admin credential) at `${XDG_CONFIG_HOME:-$HOME/.config}/docker-helper/credential.token`.
- `--workspace` is the absolute host workspace path used by docker-helper; it must also be visible at that same absolute path inside the orchestrator container. Workers mount the Session workspace at `/workspace` (`--mount .:/workspace`); the host path need not be reproduced inside workers.

## Hard invariants

- Never mount `docker.sock` into the orchestrator or agent containers; docker-helper is the only Docker access path.
- Images must not bake in provider credentials or OpenCode model config.
- The agent worker env uses `AGENT_WORKER_ENV_ALLOWLIST` in `orchestrator/src/worker.ts` plus explicit per-run bindings. Launcher credentials, admin tokens, and the orchestrator state path are never forwarded. Extend the allowlist only deliberately, keeping `orchestrator/src/main.ts` usage text and tests in sync.
- For `agent-smoke`, exit code 0 or agent JSON events alone never mean success: validate the `result.json` schema, `run_id`, artifact existence and workspace confinement (including symlinks), and unchanged task input before accepting the result.
- Every created child Session must be cleaned up on all controlled exit paths, including errors and cancellation. Cleanup failure makes the overall run unsuccessful, even when the worker succeeded.
- `base`/`go26`/`dotnet10`/`orchestrator` run as non-root `opencode` user (uid/gid 1000); `gdk` follows its upstream GDK image user model.

## Docs pointers

- `docs/manifesto.md` — design principles (OpenCode is an execution engine, not state authority; the orchestrator owns state).
- `docs/orchestration-workflow.md` — design sketch: only `smoke`/`agent-smoke` are implemented; don't treat described future interfaces as existing.
- agent-smoke result contract: worker writes a structured `result.json`, `TASK.md` digest must be unchanged; fixture in `orchestrator/fixtures/agent-smoke/`.
