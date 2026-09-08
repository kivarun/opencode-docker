# AGENTS.md

Repo of Dockerfiles for OpenCode agent images (published to GHCR via CI) plus an experimental Bun/TypeScript orchestrator under `orchestrator/`.

## Verification commands

- Orchestrator tests (only test suite in repo): `cd orchestrator && bun test`
  - Tests use fake CLI runners and temp dirs; no docker, network, or docker-helper needed.
- CI (`.github/workflows/`) only plans/builds images — it does **not** run orchestrator tests. Run `bun test` locally before pushing.
- Shell/YAML are written to pass shellcheck/yamllint (see `# yamllint disable-line` markers in workflows): `shellcheck scripts/*.sh gdk/*.sh`, `yamllint .github/`.

## Image build model

- Images are auto-discovered from root `Dockerfile_*` files; never edit a central list.
- Every normal image must declare `ARG BASE_IMAGE` + `FROM ${BASE_IMAGE}` (enforced by `scripts/build-images.sh plan`), or mark the Dockerfile `# ci: standalone` (only `Dockerfile_gdk` is standalone).
- `# ci: watch=<glob>` adds watched source paths; the Dockerfile itself is always watched. Changes under `scripts/**`, `.github/**`, or `.dockerignore` trigger a rebuild of **all** images.
- Local build of a dependent image requires `--build-arg BASE_IMAGE=...` (e.g. `ghcr.io/kivarun/opencode-docker/base:latest`); `build-images.sh build <name>` needs `EVENT_NAME`, `COMMIT_SHA`, and `REGISTRY_PREFIX` set.
- Bumping docker-helper means updating **both** `DOCKER_HELPER_VERSION` and `DOCKER_HELPER_SHA256` in `Dockerfile_base` (and `Dockerfile_gdk`, which pins it separately).

## Orchestrator (`orchestrator/`)

- Plain Bun TypeScript with **no package.json/lockfile**: Bun runs `src/main.ts` directly. Imports use explicit `.ts` extensions; type-only imports must use `import type` (`verbatimModuleSyntax`). Strict mode plus `noUncheckedIndexedAccess` — index access needs guards.
- CLI: `bun src/main.ts smoke|agent-smoke`; `agent-smoke` requires `--image` (explicit opt-in). Needs the docker-helper socket and a **Launcher** credential (never a Principal/admin credential) at `${XDG_CONFIG_HOME:-$HOME/.config}/docker-helper/credential.token`.
- `--workspace` must be an absolute path visible at the *same* absolute path inside and outside the container (docker-helper validates host paths).

## Hard invariants

- Never mount `docker.sock` into agent containers; docker-helper is the only Docker access path.
- Images must not bake in provider credentials or OpenCode model config.
- The orchestrator worker env is a strict allowlist (`src/main.ts` usage text); launcher credentials, admin tokens, and the orchestrator state path are never forwarded to workers. Extend the allowlist only deliberately.
- `base`/`go26`/`dotnet10`/`orchestrator` run as non-root `opencode` user (uid/gid 1000); `gdk` follows its upstream GDK image user model.

## Docs pointers

- `docs/manifesto.md` — design principles (OpenCode is an execution engine, not state authority; the orchestrator owns state).
- `docs/orchestration-workflow.md` — design sketch: only `smoke`/`agent-smoke` are implemented; don't treat described future interfaces as existing.
- agent-smoke result contract: worker writes a structured `result.json`, `TASK.md` digest must be unchanged; fixture in `orchestrator/fixtures/agent-smoke/`.
