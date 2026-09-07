# OpenCode Docker

Container images for running [OpenCode](https://opencode.ai/) in isolated,
repeatable development environments, plus an early deterministic orchestrator
built around [docker-helper](https://github.com/kivarun/docker-helper).

The project keeps the coding agent inside a container while Docker access stays
behind docker-helper policy. Agent containers do **not** need the Docker Engine
socket.

## Images

Images are published to GitHub Container Registry on pushes to the repository.
Every published image gets a commit-SHA tag; builds from the default branch also
get `latest`.

| Image | Purpose |
| --- | --- |
| `ghcr.io/kivarun/opencode-docker/base:latest` | OpenCode plus common CLI/development tools and docker-helper |
| `ghcr.io/kivarun/opencode-docker/go26:latest` | Base image plus Go 1.26 and native build dependencies |
| `ghcr.io/kivarun/opencode-docker/dotnet10:latest` | Base image plus .NET 10 SDK |
| `ghcr.io/kivarun/opencode-docker/gdk:latest` | Standalone GitLab Development Kit environment with OpenCode and docker-helper |
| `ghcr.io/kivarun/opencode-docker/orchestrator:latest` | Experimental deterministic launcher/orchestrator |

`base`, `go26`, `dotnet10`, and `orchestrator` run OpenCode workloads as the
non-root `opencode` user. `gdk` follows the user model of its upstream GDK
image.

## What is in the base image

`Dockerfile_base` starts from the official OpenCode image and adds a practical
agent toolset: Git, SSH, GitHub CLI, `jq`, `ripgrep`, `fd`, archive/media/PDF
utilities, ShellCheck, yamllint, BuildKit tooling, networking/debugging tools,
and related command-line utilities.

It also installs a pinned docker-helper release and its agent skill. The image
contains the **client**; the docker-helper daemon remains on the host.

Provider credentials and OpenCode model configuration are deliberately **not**
baked into the image.

## Quick start

### 1. Install docker-helper

Install and configure docker-helper on the host first:

<https://github.com/kivarun/docker-helper>

The examples below use the recommended system service and therefore expect the
socket at:

```text
/run/docker-helper/docker-helper.sock
```

A worker receives only its Session bearer and read-only access to the helper
socket directory. Never mount `docker.sock` into an agent container.

### 2. Prepare persistent OpenCode configuration

Keep container-specific OpenCode state outside the image:

```bash
export OPENCODE_DOCKER_HOME="${OPENCODE_DOCKER_HOME:-$HOME/.opencode-docker}"
mkdir -p \
  "$OPENCODE_DOCKER_HOME/.config/opencode" \
  "$OPENCODE_DOCKER_HOME/.local/share/opencode"
```

Put your normal OpenCode configuration in:

```text
$OPENCODE_DOCKER_HOME/.config/opencode/opencode.json
```

The images do not impose a provider or model. Configure OpenCode exactly as you
would on the host and pass any environment variables referenced by that config
to the container. See the upstream
[OpenCode configuration documentation](https://opencode.ai/docs/config/).

For example, if your config references `LLM_SERVER`, `LLM_KEY`, or another
provider-specific variable, add the corresponding `-e` option to the launcher.
The sample below passes `LLM_SERVER`, `LLM_KEY`, and `GH_TOKEN` as optional host
variables; Docker leaves them unset in the container when they are unset on the
host.

### 3. Add a small host launcher

The following shell functions create one docker-helper Session for the current
workspace, launch OpenCode, and remove the Session when the container exits.
They intentionally contain no registry credentials, corporate CA paths,
desktop/X11 plumbing, or workstation-specific paths.

Host requirements for this example: `docker`, `docker-helper`, and `jq`.

```bash
_dh_session_create() {
    local response

    unset DOCKER_HELPER_SESSION_ID DOCKER_HELPER_SESSION_TOKEN

    response="$(
        command docker-helper session create \
            --workspace "$PWD" \
            --json
    )" || return 1

    DOCKER_HELPER_SESSION_ID="$(
        jq -er '.session.id | select(type == "string" and length > 0)' \
            <<<"$response"
    )" || return 1

    DOCKER_HELPER_SESSION_TOKEN="$(
        jq -er '.token | select(type == "string" and length > 0)' \
            <<<"$response"
    )" || {
        command docker-helper session delete \
            --id "$DOCKER_HELPER_SESSION_ID" >/dev/null 2>&1 || true
        unset DOCKER_HELPER_SESSION_ID
        return 1
    }

    export DOCKER_HELPER_SESSION_TOKEN
}

_dh_session_delete() {
    [[ -n "${DOCKER_HELPER_SESSION_ID:-}" ]] || return 0
    command docker-helper session delete \
        --id "$DOCKER_HELPER_SESSION_ID" >/dev/null
}

_oc_run() (
    local image="$1"
    shift

    local helper_runtime_dir="${DOCKER_HELPER_RUNTIME_DIR:-/run/docker-helper}"
    local opencode_home="${OPENCODE_DOCKER_HOME:-$HOME/.opencode-docker}"
    local session_id=""

    [[ -S "$helper_runtime_dir/docker-helper.sock" ]] || {
        echo "docker-helper socket not found: $helper_runtime_dir/docker-helper.sock" >&2
        return 1
    }

    cleanup() {
        local rc="$?"
        trap - EXIT
        DOCKER_HELPER_SESSION_ID="$session_id" _dh_session_delete || \
            echo "warning: failed to delete docker-helper session $session_id" >&2
        exit "$rc"
    }
    trap cleanup EXIT

    _dh_session_create || return 1
    session_id="$DOCKER_HELPER_SESSION_ID"

    docker run --rm -it \
        --init \
        --cap-drop ALL \
        --security-opt no-new-privileges=true \
        -e LLM_SERVER \
        -e LLM_KEY \
        -e GH_TOKEN \
        -e DOCKER_HELPER_SESSION_TOKEN \
        -v "$PWD:/workspace" \
        -v "$opencode_home/.config:/home/opencode/.config" \
        -v "$opencode_home/.local/share/opencode:/home/opencode/.local/share/opencode" \
        -v "$helper_runtime_dir:/run/docker-helper:ro" \
        -w /workspace \
        "$image" "$@"
)

oc() {
    _oc_run ghcr.io/kivarun/opencode-docker/base:latest "$@"
}

oc-go26() {
    _oc_run ghcr.io/kivarun/opencode-docker/go26:latest "$@"
}

oc-dotnet10() {
    _oc_run ghcr.io/kivarun/opencode-docker/dotnet10:latest "$@"
}
```

Then run OpenCode from any authorized workspace:

```bash
cd /path/to/project
oc
```

Or use a language image:

```bash
oc-go26
oc-dotnet10
```

### Optional host integration

Keep workstation-specific integrations outside the generic launcher. Add only
what you actually need, for example:

- forward an existing SSH agent with `SSH_AUTH_SOCK` instead of copying a
  private key into the image;
- pass `GH_TOKEN` for GitHub CLI/API access;
- mount a shared exchange directory;
- add X11/Wayland/DBus mounts for desktop integrations;
- mount a private CA when your environment requires one;
- add SELinux bind-mount labels appropriate for your host.

These are host policy choices, not requirements of the images.

## docker-helper model

A typical direct worker launch looks like this:

```text
host / trusted launcher
    |
    | creates Session for the workspace
    v
docker-helper
    |
    | Session bearer + helper socket
    v
OpenCode worker container
```

The worker never receives `docker.sock`. docker-helper remains the policy and
authorization boundary for build/run operations and workspace mounts.

The helper socket is a transport, not authority by itself: the Session bearer
controls what the worker can do. Mount the helper runtime directory read-only;
this also keeps the container connected to the socket path if the daemon
replaces its socket during restart.

## Experimental orchestrator

`Dockerfile_orchestrator` contains the first deterministic orchestration layer
for docker-helper Launcher delegation. It is currently a development/proving
surface rather than a complete agent pipeline.

The orchestrator:

1. authenticates with a **Launcher credential**;
2. creates a child Session;
3. runs a worker under that Session;
4. validates the result;
5. cleans up the child Session, including signal/error paths.

Two commands currently exist:

```text
orchestrator smoke
orchestrator agent-smoke
```

`agent-smoke` additionally runs an OpenCode worker against a task file and
verifies its result contract.

### Orchestrator runtime requirements

The orchestrator needs:

- the docker-helper socket;
- a Launcher credential (never a Principal/admin credential);
- a workspace that is visible to both the orchestrator container and the host
  at the **same absolute path**.

The last point matters because docker-helper stores and validates host workspace
paths. A common container launch pattern is therefore:

```bash
workspace="$(realpath "$PWD")"
config_dir="$HOME/.config/opencode-docker-orchestrator"

# Install a Launcher credential into this private config directory first:
# XDG_CONFIG_HOME="$config_dir" docker-helper credential install

docker run --rm -it \
    --init \
    --cap-drop ALL \
    --security-opt no-new-privileges=true \
    -e XDG_CONFIG_HOME=/config \
    -e DOCKER_HELPER_SOCKET_PATH=/run/docker-helper/docker-helper.sock \
    -v /run/docker-helper:/run/docker-helper:ro \
    -v "$config_dir:/config:ro" \
    -v "$workspace:$workspace" \
    ghcr.io/kivarun/opencode-docker/orchestrator:latest \
    smoke --workspace "$workspace"
```

For `agent-smoke`, pass an explicit worker image. Model/provider environment is
forwarded by an allowlist; Launcher credentials and admin credentials are not
forwarded to workers.

## GDK image

`gdk` is a standalone image based on GitLab's GDK-in-a-box image. It contains
OpenCode and docker-helper but does not inherit from `Dockerfile_base`.

It exists for GitLab development workflows and may require additional GDK- or
host-specific mounts/configuration. Those environment-specific launch options
are intentionally not part of the generic quick start above.

## Image build model

Every image is described by a `Dockerfile_*` in the repository root.

Normal images inherit from the common base and must contain:

```dockerfile
ARG BASE_IMAGE
FROM ${BASE_IMAGE}
```

A truly independent image declares:

```dockerfile
# ci: standalone
```

A Dockerfile may also declare extra paths that should trigger its rebuild:

```dockerfile
# ci: watch=orchestrator/**
```

The Dockerfile itself is always watched.

The GitHub Actions workflow first plans which images are affected and then
builds independent images in parallel. If the base image changes, all dependent
images are rebuilt against that exact base commit.

Pull requests build affected images without publishing them. Pushes publish the
commit-SHA tags to GHCR, and the repository's default branch additionally
updates `latest`.

## Adding an image

1. Add `Dockerfile_<name>` in the repository root.
2. Inherit from `${BASE_IMAGE}`, or mark the image `# ci: standalone`.
3. Add `# ci: watch=<glob>` annotations for non-Dockerfile sources used only by
   that image.
4. Push the change. CI discovers the image automatically; no central image list
   needs to be edited.

## Project status

The image build/publish path and direct docker-helper-backed worker workflow are
usable today. The orchestrator is intentionally small while the deterministic
agent pipeline is being developed and dogfooded.

A project manifesto will document the longer-term product boundary and design
principles separately.

## License

See [LICENSE](LICENSE).
