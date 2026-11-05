#!/usr/bin/env bash

# Automatic OpenCode V1 update channel: fail-closed decisions for the
# upstream `ghcr.io/anomalyco/opencode:latest` digest pin.
#
# Dockerfile_base pins the upstream tag with a tracked manifest digest:
#
#   FROM ghcr.io/anomalyco/opencode:latest@sha256:<64 hex>
#
# The digest owns the exact build content and is refreshed by the
# opencode-updater workflow only after the content is proven to be
# OpenCode 1.x. This script is the single owner of those decisions:
#
# Usage:
#   scripts/opencode-update-gate.sh run
#       Full updater flow: resolve the upstream manifest digest, compare
#       it with the tracked digest in Dockerfile_base and - only when
#       they differ - prove the new content by pulling it, verifying the
#       daemon resolved exactly that digest, running the image and
#       checking `opencode --version`. Emits
#           decision=unchanged|update
#           resolved_digest=<sha256:...>
#           tracked_digest=<sha256:...>
#       plus, for an update,
#           new_digest=<sha256:...>
#           opencode_version=<1.x.y>
#       to $GITHUB_OUTPUT when set, otherwise to stdout. Any failure
#       exits 1 before any commit, pull request or image build.
#
#   scripts/opencode-update-gate.sh resolve
#       Resolve the upstream manifest digest only.
#
#   scripts/opencode-update-gate.sh verify-digest <sha256:...>
#       Pull the upstream tag and prove the daemon resolved exactly that
#       digest (RepoDigests check).
#
#   scripts/opencode-update-gate.sh probe-version <image-ref>
#       Run `opencode --version` in an upstream image reference. The
#       upstream image sets no HOME, so a temporary HOME is provided.
#
#   scripts/opencode-update-gate.sh check-version <image-ref>
#       Build gate for the freshly built base image: run
#       `opencode --version` without any environment override - which
#       also proves the base image's own HOME contract - and require a
#       proven `1.x.y`. Used by scripts/build-images.sh for base builds;
#       any other major, an unrecognized output or a failed run stops
#       the build before the image is saved or pushed.
#
#   scripts/opencode-update-gate.sh decide <tracked> <resolved> <version|"-"">
#       Pure decision (no network, no docker). Refuses with
#           version_unrecognized   output is not `MAJOR.MINOR.PATCH`
#           version_not_v1         a parsed version outside OpenCode 1.x
#           version_unavailable    the image could not be run
#
# Fail-closed rules:
#   - only a strict `1.x.y` version output reported by the image itself
#     is accepted; the version is never stored anywhere else;
#   - a resolved digest equal to the tracked digest exits as `unchanged`
#     without running the image, without a diff and without a PR;
#   - V2/unknown content, unrecognized output, a failed run or a digest
#     mismatch refuses the update before any commit or publication.

set -euo pipefail

UPSTREAM_REF="ghcr.io/anomalyco/opencode"
UPSTREAM_TAG="latest"
UPSTREAM_VERSION_PATTERN='^[0-9]+\.[0-9]+\.[0-9]+$'
BASE_DOCKERFILE="${BASE_DOCKERFILE:-Dockerfile_base}"

fail() {
    local reason="$1"
    shift
    printf 'opencode-update-gate: %s' "$reason" >&2
    if (( $# > 0 )); then
        printf ' (%s)' "$*" >&2
    fi
    printf '\n' >&2
    exit 1
}

output() {
    local key="$1"
    local value="$2"

    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        printf '%s=%s\n' "$key" "$value" >> "$GITHUB_OUTPUT"
    else
        printf '%s=%s\n' "$key" "$value"
    fi
}

is_digest() {
    [[ "$1" =~ ^sha256:[0-9a-f]{64}$ ]]
}

resolve_digest() {
    local token digest

    if ! token="$(
        curl -fsS "https://ghcr.io/token?service=ghcr.io&scope=repository:anomalyco/opencode:pull" \
            | jq -er '.token'
    )"; then
        fail "digest_unresolved" "could not obtain an anonymous registry token"
    fi

    if ! digest="$(
        curl -fsSI \
            -H "Authorization: Bearer ${token}" \
            -H 'Accept: application/vnd.oci.image.index.v1+json' \
            -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
            -H 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
            "https://ghcr.io/v2/anomalyco/opencode/manifests/${UPSTREAM_TAG}" \
            | tr -d '\r' \
            | awk 'tolower($1) == "docker-content-digest:" { print tolower($2) }'
    )"; then
        fail "digest_unresolved" "could not resolve the upstream manifest digest"
    fi

    if ! is_digest "$digest"; then
        fail "digest_unresolved" "the upstream manifest digest is not a sha256 digest"
    fi

    printf '%s\n' "$digest"
}

tracked_digest() {
    local digest

    digest="$(grep -Ec \
        "^FROM ${UPSTREAM_REF//./\\.}:latest@sha256:[0-9a-f]{64}\$" \
        "$BASE_DOCKERFILE" || true)"

    if [[ "$digest" != "1" ]]; then
        fail "tracked_digest_invalid" \
            "expected exactly one pinned upstream digest line in the base Dockerfile"
    fi

    digest="$(sed -nE \
        "s|^FROM ${UPSTREAM_REF//./\\.}:latest@sha256:([0-9a-f]{64})\$|sha256:\\1|p" \
        "$BASE_DOCKERFILE")"

    if ! is_digest "$digest"; then
        fail "tracked_digest_invalid" "the pinned upstream digest is not a sha256 digest"
    fi

    printf '%s\n' "$digest"
}

verify_digest() {
    local digest="$1"
    local repo_digests

    if ! is_digest "$digest"; then
        fail "digest_invalid" "expected a sha256 digest argument"
    fi

    if ! docker pull "${UPSTREAM_REF}:${UPSTREAM_TAG}" > /dev/null; then
        fail "digest_unresolved" "could not pull the upstream tag"
    fi

    repo_digests="$(
        docker image inspect --format '{{join .RepoDigests "\n"}}' \
            "${UPSTREAM_REF}:${UPSTREAM_TAG}" 2> /dev/null || true
    )"

    if ! printf '%s\n' "$repo_digests" | grep -Fx "${UPSTREAM_REF}@${digest}" > /dev/null; then
        fail "digest_mismatch" \
            "the pulled upstream tag did not resolve to the registry digest"
    fi
}

# Prints the trimmed stdout of `opencode --version` for the image
# reference. $2 selects the HOME handling:
#   pinned-home-env  provide a temporary HOME (upstream images set none)
#   image-home       run without any environment override (our base image
#                    pins HOME itself; the run therefore also proves the
#                    base HOME contract)
run_opencode_version() {
    local image_ref="$1"
    local home_mode="$2"
    local -a env_args=()

    if [[ "$home_mode" == "pinned-home-env" ]]; then
        env_args=(--env HOME=/tmp/opencode-version-probe)
    fi

    docker run --rm --entrypoint opencode \
        "${env_args[@]}" \
        "$image_ref" --version 2> /dev/null
}

# Refuses unless the trimmed output is exactly an OpenCode 1.x version.
# Prints the version on success.
decide_version() {
    local version="$1"

    version="${version#"${version%%[![:space:]]*}"}"
    version="${version%"${version##*[![:space:]]}"}"

    if [[ -z "$version" ]]; then
        fail "version_unrecognized" "the image reported no version output"
    fi

    if ! [[ "$version" =~ $UPSTREAM_VERSION_PATTERN ]]; then
        fail "version_unrecognized" "the version output is not MAJOR.MINOR.PATCH"
    fi

    if [[ "$version" != 1.* ]]; then
        fail "version_not_v1" "refusing OpenCode major versions other than 1"
    fi

    printf '%s\n' "$version"
}

probe_version() {
    local digest="$1"
    local version

    verify_digest "$digest"

    if ! version="$(run_opencode_version "${UPSTREAM_REF}@${digest}" pinned-home-env)"; then
        fail "version_unavailable" "could not run the upstream image at the resolved digest"
    fi

    decide_version "$version"
}

check_version() {
    local image_ref="$1"
    local version

    if [[ -z "$image_ref" ]]; then
        fail "invalid_options" "expected an image reference"
    fi

    if ! version="$(run_opencode_version "$image_ref" image-home)"; then
        fail "version_unavailable" "could not run the built image"
    fi

    version="$(decide_version "$version")"
    output version "$version"
}

decide() {
    local tracked="$1"
    local resolved="$2"
    local version="$3"

    if ! is_digest "$tracked" || ! is_digest "$resolved"; then
        fail "digest_invalid" "expected sha256 digests"
    fi

    if [[ "$tracked" == "$resolved" ]]; then
        output decision unchanged
        return 0
    fi

    if [[ "$version" == "-" ]]; then
        fail "version_unavailable" "the content changed but no version was proven"
    fi

    version="$(decide_version "$version")"

    output decision update
    output new_digest "$resolved"
    output opencode_version "$version"
}

MODE="${1:-}"

case "$MODE" in
    run)
        tracked="$(tracked_digest)"
        resolved="$(resolve_digest)"
        output resolved_digest "$resolved"
        output tracked_digest "$tracked"

        if [[ "$tracked" == "$resolved" ]]; then
            output decision unchanged
            exit 0
        fi

        version="$(probe_version "$resolved")"
        output decision update
        output new_digest "$resolved"
        output opencode_version "$version"
        ;;
    resolve)
        digest="$(resolve_digest)"
        printf '%s\n' "$digest"
        ;;
    verify-digest)
        verify_digest "${2:-}"
        ;;
    probe-version)
        if ! version="$(run_opencode_version "${2:-}" pinned-home-env)"; then
            fail "version_unavailable" "could not run the image"
        fi
        version="$(decide_version "$version")"
        printf '%s\n' "$version"
        ;;
    check-version)
        check_version "${2:-}"
        ;;
    decide)
        decide "${2:-}" "${3:-}" "${4--}"
        ;;
    *)
        echo "usage: $0 {run | resolve | verify-digest <digest> | probe-version <ref> | check-version <ref> | decide <tracked> <resolved> <version|->}" >&2
        exit 2
        ;;
esac
