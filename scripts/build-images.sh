#!/usr/bin/env bash

# Build (and optionally publish) the Docker images defined by the
# Dockerfile_* files in the repository root.
#
# Image selection is driven by annotations inside each Dockerfile:
#
#   # ci: standalone     image does not build on top of the shared base image
#   # ci: watch=<glob>   build the image when a changed path matches <glob>
#
# The Dockerfile itself is always watched. Changes to CI infrastructure
# (.github/workflows/**, .github/actions/**, scripts/**, .dockerignore)
# rebuild all images.
#
# Environment:
#   EVENT_NAME       push | pull_request | workflow_dispatch (required)
#   COMMIT_SHA       commit to build and tag (required)
#   BEFORE_SHA       previous head for push events (optional)
#   PR_BASE_SHA      base commit for pull_request events (optional)
#   BRANCH           branch name; "latest" is tagged when it equals DEFAULT_BRANCH
#   DEFAULT_BRANCH   default branch name
#   REGISTRY_PREFIX  image prefix without tag, e.g. ghcr.io/owner/repo (required)
#   PUBLISH          "1" to push images to the registry (default "0")
#   BASE_IMAGE_NAME  name of the shared base image (default "base")

set -euo pipefail

cd "${GITHUB_WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

EVENT_NAME="${EVENT_NAME:?EVENT_NAME is not set}"
COMMIT_SHA="${COMMIT_SHA:?COMMIT_SHA is not set}"
BEFORE_SHA="${BEFORE_SHA:-}"
PR_BASE_SHA="${PR_BASE_SHA:-}"
BRANCH="${BRANCH:-}"
DEFAULT_BRANCH="${DEFAULT_BRANCH:-}"
REGISTRY_PREFIX="${REGISTRY_PREFIX:?REGISTRY_PREFIX is not set}"
PUBLISH="${PUBLISH:-0}"
BASE_IMAGE_NAME="${BASE_IMAGE_NAME:-base}"

REGISTRY_PREFIX="${REGISTRY_PREFIX,,}"
BASE_IMAGE="${REGISTRY_PREFIX}/${BASE_IMAGE_NAME}"

is_standalone() {
    grep -Eq \
        '^[[:space:]]*#[[:space:]]*ci:[[:space:]]*standalone[[:space:]]*$' \
        "$1"
}

watch_paths() {
    local dockerfile="$1"

    printf '%s\n' "$dockerfile"

    sed -nE \
        's/^[[:space:]]*#[[:space:]]*ci:[[:space:]]*watch[[:space:]]*=[[:space:]]*(.+)[[:space:]]*$/\1/p' \
        "$dockerfile"
}

validate_base_image() {
    local dockerfile="$1"

    if is_standalone "$dockerfile"; then
        return 0
    fi

    if ! grep -Eq \
        '^[[:space:]]*ARG[[:space:]]+BASE_IMAGE([=[:space:]]|$)' \
        "$dockerfile"; then
        echo "${dockerfile}: missing 'ARG BASE_IMAGE' or '# ci: standalone'" >&2
        return 1
    fi

    if ! grep -Eq \
        '^[[:space:]]*FROM[[:space:]]+\$\{?BASE_IMAGE\}?([[:space:]]+[Aa][Ss][[:space:]]+[^[:space:]]+)?[[:space:]]*$' \
        "$dockerfile"; then
        echo "${dockerfile}: missing 'FROM \${BASE_IMAGE}'" >&2
        return 1
    fi
}

path_matches() {
    local changed_path="$1"
    local watched_path="$2"

    # Unquoted right-hand side is intentional: watched_path is a glob.
    # shellcheck disable=SC2053
    [[ "$changed_path" == $watched_path ]]
}

image_has_changes() {
    local dockerfile="$1"
    local changed_path
    local watched_path

    while IFS= read -r watched_path; do
        while IFS= read -r changed_path; do
            if path_matches "$changed_path" "$watched_path"; then
                return 0
            fi
        done <<< "$CHANGED_FILES"
    done < <(watch_paths "$dockerfile")

    return 1
}

infra_changed() {
    local changed_path

    while IFS= read -r changed_path; do
        case "$changed_path" in
            .github/workflows/* | .github/actions/* | scripts/* | .dockerignore)
                return 0
                ;;
        esac
    done <<< "$CHANGED_FILES"

    return 1
}

changed_files() {
    case "$EVENT_NAME" in
        workflow_dispatch)
            return 0
            ;;
        pull_request)
            git diff --name-only "$PR_BASE_SHA" "$COMMIT_SHA"
            return
            ;;
        push)
            if [[ -z "$BEFORE_SHA" || "$BEFORE_SHA" =~ ^0+$ ]]; then
                git diff-tree --root --no-commit-id --name-only -r "$COMMIT_SHA"
                return
            fi
            git diff --name-only "$BEFORE_SHA" "$COMMIT_SHA"
            return
            ;;
        *)
            echo "unsupported event: ${EVENT_NAME}" >&2
            return 1
            ;;
    esac
}

shopt -s nullglob

dockerfiles=(Dockerfile_*)

if (( ${#dockerfiles[@]} == 0 )); then
    echo "No Dockerfile_* files found" >&2
    exit 1
fi

if [[ ! -f "Dockerfile_${BASE_IMAGE_NAME}" ]]; then
    echo "Base Dockerfile not found: Dockerfile_${BASE_IMAGE_NAME}" >&2
    exit 1
fi

for dockerfile in "${dockerfiles[@]}"; do
    image_name="${dockerfile#Dockerfile_}"

    if [[ "$image_name" != "$BASE_IMAGE_NAME" ]]; then
        validate_base_image "$dockerfile"
    fi
done

CHANGED_FILES="$(changed_files)"

BUILD_ALL=false

if [[ "$EVENT_NAME" == "workflow_dispatch" ]]; then
    BUILD_ALL=true
fi

if infra_changed; then
    BUILD_ALL=true
fi

base_changed=false

if [[ "$BUILD_ALL" == true ]] || image_has_changes "Dockerfile_${BASE_IMAGE_NAME}"; then
    base_changed=true
fi

declare -a selected_standalone=()
declare -a selected_dependent=()

for dockerfile in "${dockerfiles[@]}"; do
    image_name="${dockerfile#Dockerfile_}"

    if [[ "$image_name" == "$BASE_IMAGE_NAME" ]]; then
        continue
    fi

    if is_standalone "$dockerfile"; then
        if [[ "$BUILD_ALL" == true ]] || image_has_changes "$dockerfile"; then
            selected_standalone+=("$dockerfile")
        fi
    else
        if [[ "$BUILD_ALL" == true ]] \
            || [[ "$base_changed" == true ]] \
            || image_has_changes "$dockerfile"; then
            selected_dependent+=("$dockerfile")
        fi
    fi
done

build_image() {
    local dockerfile="$1"
    local tag="$2"
    shift 2

    echo "docker build ${tag}"
    docker build "$@" --file "$dockerfile" --tag "$tag" .
}

push_image() {
    local image="$1"
    local tag="${image}:${COMMIT_SHA}"

    if [[ "$PUBLISH" != "1" ]]; then
        echo "skip push ${tag} (publish disabled)"
        return 0
    fi

    docker push "$tag"

    if [[ -n "$BRANCH" && "$BRANCH" == "$DEFAULT_BRANCH" ]]; then
        docker tag "$tag" "${image}:latest"
        docker push "${image}:latest"
    fi
}

built_any=false

if [[ "$base_changed" == true ]]; then
    build_image "Dockerfile_${BASE_IMAGE_NAME}" "${BASE_IMAGE}:${COMMIT_SHA}" --pull
    push_image "$BASE_IMAGE"
    built_any=true
fi

for dockerfile in "${selected_standalone[@]}"; do
    image_name="${dockerfile#Dockerfile_}"
    image="${REGISTRY_PREFIX}/${image_name}"

    build_image "$dockerfile" "${image}:${COMMIT_SHA}" --pull
    push_image "$image"
    built_any=true
done

for dockerfile in "${selected_dependent[@]}"; do
    image_name="${dockerfile#Dockerfile_}"
    image="${REGISTRY_PREFIX}/${image_name}"

    if [[ "$base_changed" == true ]]; then
        build_image "$dockerfile" "${image}:${COMMIT_SHA}" \
            --build-arg "BASE_IMAGE=${BASE_IMAGE}:${COMMIT_SHA}"
    else
        build_image "$dockerfile" "${image}:${COMMIT_SHA}" \
            --pull \
            --build-arg "BASE_IMAGE=${BASE_IMAGE}:latest"
    fi
    push_image "$image"
    built_any=true
done

if [[ "$built_any" == false ]]; then
    echo "No affected images found."
fi
