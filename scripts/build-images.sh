#!/usr/bin/env bash

# Plan and build the Docker images defined by the Dockerfile_* files in the
# repository root.
#
# Usage:
#   scripts/build-images.sh plan
#       Compute the affected images for the current event and emit
#           base_changed=<true|false>
#           standalone=<JSON array of image names>
#           dependent=<JSON array of image names>
#       to $GITHUB_OUTPUT when set, otherwise to stdout. Fails if any
#       dependent Dockerfile is invalid.
#
#   scripts/build-images.sh build <image-name>
#       Build (and optionally publish) one selected image.
#
# Image selection is driven by annotations inside each Dockerfile:
#
#   # ci: standalone     image does not build on top of the shared base image
#   # ci: watch=<glob>   build the image when a changed path matches <glob>
#
# The Dockerfile itself is always watched. Changes to CI infrastructure
# (.github/workflows/**, .github/actions/**, scripts/**, .dockerignore)
# select all images.
#
# Environment for "plan":
#   EVENT_NAME       push | pull_request | workflow_dispatch (required)
#   COMMIT_SHA       commit to plan for (required)
#   BEFORE_SHA       previous head for push events (optional)
#   PR_BASE_SHA      base commit for pull_request events (optional)
#
# Environment for "build":
#   EVENT_NAME       push | pull_request | workflow_dispatch (required)
#   COMMIT_SHA       commit to build and tag (required)
#   REGISTRY_PREFIX  image prefix without tag, e.g. ghcr.io/owner/repo (required)
#   PUBLISH          "1" to push the image to the registry (default "0")
#   BRANCH           branch name; "latest" is tagged when it equals DEFAULT_BRANCH
#   DEFAULT_BRANCH   default branch name
#   BASE_CHANGED     "true" when the base image was built in this run
#                    (dependent images only)
#   SAVE_BASE_TO     path to save the base image to when publishing is
#                    disabled (base image only)

set -euo pipefail

cd "${GITHUB_WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

BASE_IMAGE_NAME="${BASE_IMAGE_NAME:-base}"

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

json_array() {
    local out="[" first=true item

    for item in "$@"; do
        if [[ "$first" == true ]]; then
            first=false
        else
            out+=","
        fi
        out+="\"${item}\""
    done

    printf '%s]' "$out"
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

MODE="${1:-}"

if [[ "$MODE" == "plan" ]]; then
    EVENT_NAME="${EVENT_NAME:?EVENT_NAME is not set}"
    COMMIT_SHA="${COMMIT_SHA:?COMMIT_SHA is not set}"
    BEFORE_SHA="${BEFORE_SHA:-}"
    PR_BASE_SHA="${PR_BASE_SHA:-}"

    if [[ ! -f "Dockerfile_${BASE_IMAGE_NAME}" ]]; then
        echo "Base Dockerfile not found: Dockerfile_${BASE_IMAGE_NAME}" >&2
        exit 1
    fi

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

    shopt -s nullglob

    dockerfiles=(Dockerfile_*)

    if (( ${#dockerfiles[@]} == 0 )); then
        echo "No Dockerfile_* files found" >&2
        exit 1
    fi

    for dockerfile in "${dockerfiles[@]}"; do
        image_name="${dockerfile#Dockerfile_}"

        if [[ "$image_name" == "$BASE_IMAGE_NAME" ]]; then
            continue
        fi

        validate_base_image "$dockerfile"
    done

    declare -a selected_standalone=()
    declare -a selected_dependent=()

    for dockerfile in "${dockerfiles[@]}"; do
        image_name="${dockerfile#Dockerfile_}"

        if [[ "$image_name" == "$BASE_IMAGE_NAME" ]]; then
            continue
        fi

        if is_standalone "$dockerfile"; then
            if [[ "$BUILD_ALL" == true ]] || image_has_changes "$dockerfile"; then
                selected_standalone+=("$image_name")
            fi
        else
            if [[ "$BUILD_ALL" == true ]] \
                || [[ "$base_changed" == true ]] \
                || image_has_changes "$dockerfile"; then
                selected_dependent+=("$image_name")
            fi
        fi
    done

    if [[ "$base_changed" == true ]]; then
        output base_changed true
    else
        output base_changed false
    fi
    output standalone "$(json_array "${selected_standalone[@]}")"
    output dependent "$(json_array "${selected_dependent[@]}")"
elif [[ "$MODE" == "build" ]]; then
    IMAGE_NAME="${2:-}"

    if [[ -z "$IMAGE_NAME" ]]; then
        echo "usage: $0 build <image-name>" >&2
        exit 2
    fi

    EVENT_NAME="${EVENT_NAME:?EVENT_NAME is not set}"
    COMMIT_SHA="${COMMIT_SHA:?COMMIT_SHA is not set}"
    REGISTRY_PREFIX="${REGISTRY_PREFIX:?REGISTRY_PREFIX is not set}"
    PUBLISH="${PUBLISH:-0}"
    BRANCH="${BRANCH:-}"
    DEFAULT_BRANCH="${DEFAULT_BRANCH:-}"
    BASE_CHANGED="${BASE_CHANGED:-false}"
    SAVE_BASE_TO="${SAVE_BASE_TO:-}"

    REGISTRY_PREFIX="${REGISTRY_PREFIX,,}"
    BASE_IMAGE="${REGISTRY_PREFIX}/${BASE_IMAGE_NAME}"

    dockerfile="Dockerfile_${IMAGE_NAME}"

    if [[ ! -f "$dockerfile" ]]; then
        echo "Dockerfile not found: $dockerfile" >&2
        exit 1
    fi

    tag="${REGISTRY_PREFIX}/${IMAGE_NAME}:${COMMIT_SHA}"

    if [[ "$IMAGE_NAME" == "$BASE_IMAGE_NAME" ]]; then
        docker build --pull --file "$dockerfile" --tag "$tag" .

        if [[ -n "$SAVE_BASE_TO" && "$PUBLISH" != "1" ]]; then
            mkdir -p "$(dirname "$SAVE_BASE_TO")"
            docker save "$tag" | gzip > "$SAVE_BASE_TO"
        fi
    elif is_standalone "$dockerfile"; then
        docker build --pull --file "$dockerfile" --tag "$tag" .
    else
        validate_base_image "$dockerfile"

        pull_args=(--pull)
        if [[ "$BASE_CHANGED" == "true" ]]; then
            base_ref="${BASE_IMAGE}:${COMMIT_SHA}"
            if [[ "$EVENT_NAME" == "pull_request" ]]; then
                pull_args=()
            fi
        else
            base_ref="${BASE_IMAGE}:latest"
        fi

        docker build "${pull_args[@]}" \
            --build-arg "BASE_IMAGE=${base_ref}" \
            --file "$dockerfile" \
            --tag "$tag" .
    fi

    if [[ "$PUBLISH" == "1" ]]; then
        docker push "$tag"

        if [[ -n "$BRANCH" && "$BRANCH" == "$DEFAULT_BRANCH" ]]; then
            latest="${REGISTRY_PREFIX}/${IMAGE_NAME}:latest"
            docker tag "$tag" "$latest"
            docker push "$latest"
        fi
    else
        echo "skip push ${tag} (publish disabled)"
    fi
else
    echo "usage: $0 {plan | build <image-name>}" >&2
    exit 2
fi
