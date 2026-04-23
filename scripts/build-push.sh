#!/usr/bin/env bash
# Build the app image and push to Docker Hub (voidkey/vectoria).
# Run this locally or in CI — NOT on the production host.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${IMAGE:-voidkey/vectoria}"
# Optional extra registry (e.g. a private/mirror registry close to your deploy).
# Leave unset to push only to $IMAGE.
PRIVATE_IMAGE="${PRIVATE_IMAGE:-}"
TAG="${TAG:-$(git rev-parse --short HEAD)}"
# Force linux/amd64 so an ARM Mac (M1/M2/M3) builds an image the Linux prod host can actually run.
PLATFORM="${PLATFORM:-linux/amd64}"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Warning: working tree has uncommitted changes; tag $TAG may not reflect image contents." >&2
fi

TAGS=(--tag "$IMAGE:$TAG" --tag "$IMAGE:latest")
if [[ -n "$PRIVATE_IMAGE" ]]; then
    TAGS+=(--tag "$PRIVATE_IMAGE:$TAG" --tag "$PRIVATE_IMAGE:latest")
    echo "Building + pushing $IMAGE:$TAG and $PRIVATE_IMAGE:$TAG (also :latest) for $PLATFORM..."
else
    echo "Building + pushing $IMAGE:$TAG (also :latest) for $PLATFORM..."
fi

docker buildx build \
    --platform "$PLATFORM" \
    "${TAGS[@]}" \
    --push \
    .

echo "Done. Deploy with:  ssh your-host './scripts/deploy.sh'"
