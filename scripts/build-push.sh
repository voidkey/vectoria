#!/usr/bin/env bash
# Build the app image and push to Docker Hub (voidkey/vectoria).
# Run this locally or in CI — NOT on the production host.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE="${IMAGE:-voidkey/vectoria}"
PRIVATE_IMAGE="${PRIVATE_IMAGE:-faas-img-cn-beijing.cr.volces.com/vectoria/vectoria}"
TAG="${TAG:-$(git rev-parse --short HEAD)}"
# Force linux/amd64 so an ARM Mac (M1/M2/M3) builds an image the Linux prod host can actually run.
PLATFORM="${PLATFORM:-linux/amd64}"

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "Warning: working tree has uncommitted changes; tag $TAG may not reflect image contents." >&2
fi

echo "Building + pushing $IMAGE:$TAG and $PRIVATE_IMAGE:$TAG (also :latest) for $PLATFORM..."
docker buildx build \
    --platform "$PLATFORM" \
    --tag "$IMAGE:$TAG" \
    --tag "$IMAGE:latest" \
    --tag "$PRIVATE_IMAGE:$TAG" \
    --tag "$PRIVATE_IMAGE:latest" \
    --push \
    .

echo "Done. Deploy with:  ssh prod './scripts/deploy.sh'"
