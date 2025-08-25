#!/bin/bash

set -e

BASE_IMAGE=${1:-ubuntu:22.04}

echo "Creating build context..."
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$BUILD_DIR"' EXIT

cp -r ../mirmod-rs "$BUILD_DIR/mirmod-rs"
cp -r . "$BUILD_DIR/logzod"

cp Dockerfile "$BUILD_DIR/"

echo "Building Docker image with base image: $BASE_IMAGE..."
docker build -t logzod-builder --build-arg BASE_IMAGE="$BASE_IMAGE" "$BUILD_DIR"

echo "Extracting binary..."
CONTAINER_ID=$(docker create logzod-builder)
docker cp $CONTAINER_ID:/usr/local/bin/logzod ./logzod
docker rm $CONTAINER_ID

chmod +x ./logzod

echo "Build complete! Binary is available at ./logzod" 