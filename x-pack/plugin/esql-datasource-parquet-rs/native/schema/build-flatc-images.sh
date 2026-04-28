#!/bin/sh
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.
#

set -eu

# Version used by Arrow-Java
JAVA_VERSION="v23.5.26"
# Version used by Arrow-Rust
RUST_VERSION="v25.12.19"

TAG="flatbuffers"

docker build --build-arg VERSION="$JAVA_VERSION" --tag "$TAG:$JAVA_VERSION" .

docker build --build-arg VERSION="$RUST_VERSION" --tag "$TAG:$RUST_VERSION" .

