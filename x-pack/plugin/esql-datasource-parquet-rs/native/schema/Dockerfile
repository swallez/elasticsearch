# Build the FlatBuffers compiler

FROM debian:trixie-slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    cmake \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Checkout only what's needed to build flatc.
RUN git clone --filter=blob:none --no-checkout https://github.com/google/flatbuffers/ \
    && cd flatbuffers \
    && git sparse-checkout init --cone \
    && git sparse-checkout set CMake include src grpc samples tests

# Build the specific version
ARG VERSION

RUN cd flatbuffers  \
    && git checkout $VERSION \
    && cmake -DCMAKE_BUILD_TYPE=Release \
    && make -j4

FROM debian:trixie-slim

COPY --from=build /workspace/flatbuffers/flatc /usr/bin/

WORKDIR /workspace
