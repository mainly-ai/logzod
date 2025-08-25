ARG BASE_IMAGE=ubuntu:22.04

FROM ${BASE_IMAGE} as builder

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
RUN mkdir -p /workspace/mirmod-rs /workspace/logzod

COPY mirmod-rs /workspace/mirmod-rs
COPY logzod /workspace/logzod

WORKDIR /workspace/logzod

RUN cargo build --release

FROM ${BASE_IMAGE} as runtime

COPY --from=builder /workspace/logzod/target/release/logzod /usr/local/bin/logzod

ENTRYPOINT ["/usr/local/bin/logzod"] 