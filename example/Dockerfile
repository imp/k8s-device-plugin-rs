# syntax=docker/dockerfile:1
#
# Build from the repository root (the build context must include the
# workspace Cargo.toml/Cargo.lock and the proto/kubelet git submodule):
#
#   git submodule update --init
#   docker build -f example/Dockerfile -t k8s-device-plugin-example .
#
# For multi-arch images, drive it through buildx so each platform's builder
# stage actually runs as that platform (native or emulated) rather than
# cross-compiling:
#
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       -f example/Dockerfile -t k8s-device-plugin-example .

ARG RUST_VERSION=1.90

FROM rust:${RUST_VERSION}-slim AS builder
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler musl-tools \
    && rm -rf /var/lib/apt/lists/*
RUN case "${TARGETARCH}" in \
        amd64) echo x86_64-unknown-linux-musl > /rust_target ;; \
        arm64) echo aarch64-unknown-linux-musl > /rust_target ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac
RUN rustup target add "$(cat /rust_target)"
WORKDIR /src
COPY . .
RUN mkdir /out \
    && cargo build --release --locked --target "$(cat /rust_target)" -p k8s-device-plugin-example \
    && cp "target/$(cat /rust_target)/release/k8s-device-plugin-example" /out/k8s-device-plugin-example

# Runs as root (uid 0) deliberately: kubelet's /var/lib/kubelet/device-plugins
# directory and most real device nodes under /dev are root-owned. If your
# hardware allows a stricter security context, switch to the `:nonroot` tag
# here and adjust example/k8s/daemonset.yaml's securityContext to match.
FROM gcr.io/distroless/static-debian12 AS runtime
COPY --from=builder /out/k8s-device-plugin-example /usr/local/bin/k8s-device-plugin-example
ENTRYPOINT ["/usr/local/bin/k8s-device-plugin-example"]
