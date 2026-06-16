FROM rust:1.87-bookworm AS builder

RUN rustup target add x86_64-unknown-linux-musl && \
    apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN cargo build --release --target x86_64-unknown-linux-musl

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/sturnus /sturnus
# Give gcp_auth a home to find ADC creds at $HOME/.config/gcloud/ (scratch sets none).
ENV HOME=/root
ENTRYPOINT ["/sturnus"]
