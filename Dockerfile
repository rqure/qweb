# Build stage
FROM rust:1.90 as builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml ./

# Create a dummy main.rs to cache dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy source code
COPY src ./src

# Build the application
RUN touch src/main.rs && \
    cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/qweb /app/qweb

# Set default environment variables
ENV QCORE_ADDRESS=127.0.0.1:8080
ENV BIND_ADDRESS=0.0.0.0:3000
ENV RUST_LOG=info

EXPOSE 3000

CMD ["/app/qweb"]
