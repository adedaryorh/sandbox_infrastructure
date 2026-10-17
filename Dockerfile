# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM golang:1.22-bookworm AS builder

WORKDIR /app

# copy dependency files first — layer cached unless go.mod/go.sum change
COPY go.mod go.sum ./
RUN go mod download

# copy source
COPY . .

# build a static binary with no CGO dependency
# -ldflags "-s -w" strips debug info to reduce binary size
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /bin/server ./cmd/server

# build CLI binary too
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /bin/sandbox ./cli/cmd

# ── Stage 2: Runtime ───────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# install CA certs so HTTPS calls (webhooks, Caddy API) work
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# copy binaries from builder
COPY --from=builder /bin/server  /usr/local/bin/server
COPY --from=builder /bin/sandbox /usr/local/bin/sandbox

# run as non-root
RUN useradd -r -u 1001 -g root sandboxing
USER 1001

EXPOSE 8080

# health check — Kubernetes uses this too
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["/usr/local/bin/server"]
