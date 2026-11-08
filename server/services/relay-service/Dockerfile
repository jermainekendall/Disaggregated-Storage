# Multi-stage Dockerfile for libp2p Relay
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git gcc musl-dev

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY main.go ./

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o relay \
    main.go

# Runtime stage
FROM alpine:latest

# Add ca-certificates for TLS
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1000 relay && \
    adduser -D -u 1000 -G relay relay

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/relay /app/relay

# Change ownership
RUN chown -R relay:relay /app

# Switch to non-root user
USER relay

# Expose relay port
EXPOSE 4001

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD nc -z localhost 4001 || exit 1

# Run relay
ENTRYPOINT ["/app/relay"]
CMD ["--port=4001", "--max-reservations=512", "--max-circuits=256"]
