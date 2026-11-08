# One Dockerfile for every service. Build from the server/ directory:
#   docker build --build-arg SERVICE=peer-registry -t orchestrator/peer-registry:dev .
FROM golang:1.24-alpine AS build
ARG SERVICE
RUN test -n "$SERVICE" || (echo "pass --build-arg SERVICE=<name>" && false)
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY proto ./proto
COPY services ./services
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./services/${SERVICE}

# Distroless: CA certificates and a non-root user (65532), no shell or
# package manager, so nothing to install at build time.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
