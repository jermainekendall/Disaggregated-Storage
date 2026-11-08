 FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /api-gateway -ldflags="-w -s" .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /api-gateway /api-gateway

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD nc -z localhost 8080 || exit 1

CMD ["/api-gateway"]
