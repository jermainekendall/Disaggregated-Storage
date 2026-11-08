FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /replication-worker -ldflags="-w -s" .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /replication-worker /replication-worker
CMD ["/replication-worker"]
