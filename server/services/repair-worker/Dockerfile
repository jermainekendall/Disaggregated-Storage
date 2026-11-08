
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /repair-worker -ldflags="-w -s" .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /repair-worker /repair-worker
CMD ["/repair-worker"]
