FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go files
COPY go.mod go.su[m] ./
RUN go mod download

# Copy proto files
COPY proto/ ./proto/

# Copy source
COPY *.go ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /placement-service -ldflags="-w -s" .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /placement-service .
EXPOSE 50052
CMD ["./placement-service"]
