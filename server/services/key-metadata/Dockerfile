FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /key-metadata -ldflags="-w -s" .

FROM alpine:latest
RUN apk --no-cache add ca-certificates netcat-openbsd
COPY --from=builder /key-metadata /key-metadata
EXPOSE 50054
CMD ["/key-metadata"]
