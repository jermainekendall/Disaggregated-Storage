 
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /audit-service -ldflags="-w -s" .

FROM alpine:latest
RUN apk --no-cache add ca-certificates netcat-openbsd
COPY --from=builder /audit-service /audit-service
EXPOSE 50053
CMD ["/audit-service"]
