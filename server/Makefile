# Orchestrator server build helpers. Run from the server/ directory.
#
#   make proto   regenerate gRPC stubs (needs protoc, protoc-gen-go, protoc-gen-go-grpc;
#                `pip install grpcio-tools` provides protoc as `python3 -m grpc_tools.protoc`)
#   make build   compile every service
#   make test    unit tests; set TEST_DATABASE_URL to also run the database tests
#   make images  build all Docker images as orchestrator/<service>:dev

SERVICES := api-gateway peer-registry placement-service audit-service billing-service \
            key-metadata repair-worker replication-worker analytics-worker relay-service
PROTOC   ?= $(shell command -v protoc 2>/dev/null || echo "python3 -m grpc_tools.protoc")
TAG      ?= dev

.PHONY: proto build test vet images

proto:
	$(PROTOC) -I proto \
	  --go_out=proto --go_opt=paths=source_relative \
	  --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
	  proto/peer/peer.proto proto/placement/placement.proto proto/audit/audit.proto \
	  proto/billing/billing.proto proto/keymeta/keymeta.proto

build:
	go build ./...

vet:
	go vet ./...

test:
	go test ./...

images:
	for s in $(SERVICES); do \
	  docker build --build-arg SERVICE=$$s -t orchestrator/$$s:$(TAG) . || exit 1; \
	done
