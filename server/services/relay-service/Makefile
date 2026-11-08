# Makefile for libp2p Relay Service

.PHONY: help build push deploy clean logs test

VERSION ?= v1.0.0
IMAGE_NAME = libp2p-relay
REGISTRY = localhost:5000
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(VERSION)
NAMESPACE = orchestrator

help:
	@echo "libp2p Relay Service - Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make build          - Build Docker image"
	@echo "  make push           - Push to local registry"
	@echo "  make deploy         - Deploy to Kubernetes"
	@echo "  make all            - Build, push, and deploy"
	@echo "  make logs           - Tail relay logs"
	@echo "  make status         - Show deployment status"
	@echo "  make scale N=<num>  - Scale to N replicas"
	@echo "  make clean          - Remove deployment"
	@echo "  make test           - Test relay connectivity"
	@echo ""

build:
	@echo "Building Docker image..."
	docker build -t $(IMAGE_NAME):$(VERSION) .
	@echo "✓ Built: $(IMAGE_NAME):$(VERSION)"

push: build
	@echo "Tagging and pushing to registry..."
	docker tag $(IMAGE_NAME):$(VERSION) $(FULL_IMAGE)
	docker push $(FULL_IMAGE)
	@echo "✓ Pushed: $(FULL_IMAGE)"

deploy: push
	@echo "Deploying to Kubernetes..."
	kubectl apply -f relay-deployment.yaml
	@echo "Waiting for rollout..."
	kubectl rollout status deployment/libp2p-relay -n $(NAMESPACE) --timeout=120s
	@echo "✓ Deployed successfully"

all: deploy
	@echo "✓ Complete deployment finished"
	@$(MAKE) status

logs:
	@echo "Tailing relay logs (Ctrl+C to exit)..."
	kubectl logs -n $(NAMESPACE) -l app=libp2p-relay -f --tail=50

status:
	@echo "=== Relay Status ==="
	@echo ""
	@echo "Pods:"
	@kubectl get pods -n $(NAMESPACE) -l app=libp2p-relay
	@echo ""
	@echo "Service:"
	@kubectl get svc -n $(NAMESPACE) libp2p-relay
	@echo ""
	@echo "HPA:"
	@kubectl get hpa -n $(NAMESPACE) libp2p-relay-hpa 2>/dev/null || echo "  Not found"
	@echo ""

scale:
	@if [ -z "$(N)" ]; then \
		echo "Error: Must specify N=<number>"; \
		echo "Example: make scale N=3"; \
		exit 1; \
	fi
	@echo "Scaling to $(N) replicas..."
	kubectl scale deployment/libp2p-relay --replicas=$(N) -n $(NAMESPACE)
	@echo "✓ Scaled to $(N) replicas"

clean:
	@echo "Removing relay deployment..."
	kubectl delete -f relay-deployment.yaml --ignore-not-found
	@echo "✓ Removed"

test:
	@echo "Testing relay connectivity..."
	@NODE_IP=$$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[0].address}'); \
	echo "Testing connection to $$NODE_IP:30001..."; \
	nc -zv $$NODE_IP 30001 && echo "✓ Relay is accessible" || echo "✗ Cannot reach relay"

peer-id:
	@echo "Getting relay peer ID..."
	@POD=$$(kubectl get pods -n $(NAMESPACE) -l app=libp2p-relay -o jsonpath='{.items[0].metadata.name}'); \
	kubectl logs -n $(NAMESPACE) $$POD | grep "Peer ID:" | head -1 || echo "Not found - relay may still be starting"

info: peer-id
	@echo ""
	@NODE_IP=$$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[0].address}'); \
	PEER_ID=$$(kubectl logs -n $(NAMESPACE) -l app=libp2p-relay | grep "Peer ID:" | head -1 | awk '{print $$NF}'); \
	if [ -n "$$PEER_ID" ]; then \
		echo "Relay Connection Info:"; \
		echo "  External: /ip4/$$NODE_IP/tcp/30001/p2p/$$PEER_ID"; \
		echo "  Internal: /dns/libp2p-relay-internal.$(NAMESPACE).svc.cluster.local/tcp/4001/p2p/$$PEER_ID"; \
	fi
