# Orchestrator peer agent and client. Run from peer/.
#
#   make build             build ./bin/orchestrator
#   make test              unit tests
#   make integration       full network test (needs TEST_DATABASE_URL, see README)
#   make install           copy the binary to ~/.local/bin
#   make install-service   install and start the systemd user service
VERSION ?= 0.1.0
BIN     := bin/orchestrator

.PHONY: build test integration install install-service

build:
	go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN) ./cmd/orchestrator

test:
	go test ./internal/...

integration:
	go test ./test -v -count=1 -timeout 10m

install: build
	install -D -m 0755 $(BIN) $(HOME)/.local/bin/orchestrator

install-service: install
	install -D -m 0644 packaging/orchestrator-agent.service $(HOME)/.config/systemd/user/orchestrator-agent.service
	systemctl --user daemon-reload
	systemctl --user enable --now orchestrator-agent
	@echo "To keep the agent running while you are logged out: sudo loginctl enable-linger $$USER"
