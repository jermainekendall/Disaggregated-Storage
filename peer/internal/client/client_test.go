package client

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jermainekendall/disaggregated-storage/peer/internal/config"
)

func testConfig(t *testing.T, agentFile string) *config.Config {
	dir := t.TempDir()
	c := config.Defaults()
	c.GatewayURL, c.RegistryAddr, c.AgentPeerIDFile = "http://127.0.0.1:1", "127.0.0.1:2", agentFile
	if err := c.Save(filepath.Join(dir, "config.yaml")); err != nil {
		t.Fatal(err)
	}
	return &c
}

func TestLocalAgentPeerID(t *testing.T) {
	// System service: the ID comes from the published file.
	pub := filepath.Join(t.TempDir(), "peer-id")
	os.WriteFile(pub, []byte("9f7eef8a-5eef-499d-a9b9-5e7c0887891b\n"), 0o644)
	c := &Client{cfg: testConfig(t, pub)}
	if got := c.localAgentPeerID(); got != "9f7eef8a-5eef-499d-a9b9-5e7c0887891b" {
		t.Fatalf("published file: got %q", got)
	}

	// Agent running as this user: state.json in the config directory wins.
	os.WriteFile(filepath.Join(c.cfg.Dir(), "state.json"), []byte(`{"peer_id":"11111111-1111-4111-8111-111111111111"}`), 0o600)
	if got := c.localAgentPeerID(); got != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("state.json: got %q", got)
	}

	// No local agent at all.
	c2 := &Client{cfg: testConfig(t, filepath.Join(t.TempDir(), "missing"))}
	if got := c2.localAgentPeerID(); got != "" {
		t.Fatalf("no agent: got %q", got)
	}
}
