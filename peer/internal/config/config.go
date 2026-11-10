// Package config loads the agent/client configuration.
//
// Default location: ~/.config/orchestrator/config.yaml. Every path in the
// file can be relative to the config directory. State written by the program
// (peer ID, login session, identity key) lives next to it.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/multiformats/go-multiaddr"
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Server
	GatewayURL   string `yaml:"gateway_url"`   // e.g. http://192.168.1.201:30080
	RegistryAddr string `yaml:"registry_addr"` // e.g. 192.168.1.201:30051
	RelayAddr    string `yaml:"relay_addr"`    // /ip4/.../tcp/30001/p2p/12D3...

	// Storage offered to the network
	StorageDir string  `yaml:"storage_dir"`
	StorageGB  float64 `yaml:"storage_gb"`

	// libp2p
	ListenPort       int      `yaml:"listen_port"`       // TCP and QUIC
	AnnounceAddrs    []string `yaml:"announce_addrs"`    // optional override of advertised addresses
	AnnounceLoopback bool     `yaml:"announce_loopback"` // advertise 127.0.0.1 (single-machine tests)
	KeyPath          string   `yaml:"key_path"`          // identity key file

	// The agent writes its server peer ID here (world-readable) so client
	// commands run by other users can avoid placing shards on it. The
	// system service sets /run/orchestrator/peer-id.
	PeerIDFile string `yaml:"peer_id_file,omitempty"`
	// Where client commands look for the local agent's peer ID when it is not
	// in this config directory (the system service's file by default).
	AgentPeerIDFile string `yaml:"agent_peer_id_file,omitempty"`

	// Location reported at registration (timezone is read from the system)
	Region string `yaml:"region"`
	ASN    string `yaml:"asn"`

	// Timing
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	PollInterval      time.Duration `yaml:"poll_interval"`

	dir string `yaml:"-"`
}

// DefaultPath returns ~/.config/orchestrator/config.yaml.
func DefaultPath() string {
	base, err := os.UserConfigDir()
	if err != nil {
		base = filepath.Join(os.Getenv("HOME"), ".config")
	}
	return filepath.Join(base, "orchestrator", "config.yaml")
}

// Defaults returns a config with every optional field filled in.
func Defaults() Config {
	home, _ := os.UserHomeDir()
	return Config{
		StorageDir:        filepath.Join(home, ".local", "share", "orchestrator", "storage"),
		StorageGB:         10,
		ListenPort:        4001,
		KeyPath:           "identity.key",
		AgentPeerIDFile:   "/run/orchestrator/peer-id",
		Region:            "",
		HeartbeatInterval: 5 * time.Minute, // the server marks peers offline after 15
		PollInterval:      30 * time.Second,
	}
}

// Load reads path and applies defaults for anything unset.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("no config at %s; run `orchestrator setup` first", path)
	}
	if err != nil {
		return nil, err
	}
	c := Defaults()
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	c.dir = filepath.Dir(path)
	return &c, c.Validate()
}

// Save writes the config with owner-only permissions.
func (c *Config) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	c.dir = filepath.Dir(path)
	return os.WriteFile(path, raw, 0o600)
}

// Validate checks every field and returns all problems at once.
func (c *Config) Validate() error {
	var errs []string
	if u, err := url.Parse(c.GatewayURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, "gateway_url must be like http://192.168.1.201:30080")
	}
	if !strings.Contains(c.RegistryAddr, ":") {
		errs = append(errs, "registry_addr must be host:port, e.g. 192.168.1.201:30051")
	}
	if c.RelayAddr != "" {
		if _, err := multiaddr.NewMultiaddr(c.RelayAddr); err != nil || !strings.Contains(c.RelayAddr, "/p2p/") {
			errs = append(errs, "relay_addr must be a multiaddr ending in /p2p/<relay peer id>")
		}
	}
	for _, a := range c.AnnounceAddrs {
		if _, err := multiaddr.NewMultiaddr(a); err != nil {
			errs = append(errs, fmt.Sprintf("announce_addrs: %q is not a multiaddr", a))
		}
	}
	if c.StorageDir == "" {
		errs = append(errs, "storage_dir is required")
	}
	if c.StorageGB <= 0 {
		errs = append(errs, "storage_gb must be positive")
	}
	if c.ListenPort < 0 || c.ListenPort > 65535 {
		errs = append(errs, "listen_port must be 0-65535 (0 picks a free port)")
	}
	if c.HeartbeatInterval < 10*time.Second || c.HeartbeatInterval > 14*time.Minute {
		errs = append(errs, "heartbeat_interval must be between 10s and 14m (the server times peers out after 15m)")
	}
	if c.PollInterval < time.Second {
		errs = append(errs, "poll_interval must be at least 1s")
	}
	if len(errs) > 0 {
		return errors.New("invalid config:\n  - " + strings.Join(errs, "\n  - "))
	}
	return nil
}

// Dir is the directory holding the config file and program state.
func (c *Config) Dir() string { return c.dir }

// Resolve makes p absolute relative to the config directory.
func (c *Config) Resolve(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	if strings.HasPrefix(p, "~/") {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, p[2:])
	}
	return filepath.Join(c.dir, p)
}

// StoragePath is the absolute storage directory.
func (c *Config) StoragePath() string { return c.Resolve(c.StorageDir) }

// CapacityBytes is storage_gb in bytes.
func (c *Config) CapacityBytes() int64 { return int64(c.StorageGB * (1 << 30)) }
