// Command orchestrator is the peer agent and client for the Orchestrator
// storage network.
//
//	orchestrator setup                 interactive configuration (storage, server)
//	orchestrator run                   run the storage agent (foreground; see packaging/ for systemd)
//	orchestrator status                local agent and storage status
//	orchestrator login [--register]    log in (or create an account)
//	orchestrator logout
//	orchestrator upload <file>         encrypt, erasure code and store a file
//	orchestrator ls                    list your files
//	orchestrator download <id> [-o path] [--force]
//	orchestrator rm <id>
//	orchestrator version
//
// Global flags (before the command): --config <path>, --gateway <url>,
// --registry <host:port>. Flags override the config file.
//
// For scripts: ORCHESTRATOR_PASSWORD and ORCHESTRATOR_PASSPHRASE replace the
// interactive prompts.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"golang.org/x/term"

	"github.com/jermainekendall/disaggregated-storage/peer/internal/agent"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/client"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/config"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/crypt"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/identity"
	"github.com/jermainekendall/disaggregated-storage/peer/internal/shardstore"
)

var version = "0.1.0"

type globals struct {
	configPath string
	gateway    string
	registry   string
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime)
	g := globals{}
	fs := flag.NewFlagSet("orchestrator", flag.ExitOnError)
	fs.StringVar(&g.configPath, "config", config.DefaultPath(), "config file")
	fs.StringVar(&g.gateway, "gateway", "", "API gateway URL (overrides the config)")
	fs.StringVar(&g.registry, "registry", "", "peer registry host:port (overrides the config)")
	// Old flag names from the previous client, kept as aliases.
	peerMode := fs.Bool("peer", false, "same as `run`")
	setupMode := fs.Bool("setup", false, "same as `setup`")
	statusMode := fs.Bool("status", false, "same as `status`")
	versionMode := fs.Bool("version", false, "same as `version`")
	fs.Usage = usage
	fs.Parse(os.Args[1:])

	args := fs.Args()
	cmd := ""
	switch {
	case *peerMode:
		cmd = "run"
	case *setupMode:
		cmd = "setup"
	case *statusMode:
		cmd = "status"
	case *versionMode:
		cmd = "version"
	case len(args) > 0:
		cmd, args = args[0], args[1:]
	default:
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch cmd {
	case "setup":
		err = cmdSetup(g, args)
	case "run":
		err = withConfig(g, func(c *config.Config) error { return agent.Run(ctx, c, version) })
	case "status":
		err = withConfig(g, cmdStatus)
	case "login":
		err = cmdLogin(ctx, g, args)
	case "logout":
		err = withConfig(g, client.Logout)
	case "upload":
		err = cmdUpload(ctx, g, args)
	case "ls", "list":
		err = cmdList(ctx, g)
	case "download", "get":
		err = cmdDownload(ctx, g, args)
	case "rm", "delete":
		err = cmdDelete(ctx, g, args)
	case "version":
		fmt.Println("orchestrator", version)
	case "help", "-h", "--help":
		usage()
	default:
		err = fmt.Errorf("unknown command %q (see `orchestrator help`)", cmd)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `orchestrator: peer agent and client for the Orchestrator storage network

Usage: orchestrator [--config path] [--gateway url] [--registry host:port] <command> [args]

Agent:
  setup                       configure storage and server (interactive)
  run                         run the storage agent in the foreground
  status                      show local agent and storage status

Files:
  login [--register] [--email you@example.com]
  logout
  upload <file> [--tier cold|warm|hot]
  ls
  download <file-id> [-o path] [--force]
  rm <file-id>

  version
`)
}

func loadConfig(g globals) (*config.Config, error) {
	c, err := config.Load(g.configPath)
	if c == nil {
		return nil, err
	}
	if g.gateway != "" || g.registry != "" {
		if g.gateway != "" {
			c.GatewayURL = g.gateway
		}
		if g.registry != "" {
			c.RegistryAddr = g.registry
		}
		err = c.Validate()
	}
	return c, err
}

func withConfig(g globals, fn func(*config.Config) error) error {
	c, err := loadConfig(g)
	if err != nil {
		return err
	}
	return fn(c)
}

// ---------------------------------------------------------------- prompts

var stdin = bufio.NewReader(os.Stdin)

func ask(prompt, def string) string {
	if def != "" {
		fmt.Printf("%s [%s]: ", prompt, def)
	} else {
		fmt.Printf("%s: ", prompt)
	}
	line, _ := stdin.ReadString('\n')
	if line = strings.TrimSpace(line); line == "" {
		return def
	}
	return line
}

func secret(prompt string) (string, error) {
	fmt.Fprint(os.Stderr, prompt)
	if term.IsTerminal(int(os.Stdin.Fd())) {
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		return string(b), err
	}
	line, err := stdin.ReadString('\n')
	fmt.Fprintln(os.Stderr)
	return strings.TrimRight(line, "\r\n"), err
}

func passphrase(confirm bool) (string, error) {
	if p := os.Getenv("ORCHESTRATOR_PASSPHRASE"); p != "" {
		return p, nil
	}
	if confirm {
		fmt.Fprintln(os.Stderr, "Choose an encryption passphrase. It encrypts every file you upload and is")
		fmt.Fprintln(os.Stderr, "never sent to the server: if you lose it, your files cannot be recovered.")
	}
	p, err := secret("Encryption passphrase: ")
	if err != nil {
		return "", err
	}
	if confirm {
		if len(p) < crypt.MinPassphraseLength {
			return "", fmt.Errorf("passphrase must be at least %d characters", crypt.MinPassphraseLength)
		}
		again, err := secret("Repeat passphrase: ")
		if err != nil {
			return "", err
		}
		if again != p {
			return "", errors.New("passphrases do not match")
		}
	}
	return p, nil
}

// ---------------------------------------------------------------- setup

func cmdSetup(g globals, args []string) error {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	nonInteractive := fs.Bool("non-interactive", false, "use flags and defaults without prompting")
	gateway := fs.String("gateway", g.gateway, "API gateway URL, e.g. http://192.168.1.201:30080")
	registry := fs.String("registry", g.registry, "peer registry, e.g. 192.168.1.201:30051")
	relay := fs.String("relay", "", "relay multiaddr, /ip4/.../tcp/30001/p2p/<id>")
	storageDir := fs.String("storage-dir", "", "directory for shards stored for others")
	storageGB := fs.Float64("storage-gb", 0, "space to offer, in GB")
	port := fs.Int("port", -1, "libp2p listen port (TCP and UDP)")
	region := fs.String("region", "", "region label, e.g. gy-georgetown")
	asn := fs.String("asn", "", "your ISP's AS number, e.g. AS20057 (optional)")
	loopback := fs.Bool("loopback", false, "advertise 127.0.0.1 (several agents on one machine)")
	fs.Parse(args)

	c := config.Defaults()
	if old, err := config.Load(g.configPath); old != nil {
		c = *old // re-running setup starts from the current values
	} else if err != nil && !strings.Contains(err.Error(), "no config") {
		fmt.Println("note: existing config is invalid and will be replaced:", err)
	}
	set := func(dst *string, v string) {
		if v != "" {
			*dst = v
		}
	}
	set(&c.GatewayURL, *gateway)
	set(&c.RegistryAddr, *registry)
	set(&c.RelayAddr, *relay)
	set(&c.StorageDir, *storageDir)
	set(&c.Region, *region)
	set(&c.ASN, *asn)
	if *storageGB > 0 {
		c.StorageGB = *storageGB
	}
	if *port >= 0 {
		c.ListenPort = *port
	}
	if *loopback {
		c.AnnounceLoopback = true
	}

	if !*nonInteractive {
		fmt.Println("Orchestrator setup. Press Enter to keep the value in brackets.")
		fmt.Println()
		c.GatewayURL = ask("API gateway URL", orDefault(c.GatewayURL, "http://192.168.1.201:30080"))
		host := hostOf(c.GatewayURL)
		c.RegistryAddr = ask("Peer registry (gRPC)", orDefault(c.RegistryAddr, host+":30051"))
		c.RelayAddr = ask("Relay address (printed at the end of server install; empty for none)", c.RelayAddr)
		fmt.Println()
		fmt.Println("Storage for other people's shards:")
		fmt.Println("  1) Use a directory on an existing filesystem")
		fmt.Println("  2) Create a dedicated partition (not automated: create and mount it yourself, then choose 1)")
		if ask("Choice", "1") == "2" {
			return errors.New("partition creation is not automated; mount the partition and run setup again with option 1")
		}
		c.StorageDir = ask("Storage directory", c.StorageDir)
		gbStr := ask("Space to offer (GB)", strconv.FormatFloat(c.StorageGB, 'f', -1, 64))
		if v, err := strconv.ParseFloat(gbStr, 64); err == nil {
			c.StorageGB = v
		}
		portStr := ask("libp2p port (TCP+UDP; forward it on your router for best results)", strconv.Itoa(c.ListenPort))
		if v, err := strconv.Atoi(portStr); err == nil {
			c.ListenPort = v
		}
		c.Region = ask("Region label (optional)", c.Region)
		c.ASN = ask("ISP AS number (optional)", c.ASN)
	}
	if err := c.Validate(); err != nil {
		return err
	}

	dir := c.StorageDir
	if strings.HasPrefix(dir, "~/") {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, dir[2:])
	}
	dir, _ = filepath.Abs(dir)
	c.StorageDir = dir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := shardstore.CheckPermissions(dir); err != nil {
		if !*nonInteractive && strings.HasPrefix(strings.ToLower(ask("Storage directory is readable by others. Fix with chmod 700? (y/n)", "y")), "y") {
			if err := os.Chmod(dir, 0o700); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if err := c.Save(g.configPath); err != nil {
		return err
	}
	if _, err := identity.LoadOrCreateKey(c.Resolve(c.KeyPath)); err != nil {
		return err
	}
	fmt.Printf("\nSaved %s\n", g.configPath)
	fmt.Println("Next: `orchestrator run` to start the agent (or install the systemd unit, see README),")
	fmt.Println("then `orchestrator login --register` to create an account and upload files.")
	return nil
}

func orDefault(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

func hostOf(u string) string {
	u = strings.TrimPrefix(strings.TrimPrefix(u, "http://"), "https://")
	if i := strings.IndexAny(u, ":/"); i >= 0 {
		u = u[:i]
	}
	return u
}

// ---------------------------------------------------------------- status

func cmdStatus(c *config.Config) error {
	var st agent.State
	identity.ReadJSON(agent.StatePath(c), &st)
	fmt.Printf("Config:        %s\n", filepath.Join(c.Dir(), "config.yaml"))
	fmt.Printf("Gateway:       %s\n", c.GatewayURL)
	fmt.Printf("Server peer:   %s\n", orDefault(st.PeerID, "(not registered yet; run the agent)"))
	key, err := identity.LoadOrCreateKey(c.Resolve(c.KeyPath))
	if err == nil {
		if id, err := peerIDOf(key); err == nil {
			fmt.Printf("libp2p ID:     %s\n", id)
		}
	}
	store, err := shardstore.Open(c.StoragePath(), c.CapacityBytes())
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	n, used, capacity := store.Stats()
	fmt.Printf("Storage:       %s\n", c.StoragePath())
	fmt.Printf("Shards held:   %d (%.2f GB of %.2f GB offered, %.2f GB available)\n",
		n, gb(used), gb(capacity), gb(store.Available()))
	return nil
}

func gb(b int64) float64 { return float64(b) / (1 << 30) }

// ---------------------------------------------------------------- files

func cmdLogin(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	register := fs.Bool("register", false, "create a new account")
	email := fs.String("email", "", "account email")
	fs.Parse(args)
	c, err := loadConfig(g)
	if err != nil {
		return err
	}
	if *email == "" {
		*email = os.Getenv("ORCHESTRATOR_EMAIL")
	}
	if *email == "" {
		*email = ask("Email", "")
	}
	pw := os.Getenv("ORCHESTRATOR_PASSWORD")
	if pw == "" {
		if pw, err = secret("Account password: "); err != nil {
			return err
		}
	}
	s, err := client.Login(ctx, c, *email, pw, *register)
	if err != nil {
		return err
	}
	fmt.Printf("Logged in as %s (session valid until %s)\n", s.Email, time.Unix(s.ExpiresAt, 0).Format(time.Kitchen))
	return nil
}

func openClient(ctx context.Context, g globals) (*client.Client, error) {
	c, err := loadConfig(g)
	if err != nil {
		return nil, err
	}
	cl, err := client.Open(ctx, c, passphrase)
	if err != nil {
		return nil, err
	}
	cl.Log = func(f string, a ...interface{}) { fmt.Printf(f+"\n", a...) }
	return cl, nil
}

func cmdUpload(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	tier := fs.String("tier", "cold", "storage tier: cold, warm or hot")
	fs.Parse(reorder(args))
	if fs.NArg() != 1 {
		return errors.New("usage: orchestrator upload <file> [--tier cold|warm|hot]")
	}
	cl, err := openClient(ctx, g)
	if err != nil {
		return err
	}
	defer cl.Close()
	start := time.Now()
	r, err := cl.Upload(ctx, fs.Arg(0), *tier)
	if err != nil {
		return err
	}
	fmt.Printf("Uploaded %s as %s (%d bytes, %d stored, %d+%d shards) in %s\n",
		r.Name, r.FileID, r.PlainBytes, r.StoredBytes, r.Data, r.Parity, time.Since(start).Round(time.Millisecond))
	return nil
}

func cmdList(ctx context.Context, g globals) error {
	cl, err := openClient(ctx, g)
	if err != nil {
		return err
	}
	defer cl.Close()
	files, err := cl.List(ctx)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		fmt.Println("No files yet. Upload one with `orchestrator upload <file>`.")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "FILE ID\tNAME\tSTORED\tSTATUS\tSHARDS\tTIER\tUPLOADED")
	for _, f := range files {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d/%d\t%s\t%s\n", f.FileID, f.Name, human(f.FileSizeBytes), f.Status,
			f.AvailableShards, f.DataShards+f.ParityShards, f.StorageTier, f.CreatedAt.Local().Format("2006-01-02 15:04"))
	}
	return tw.Flush()
}

func cmdDownload(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	out := fs.String("o", "", "output file or directory (default: original name in the current directory)")
	force := fs.Bool("force", false, "overwrite an existing file")
	fs.Parse(reorder(args))
	if fs.NArg() != 1 {
		return errors.New("usage: orchestrator download <file-id> [-o path] [--force]")
	}
	cl, err := openClient(ctx, g)
	if err != nil {
		return err
	}
	defer cl.Close()
	start := time.Now()
	p, err := cl.Download(ctx, fs.Arg(0), *out, *force)
	if err != nil {
		return err
	}
	fmt.Printf("Saved %s in %s\n", p, time.Since(start).Round(time.Millisecond))
	return nil
}

func cmdDelete(ctx context.Context, g globals, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: orchestrator rm <file-id>")
	}
	cl, err := openClient(ctx, g)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := cl.Delete(ctx, args[0]); err != nil {
		return err
	}
	fmt.Println("Deleted", args[0], "(peers remove their shards on their next poll)")
	return nil
}

// reorder moves flags in front of positional arguments, so both
// `upload file --tier hot` and `upload --tier hot file` work.
func reorder(args []string) []string {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") && a != "-" {
			flags = append(flags, a)
			if !strings.Contains(a, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") && takesValue(a) {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		pos = append(pos, a)
	}
	return append(flags, pos...)
}

func takesValue(f string) bool {
	switch strings.TrimLeft(f, "-") {
	case "tier", "o":
		return true
	}
	return false
}

func human(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
