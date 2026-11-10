// Package sysinfo reads what the heartbeat reports about this machine on
// Ubuntu: power state, network type and timezone.
package sysinfo

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Power describes the machine's power supply.
type Power struct {
	OnAC         bool
	BatteryLevel int // 1-100, or 0 when there is no battery
}

// ReadPower inspects /sys/class/power_supply. A desktop without a battery is
// reported as on AC power with no battery level.
func ReadPower() Power {
	return readPower("/sys/class/power_supply")
}

func readPower(root string) Power {
	p := Power{}
	sawMains := false
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		dir := filepath.Join(root, e.Name())
		switch strings.TrimSpace(read(filepath.Join(dir, "type"))) {
		case "Mains":
			sawMains = true
			if strings.TrimSpace(read(filepath.Join(dir, "online"))) == "1" {
				p.OnAC = true
			}
		case "Battery":
			if strings.TrimSpace(read(filepath.Join(dir, "scope"))) == "Device" {
				continue // e.g. a wireless mouse
			}
			if n, err := strconv.Atoi(strings.TrimSpace(read(filepath.Join(dir, "capacity")))); err == nil && n > 0 && n <= 100 {
				p.BatteryLevel = n
			}
			if st := strings.TrimSpace(read(filepath.Join(dir, "status"))); st == "Charging" || st == "Full" {
				p.OnAC = true
			}
		}
	}
	if !sawMains && p.BatteryLevel == 0 {
		p.OnAC = true // no power supply info at all: a desktop or VM
	}
	return p
}

// NetworkType returns "wifi" or "ethernet" for the interface carrying the
// default route, or "unknown".
func NetworkType() string {
	return networkType("/proc/net/route", "/sys/class/net")
}

func networkType(routeFile, netDir string) string {
	iface := ""
	for _, line := range strings.Split(read(routeFile), "\n")[1:] {
		f := strings.Fields(line)
		if len(f) > 2 && f[1] == "00000000" { // destination 0.0.0.0
			iface = f[0]
			break
		}
	}
	if iface == "" {
		return "unknown"
	}
	if _, err := os.Stat(filepath.Join(netDir, iface, "wireless")); err == nil {
		return "wifi"
	}
	return "ethernet"
}

// Timezone returns the IANA name of the local timezone, or "" if unknown.
func Timezone() string {
	if tz := os.Getenv("TZ"); tz != "" {
		if _, err := time.LoadLocation(tz); err == nil {
			return tz
		}
	}
	if tz := strings.TrimSpace(read("/etc/timezone")); tz != "" {
		return tz
	}
	if target, err := os.Readlink("/etc/localtime"); err == nil {
		if i := strings.Index(target, "zoneinfo/"); i >= 0 {
			return target[i+len("zoneinfo/"):]
		}
	}
	return ""
}

func read(p string) string {
	b, _ := os.ReadFile(p)
	return string(b)
}
