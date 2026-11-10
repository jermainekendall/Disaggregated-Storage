package sysinfo

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, path, content string) {
	os.MkdirAll(filepath.Dir(path), 0o755)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPowerLaptopOnBattery(t *testing.T) {
	root := t.TempDir()
	write(t, root+"/AC/type", "Mains\n")
	write(t, root+"/AC/online", "0\n")
	write(t, root+"/BAT0/type", "Battery\n")
	write(t, root+"/BAT0/capacity", "57\n")
	write(t, root+"/BAT0/status", "Discharging\n")
	write(t, root+"/hidpp_battery_0/type", "Battery\n") // mouse
	write(t, root+"/hidpp_battery_0/scope", "Device\n")
	write(t, root+"/hidpp_battery_0/capacity", "5\n")
	if p := readPower(root); p.OnAC || p.BatteryLevel != 57 {
		t.Fatalf("%+v", p)
	}
}

func TestPowerDesktop(t *testing.T) {
	if p := readPower(t.TempDir()); !p.OnAC || p.BatteryLevel != 0 {
		t.Fatalf("%+v", p)
	}
}

func TestNetworkType(t *testing.T) {
	dir := t.TempDir()
	write(t, dir+"/route", "Iface\tDestination\tGateway\nwlp2s0\t0000A8C0\t00000000\nwlp2s0\t00000000\t0101A8C0\n")
	write(t, dir+"/net/wlp2s0/wireless/x", "")
	if got := networkType(dir+"/route", dir+"/net"); got != "wifi" {
		t.Fatalf("got %s", got)
	}
	write(t, dir+"/route2", "Iface\tDestination\tGateway\nenp3s0\t00000000\t0101A8C0\n")
	if got := networkType(dir+"/route2", dir+"/net"); got != "ethernet" {
		t.Fatalf("got %s", got)
	}
}
