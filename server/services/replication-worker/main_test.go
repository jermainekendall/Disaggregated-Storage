package main

import "testing"

func TestTargetCopies(t *testing.T) {
	cases := []struct {
		tier              string
		data, parity, max int
		want              int
	}{
		{"cold", 10, 4, 28, 0},
		{"warm", 10, 4, 28, 5},
		{"warm", 9, 4, 28, 5},
		{"hot", 10, 4, 28, 14},
		{"hot", 26, 4, 28, 0},  // already at the cap
		{"warm", 20, 4, 28, 4}, // limited by room under the cap
	}
	for _, c := range cases {
		if got := targetCopies(c.tier, c.data, c.parity, c.max); got != c.want {
			t.Errorf("targetCopies(%s, %d, %d, %d) = %d, want %d", c.tier, c.data, c.parity, c.max, got, c.want)
		}
	}
}
