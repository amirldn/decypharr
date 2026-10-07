package utils

import (
	"math"
	"testing"
)

func TestParseRateValue(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"12/minute", 12},
		{"12/min", 12},
		{"12/mins", 12},
		{"10/second", 600},
		{"10/sec", 600},
		{"60/hour", 1},
		{"60/hr", 1},
		{"24/day", 24.0 / 1440},
		{"1/d", 1.0 / 1440},
		{" 7 / minute ", 7},
	}
	for _, tc := range cases {
		got, ok := ParseRateValue(tc.in)
		if !ok {
			t.Errorf("ParseRateValue(%q) rejected", tc.in)
			continue
		}
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("ParseRateValue(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}

	for _, bad := range []string{"", "abc", "12", "12/", "/minute", "0/minute", "-3/min", "12/fortnight", "x/minute"} {
		if _, ok := ParseRateValue(bad); ok {
			t.Errorf("ParseRateValue(%q) accepted an invalid rate", bad)
		}
	}
}

func TestParseRateLimitUsesSharedSyntax(t *testing.T) {
	for _, valid := range []string{"250/minute", "10/second", "60/hour", "24/day"} {
		if ParseRateLimit(valid) == nil {
			t.Errorf("ParseRateLimit(%q) = nil, want limiter", valid)
		}
		if _, ok := ParseRateValue(valid); !ok {
			t.Errorf("ParseRateValue(%q) rejected a rate ParseRateLimit accepts", valid)
		}
	}
	for _, invalid := range []string{"", "250", "0/minute", "-1/second", "250/fortnight"} {
		if ParseRateLimit(invalid) != nil {
			t.Errorf("ParseRateLimit(%q) = limiter, want nil", invalid)
		}
		if _, ok := ParseRateValue(invalid); ok {
			t.Errorf("ParseRateValue(%q) accepted a rate ParseRateLimit rejects", invalid)
		}
	}
}
