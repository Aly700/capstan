package engine

import (
	"math"
	"testing"
)

func TestRoundUSD(t *testing.T) {
	for _, tc := range []struct {
		name   string
		amount float64
		want   float64
	}{
		{"zero", 0, 0},
		{"below_half", .0000004, 0},
		{"positive_half", .0000005, .000001},
		{"negative_half", -.0000005, -.000001},
		{"positive_tie", 1.2345675, 1.234568},
		{"negative_tie", -1.2345675, -1.234568},
		{"already_rounded", 1.234567, 1.234567},
		{"large_finite", math.MaxFloat64, math.MaxFloat64},
		{"large_negative_finite", -math.MaxFloat64, -math.MaxFloat64},
		{"whole_dollar_boundary", 1 << 52, 1 << 52},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := roundUSD(tc.amount); got != tc.want {
				t.Fatalf("roundUSD(%.18g) = %.18g, want %.18g", tc.amount, got, tc.want)
			}
		})
	}
}
