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
		{"positive_half", .0000005, .000001},
		{"large_finite", math.MaxFloat64, math.MaxFloat64},
		{"large_negative_finite", -math.MaxFloat64, -math.MaxFloat64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := roundUSD(tc.amount); got != tc.want {
				t.Fatalf("roundUSD(%.18g) = %.18g, want %.18g", tc.amount, got, tc.want)
			}
		})
	}
}
