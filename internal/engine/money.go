package engine

import "math"

// roundUSD rounds to the ledger's micro-dollar precision, half away from zero.
// Callers validate raw amounts before rounding so invalid values cannot become zero.
func roundUSD(amount float64) float64 {
	// Float64 values this large are already whole dollars; avoid scaling overflow.
	if math.Abs(amount) >= 1<<52 {
		return amount
	}
	return math.Round(amount*1_000_000) / 1_000_000
}
