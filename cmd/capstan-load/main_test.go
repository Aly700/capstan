package main

import (
	"testing"
	"time"
)

func TestSummaryCountsFailuresAndNearestRankLatency(t *testing.T) {
	rows := make([]sample, 100)
	for i := range rows {
		rows[i] = sample{LatencyMS: float64(i + 1), Activities: 5}
	}
	rows = append(rows, sample{LatencyMS: 9000, Error: "failed", Activities: 2})
	got := summarize(rows, 10*time.Second, 50)
	if got.Completed != 100 || got.Errors != 1 || got.Activities != 502 || got.RunsPerSecond != 10 || got.ActivitiesPerSecond != 50.2 || got.P50MS != 50 || got.P99MS != 99 {
		t.Fatalf("bad summary: %+v", got)
	}
	got = summarize([]sample{{Error: "unavailable"}}, time.Second, 1)
	if got.Completed != 0 || got.Errors != 1 || got.P50MS != 0 || got.P99MS != 0 {
		t.Fatalf("no-success summary invented percentiles: %+v", got)
	}
}
