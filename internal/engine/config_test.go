package engine

import (
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"math"
	"testing"
	"time"
)

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	s := newTestStore(t)
	for _, cfg := range []Config{{DefaultTaskTimeout: -time.Second}, {DefaultTaskTimeout: 11 * time.Minute}, {DefaultTaskTimeout: 1500 * time.Microsecond}, {TaskRetryInitial: -time.Second}, {TaskRetryMax: -time.Second}, {MaxHistoryEvents: -1}, {DailyCapUSD: -1}, {DailyCapUSD: math.NaN()}, {DailyCapUSD: math.Inf(1)}, {GatePollInitial: -time.Second}, {GatePollMax: -time.Second}, {DefaultRetry: &v1.RetryPolicy{BackoffCoefficient: .5}}, {ModelPrices: map[string]ModelPrice{"bad": {Input: math.NaN()}}}, {ModelPrices: map[string]ModelPrice{"bad": {Output: -1}}}} {
		if _, err := New(Deps{Store: s}, cfg); err == nil {
			t.Errorf("accepted invalid config %+v", cfg)
		}
	}
}
func TestNewCopiesConfiguration(t *testing.T) {
	s := newTestStore(t)
	prices := map[string]ModelPrice{"custom": {Input: 2}}
	retry := &v1.RetryPolicy{InitialInterval: durationpb.New(time.Second), NonRetryableErrorTypes: []string{"Fatal"}}
	e, err := New(Deps{Store: s}, Config{ModelPrices: prices, DefaultRetry: retry})
	if err != nil {
		t.Fatal(err)
	}
	prices["custom"] = ModelPrice{Input: 99}
	retry.InitialInterval.Seconds = 99
	retry.NonRetryableErrorTypes[0] = "Changed"
	if e.cfg.ModelPrices["custom"].Input != 2 || e.cfg.DefaultRetry.InitialInterval.Seconds != 1 || e.cfg.DefaultRetry.NonRetryableErrorTypes[0] != "Fatal" {
		t.Fatal("constructor retained mutable caller configuration")
	}
}
