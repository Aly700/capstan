package lab

import (
	"context"
	"testing"
)

func TestProtocolProbes(t *testing.T) {
	for seed := int64(0); seed < 10; seed++ {
		t.Run(protocolProbeNames[seed], func(t *testing.T) {
			if err := runProtocolProbes(context.Background(), seed); err != nil {
				t.Fatal(err)
			}
		})
	}
}
