package lab

import (
	"testing"

	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
)

func TestFaultStoreRunCloseNotification(t *testing.T) {
	for _, kind := range []string{"commit", "failure", "crash"} {
		t.Run(kind, func(t *testing.T) {
			base := memstore.New()
			defer base.Close()
			s := NewFaultStore(base)
			ch, cancel := s.SubscribeRun("r")
			defer cancel()
			if kind != "commit" {
				s.Arm(1, kind == "crash")
			}
			var caught any
			func() {
				defer func() { caught = recover() }()
				if err := s.InTx(t.Context(), func(tx store.Tx) error { tx.NotifyRunClosed("r"); return nil }); err != nil {
					t.Error(err)
				}
			}()
			if kind == "commit" {
				if caught != nil {
					t.Fatal(caught)
				}
				select {
				case <-ch:
				default:
					t.Fatal("committed close did not notify")
				}
				return
			}
			want := ErrInjected
			if kind == "crash" {
				want = ErrServerCrash
			}
			if caught != want || s.Hits() != 1 {
				t.Fatalf("panic=%v hits=%d", caught, s.Hits())
			}
			select {
			case <-ch:
				t.Fatal("rolled-back close notified")
			default:
			}
		})
	}
}
