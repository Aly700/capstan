//go:build pgengine

package engine

import (
	"testing"

	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
	"github.com/Aly700/capstan/internal/testpg"
)

func openTestStore(t *testing.T) store.Store {
	t.Helper()
	s, err := pgstore.Open(t.Context(), testpg.New(t))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
