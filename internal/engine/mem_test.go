//go:build !pgengine

package engine

import (
	"testing"

	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/memstore"
)

func openTestStore(t *testing.T) store.Store {
	t.Helper()
	return memstore.New()
}
