// Package memstore is an in-memory store.Store with the same semantics as pgstore. It backs
// engine unit tests and the fault lab. Implemented by the engine lane; must pass the shared
// conformance suite in store/storetest.
package memstore

import "github.com/Aly700/capstan/internal/store"

// New returns an empty in-memory store.
func New() store.Store {
	panic("memstore: not implemented yet (engine lane)")
}
