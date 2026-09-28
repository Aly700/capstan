// Package storetest is the executable persistence contract shared by every store.
// Run opens a fresh store for each case. It only uses store.Store and store.Tx;
// database setup and cleanup belong to the implementation's test harness.
package storetest
