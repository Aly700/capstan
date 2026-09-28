// Package auth handles named, hashed API keys. Plain keys are never retained.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
)

var keyName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

// ParseHashes parses comma-separated name:sha256hex pairs. Both names and hashes
// must be unique so an authenticated key has exactly one identity.
func ParseHashes(s string) (map[string][32]byte, error) {
	keys := make(map[string][32]byte)
	seen := make(map[[32]byte]bool)
	for _, entry := range strings.Split(s, ",") {
		name, encoded, ok := strings.Cut(strings.TrimSpace(entry), ":")
		raw, err := hex.DecodeString(encoded)
		if !ok || !keyName.MatchString(name) || err != nil || len(raw) != sha256.Size {
			return nil, errors.New("expected name:sha256hex pairs with valid key names")
		}
		sum := [32]byte(raw)
		if _, exists := keys[name]; exists || seen[sum] {
			return nil, errors.New("key names and hashes must be unique")
		}
		keys[name], seen[sum] = sum, true
	}
	return keys, nil
}

// Identify authenticates a Bearer header by comparing every configured SHA-256
// hash in constant time. The scheme is case insensitive; the key is not.
func Identify(header string, keys map[string][32]byte) (name string, ok bool) {
	scheme, key, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || key == "" {
		return "", false
	}
	for _, c := range key {
		if c <= ' ' || c >= 127 || c == ',' {
			return "", false
		}
	}
	sum := sha256.Sum256([]byte(key))
	for candidate, expected := range keys {
		if subtle.ConstantTimeCompare(sum[:], expected[:]) == 1 {
			name, ok = candidate, true
		}
	}
	return name, ok
}

// NewKey returns a new 256-bit key. The caller decides where to display it once.
func NewKey() string {
	var random [32]byte
	_, _ = rand.Read(random[:]) // crypto/rand terminates the process if entropy fails.
	return "cap_" + base64.RawURLEncoding.EncodeToString(random[:])
}
