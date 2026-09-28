package auth

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

func TestParseHashesAndIdentify(t *testing.T) {
	sum := sha256.Sum256([]byte("secret"))
	keys, err := ParseHashes(fmt.Sprintf("owner:%x", sum))
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []string{"Bearer secret", "bearer secret"} {
		if name, ok := Identify(header, keys); !ok || name != "owner" {
			t.Errorf("Identify(%q) = %q, %v", header, name, ok)
		}
	}
	for _, header := range []string{"", "secret", "Basic secret", "Bearer", "Bearer ", "Bearer wrong", "Bearer  secret", "Bearer secret extra", "Bearer secret\n", "Bearer secret,Bearer secret"} {
		if name, ok := Identify(header, keys); ok || name != "" {
			t.Errorf("accepted malformed or incorrect header %q", header)
		}
	}
	if _, ok := Identify("Bearer secret", nil); ok {
		t.Fatal("accepted without keys")
	}
}

func TestParseHashesRejectsAmbiguousOrMalformedEntries(t *testing.T) {
	h := strings.Repeat("ab", 32)
	for _, input := range []string{"", "owner", ":" + h, "bad name:" + h, "a:ab", "a:" + strings.Repeat("gg", 32), "a:" + h + ",a:" + strings.Repeat("cd", 32), "a:" + h + ",b:" + h, "a:" + h + ","} {
		if _, err := ParseHashes(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	keys, err := ParseHashes("owner:" + h + ",worker:" + strings.Repeat("cd", 32))
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
}

func TestNewKey(t *testing.T) {
	a, b := NewKey(), NewKey()
	if a == b {
		t.Fatal("repeated random key")
	}
	if !strings.HasPrefix(a, "cap_") {
		t.Fatalf("missing prefix: %q", a)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(a, "cap_"))
	if err != nil || len(raw) != 32 {
		t.Fatalf("not 32 random bytes: len=%d err=%v", len(raw), err)
	}
}
