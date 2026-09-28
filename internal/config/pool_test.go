package config

import (
	"strconv"
	"strings"
	"testing"
)

func TestDatabaseMaxConnections(t *testing.T) {
	c, err := load(env())
	if err != nil || c.DBMaxConns != 40 {
		t.Fatalf("default pool limit=%d error=%v", c.DBMaxConns, err)
	}
	for _, value := range []string{"1", "20", "40"} {
		values := env()
		values["CAPSTAN_DB_MAX_CONNS"] = value
		c, err := load(values)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := strconv.Atoi(value)
		if c.DBMaxConns != int32(want) {
			t.Fatalf("pool size = %d", c.DBMaxConns)
		}
	}
	for _, value := range []string{"0", "-1", "1.5", "2147483648", "secret-value"} {
		values := env()
		values["CAPSTAN_DB_MAX_CONNS"] = value
		_, err := load(values)
		if err == nil || !strings.Contains(err.Error(), "CAPSTAN_DB_MAX_CONNS") || strings.Contains(err.Error(), "secret-value") {
			t.Fatalf("invalid pool size error = %v", err)
		}
	}
}
