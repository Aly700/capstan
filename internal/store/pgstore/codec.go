package pgstore

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
)

func marshal(m proto.Message) ([]byte, error) {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil, nil
	}
	b, err := proto.Marshal(m)
	if err == nil && b == nil {
		b = []byte{}
	}
	return b, err
}

func unmarshal[T any, M interface {
	*T
	proto.Message
}](data []byte, dest **T) error {
	if data == nil {
		*dest = nil
		return nil
	}
	m := M(new(T))
	if err := proto.Unmarshal(data, m); err != nil {
		return fmt.Errorf("pgstore: decode protobuf: %w", err)
	}
	*dest = m
	return nil
}

func nullTime(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v.UTC()
}

// pgx calls this for both nullable and required timestamptz columns. Keeping the
// normalization here prevents the host timezone from leaking into store records.
type utcTime struct{ dest *time.Time }

func (v utcTime) ScanTimestamptz(src pgtype.Timestamptz) error {
	if !src.Valid {
		*v.dest = time.Time{}
		return nil
	}
	if src.InfinityModifier != pgtype.Finite {
		return fmt.Errorf("pgstore: infinite timestamp")
	}
	*v.dest = src.Time.UTC()
	return nil
}
