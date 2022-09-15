package cache

import (
	"fmt"
	"time"
)

type Record struct {
	id        string
	Key       string
	Timestamp int64
	Value     []byte
}

func NewRecord(key string, value []byte) Record {
	ts := time.Now().UnixMilli()
	id := fmt.Sprintf("%s:%d", key, ts)
	return Record{
		id:        id,
		Key:       key,
		Timestamp: ts,
		Value:     value,
	}
}
