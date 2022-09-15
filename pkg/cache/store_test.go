package cache_test

import (
	"fmt"
	"kafkaaggregator/pkg/cache"
	"testing"
	"time"
)

func TestMemDatastore_Create(t *testing.T) {
	_, err := cache.NewMemDatastore()
	if err != nil {
		t.Error(err)
	}
}

func TestMemDatastore_Insert(t *testing.T) {
	cacheStore, err := cache.NewMemDatastore()
	r := cache.NewRecord("1234", []byte("12345"))
	if err != nil {
		t.Error(err)
	}
	err = cacheStore.Insert(r)
	if err != nil {
		t.Error(err)
	}
}

func TestMemDatastore_InsertAndRead(t *testing.T) {
	cacheStore, err := cache.NewMemDatastore()
	r := cache.NewRecord("1234", []byte("12345"))
	if err != nil {
		t.Error(err)
	}
	err = cacheStore.Insert(r)
	if err != nil {
		t.Error(err)
	}
	r, err = cacheStore.GetFirst()
	if err != nil {
		t.Error(err)
	}
	t.Logf("record: %v", r)
}

func TestMemDatastore_GetRecordsOlderThan(t *testing.T) {
	cacheStore, err := cache.NewMemDatastore()
	if err != nil {
		t.Error(err)
	}
	r1 := cache.NewRecord("1111", []byte("1111"))
	r2 := cache.NewRecord("2222", []byte("2222"))
	time.Sleep(time.Second * 5)
	r3 := cache.NewRecord("3333", []byte("3333"))
	recs := [3]cache.Record{r1, r2, r3}
	for _, rec := range recs {
		err = cacheStore.Insert(rec)
		if err != nil {
			t.Error(err)
		}
	}
	savedRecs, err := cacheStore.GetRecordsOlderThan(time.Second * 3)
	var count int = 0
	for ix, result := range savedRecs {
		t.Logf("%d Record: %v", ix, result)
		count++
	}
	if len(savedRecs) != 2 {
		t.Failed()
	}
}

type MockProcessor struct{}

func (m *MockProcessor) ProcessAggregatedMessages(key string, records [][]byte) error {
	fmt.Printf("KEY: %s Records: %d\n", key, len(records))
	return nil
}

func TestMemDatastore_Aggregate(t *testing.T) {
	cacheStore, err := cache.NewMemDatastore()
	if err != nil {
		t.Error(err)
	}
	r1 := cache.NewRecord("1111", []byte("1111"))
	time.Sleep(time.Second * 1)
	r2 := cache.NewRecord("1111", []byte("2222"))
	time.Sleep(time.Second * 5)
	r3 := cache.NewRecord("3333", []byte("3333"))
	recs := [3]cache.Record{r1, r2, r3}
	for _, rec := range recs {
		err = cacheStore.Insert(rec)
		if err != nil {
			t.Error(err)
		}
	}
	mp := MockProcessor{}
	err = cacheStore.AggregateRecordsOlderThan(time.Second*3, &mp)
	t.Log("You should see above 2 entries ^^^")
	if err != nil {
		t.Error(err)
	}
}
