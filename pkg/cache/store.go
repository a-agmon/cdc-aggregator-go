package cache

import (
	"fmt"
	"github.com/hashicorp/go-memdb"
	"log"
	"time"
)

type MemDatastore struct {
	db *memdb.MemDB
}

type AggregateProcessor interface {
	ProcessAggregatedMessages(key string, records [][]byte) error
}

const (
	TableName = "records"
)

func NewMemDatastore() (*MemDatastore, error) {
	schema := getDBSchema()
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		return nil, err
	}
	return &MemDatastore{db}, nil
}

func getTableSchema() *memdb.TableSchema {
	return &memdb.TableSchema{
		Name: TableName,
		Indexes: map[string]*memdb.IndexSchema{
			"id": &memdb.IndexSchema{
				Name:    "id",
				Unique:  true,
				Indexer: &memdb.StringFieldIndex{Field: "id"},
			},
			"Key": &memdb.IndexSchema{
				Name:    "Key",
				Unique:  false,
				Indexer: &memdb.StringFieldIndex{Field: "Key"},
			},
			"Timestamp": &memdb.IndexSchema{
				Name:    "Timestamp",
				Unique:  false,
				Indexer: &memdb.IntFieldIndex{Field: "Timestamp"},
			},
		},
	}
}

func getDBSchema() *memdb.DBSchema {
	tableSchema := getTableSchema()
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			TableName: tableSchema,
		},
	}
}

func (md *MemDatastore) Handle(key string, msgBytes []byte) error {
	txn := md.db.Txn(true)
	log.Printf("Recieved Message from Kafka %s", string(msgBytes))
	r := NewRecord(key, msgBytes)
	err := txn.Insert(TableName, r)
	if err != nil {
		return err
	}
	txn.Commit()
	return nil
}
func (md *MemDatastore) Insert(r Record) error {
	txn := md.db.Txn(true)
	err := txn.Insert(TableName, r)
	if err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (md *MemDatastore) GetFirst() (Record, error) {
	txn := md.db.Txn(false)
	rawMsg, err := txn.First(TableName, "Key")
	if err != nil {
		return Record{}, err
	}
	record, ok := rawMsg.(Record)
	if !ok {
		return Record{}, fmt.Errorf("unable to cast record object from cache")
	}
	txn.Commit()
	return record, nil
}

func (md *MemDatastore) GetRecordsOlderThan(secondsBack time.Duration) ([]Record, error) {
	txn := md.db.Txn(true)
	tsUpperBound := time.Now().UnixMilli() - secondsBack.Milliseconds()
	it, err := txn.ReverseLowerBound(TableName, "Timestamp", tsUpperBound)
	if err != nil {
		return nil, err
	}
	records := make([]Record, 0)
	for rawRec := it.Next(); rawRec != nil; rawRec = it.Next() {
		record := rawRec.(Record)
		records = append(records, record)
	}
	txn.Commit()
	return records, nil
}

func (md *MemDatastore) AggregateRecordsOlderThan(secondsBack time.Duration, proc AggregateProcessor) error {
	txn := md.db.Txn(true)
	log.Printf("Starting Aggregation Query")
	defer txn.Abort()
	tsUpperBound := time.Now().UnixMilli() - secondsBack.Milliseconds()
	it, err := txn.ReverseLowerBound(TableName, "Timestamp", tsUpperBound)
	if err != nil {
		return err
	}
	records := make(map[string][][]byte, 0)
	for rawRec := it.Next(); rawRec != nil; rawRec = it.Next() {
		record := rawRec.(Record)
		records[record.Key] = append(records[record.Key], record.Value)
		err = txn.Delete(TableName, rawRec)
		if err != nil {
			return err
		}
	}
	for key, val := range records {
		err := proc.ProcessAggregatedMessages(key, val)
		if err != nil {
			panic(err) //Todo: Should we panic here?
		}
	}
	txn.Commit() //t
	return nil
}
