package main

import (
	"encoding/json"
	"fmt"
	"kafkaaggregator/pkg/cache"
	"kafkaaggregator/pkg/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type RecordProcessor struct {
}

type NSCDCRecord struct {
	CustomField   string `json:"CUSTOM_FIELD,omitempty"`
	StandardField string `json:"STANDARD_FIELD,omitempty"`
	ValueNew      string `json:"VALUE_NEW,omitempty"`
	ValueOld      string `json:"VALUE_OLD,omitempty"`
	Operation     string `json:"OPERATION,omitempty"`
	DateCreated   string `json:"DATE_CREATED,omitempty"`
	TransactionId string `json:"TRANSACTION_ID,omitempty"`
}
type NSCDCAgg struct {
	Key    string        `json:"KEY,omitempty"`
	Events []NSCDCRecord `json:"EVENTS,omitempty"`
}

func main() {
	fmt.Printf("Kafka Aggregator Service Starting")

	KafkaTopic := "****"
	KafkaConsumerGroup := "cdc-agg-test-1"
	KafkaBrokers := []string{"*****"}

	cacheService, err := cache.NewMemDatastore()
	if err != nil {
		fmt.Printf("error creating cacheService: %v", err)
		panic(err)
	}
	kafkaClient, err := kafka.NewClientWrapper(KafkaTopic, KafkaConsumerGroup, KafkaBrokers)
	if err != nil {
		fmt.Printf("error creating Kafka client: %v", err)
		panic(err)
	}
	recordProcessor := RecordProcessor{}
	kafkaClient.StartPolling(cacheService)
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan bool)
	go func(cacheService *cache.MemDatastore) {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				err := cacheService.AggregateRecordsOlderThan(time.Second*10, &recordProcessor)
				if err != nil {
					panic(err) //TODO: should we panic here?
				}
			}
		}
	}(cacheService)

	exitChannel := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(exitChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-exitChannel //block until we get a signal

}

func (rp *RecordProcessor) ProcessAggregatedMessages(key string, recordsBytes [][]byte) error {
	aggBytes, err := rp.ExtractJsonMessage(key, recordsBytes)
	if err != nil {
		return err
	}
	log.Printf(string(aggBytes))
	return nil
}

func (rp *RecordProcessor) ExtractJsonMessage(key string, recordsBytes [][]byte) ([]byte, error) {
	records := make([]NSCDCRecord, len(recordsBytes), len(recordsBytes))
	for i, recordBytes := range recordsBytes {
		record := NSCDCRecord{}
		err := json.Unmarshal(recordBytes, &record)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}
	aggregatedRecord := NSCDCAgg{
		Key:    key,
		Events: records,
	}
	aggBytes, _ := json.Marshal(aggregatedRecord)
	return aggBytes, nil
}
