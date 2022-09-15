package kafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type MessageHandler interface {
	Handle(key string, msg []byte) error
}

type ClientWrapper struct {
	pollTopic     string
	consumerGroup string
	brokers       []string
	*kgo.Client   //embedding client her
}

func NewClientWrapper(pollTopic string, consumerGroup string, brokers []string) (*ClientWrapper, error) {
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup), //onlu when consumerGroup is needed
		// in the consume group scenario we will start from the offset, here we override this (and start from the end
		//kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().UnixMilli())),
		kgo.ConsumeTopics(pollTopic),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating new Kafka client (probably configuration issue): %w", err)
	}
	return &ClientWrapper{
		pollTopic:     pollTopic,
		consumerGroup: consumerGroup,
		brokers:       brokers,
		Client:        kafkaClient,
	}, nil
}

func (kcw *ClientWrapper) StartPolling(handler MessageHandler) {
	go func() {
		defer kcw.Close() //TODO should this defer be here?
		ctx := context.Background()
		for {
			fetches := kcw.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				fmt.Printf("error fetching events from Kafka: %v", errs)
				break
				//TODO decide how to deal with this exception - panic or just let it continue
			}
			fetches.EachRecord(func(record *kgo.Record) {
				err := handler.Handle(string(record.Key), record.Value)
				if err != nil {
					//TODO big issue - panic = error storing things in the cache
					panic(err)
				}
			})
			//TODO: Remove this break - for debugging purposes only
			break
		}
	}()
}

func (kcw *ClientWrapper) StopPolling() {
	kcw.Close()
}
