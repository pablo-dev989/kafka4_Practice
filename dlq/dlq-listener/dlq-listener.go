package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

const (
	dlqTopic   = "orders-dlq"
	dlqGroupId = "dlq-handler-group"
	brokerUrl  = "localhost:9092"
)

func main() {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerUrl},
		Topic:   dlqTopic,
		GroupID: dlqGroupId,
	})

	defer reader.Close()
	log.Println("Starting DLQ Monitor for topic:", dlqTopic)

	ctx := context.Background()

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			panic(err)
		}
		log.Printf("DLQ recived [%s] offset %d: %s", string(m.Key), m.Offset, string(m.Value))
		reader.CommitMessages(ctx, m)
	}

}
