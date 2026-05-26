package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	requestTopic = "orders"
	dlqTopic     = "orders-dlq"
	groupId      = "order-processing-group"
	brokerUrl    = "localhost:9092"
)

func main() {

	writer := kafka.Writer{
		Addr:  kafka.TCP(brokerUrl),
		Topic: requestTopic,
	}
	defer writer.Close()

	messages := []struct {
		OrderID string
		Items   int
	}{
		{"order-001", 3},
		{"order-002", 2},
		{"FAIL-ME-ORDER", 1}, // this will be our "bad message"
		{"order-003", 5},
		{"order-004", 9},
	}

	ctx := context.Background()

	for _, msg := range messages {
		payload, err := json.Marshal(msg)
		if err != nil {
			panic(err)
		}

		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(msg.OrderID),
			Value: payload,
		})
		if err != nil {
			panic(err)
		}

		log.Println("Producer message", msg.OrderID)
		time.Sleep(time.Second)

	}
	log.Println("Finished producing messages")

}
