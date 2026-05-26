package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {

	requestTopic := "fraud-check-requests"
	groupId := "fraud-detection-service-segio"

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092", "localhost:9094", "localhost:9095"},
		Topic:   requestTopic,
		GroupID: groupId,
	})
	defer consumer.Close()

	// We create a generic Writer. By leaving the Topic field blank.
	// we can specify the topic for each message we send.
	producer := &kafka.Writer{
		Addr: kafka.TCP("localhost:9092", "localhost:9094", "localhost:9095"),
	}
	defer producer.Close()

	fmt.Println("Replier service started. Waiting for requests...")

	ctx := context.Background()
	for {
		msg, err := consumer.ReadMessage(ctx)
		if err != nil {
			log.Println("could not read message:", err)
			break
		}

		fmt.Println("Received request:", string(msg.Value))

		var correlationId, replyTopic string
		for _, h := range msg.Headers {
			if h.Key == "correlation-id" {
				correlationId = string(h.Value)
			}
			if h.Key == "reply-to-topic" {
				replyTopic = string(h.Value)
			}
		}

		if correlationId != "" && replyTopic != "" {
			fmt.Printf("Replying to topic %s with correlation ID %s", replyTopic, correlationId)

			// Create the reply message, specifying the dynamic topic
			replyMessage := kafka.Message{
				Topic: replyTopic,
				Value: []byte("Transaction Approved by SegmentIO Replier"),
				Headers: []kafka.Header{
					{Key: "correlation-id", Value: []byte(correlationId)},
				},
			}

			// Send the reply
			err := producer.WriteMessages(ctx, replyMessage)
			if err != nil {
				fmt.Println("Failed to send reply:", err)
			} else {
				fmt.Println("Reply sent for correlation ID:", correlationId)
			}
		}
	}
}
