package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {

	topic := "user-profiles-segio"

	writer := kafka.Writer{
		Addr:  kafka.TCP("localhost:9094"),
		Topic: topic,
		// Idempotence config
		RequiredAcks: kafka.RequireAll, // This enables idempotence in our code
		MaxAttempts:  10,
	}
	defer func() {
		err := writer.Close()
		if err != nil {
			log.Fatal("Failed to close the writer:", err)
		}
	}()

	fmt.Println("Producer started. Sending messages to topic:", topic)

	// We'll send 10 message to topic
	for i := 1; i <= 10; i++ {
		message := fmt.Sprintf("Message #%d for user", i)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := writer.WriteMessages(ctx, kafka.Message{
			Value: []byte(message),
		})
		if err != nil {
			log.Fatalf("Failed to write message #%d: %v", i, err)
		}
		fmt.Printf("Sent message: %s\n", message)

		time.Sleep(500 * time.Millisecond)

	}

}
