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

	groupID := "user-profiles-segio-readers"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092", "localhost:9094", "localhost:9095"},
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: time.Second,
	})
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Fatal("Failed to close reader:", err)
		}
	}()

	ctx := context.Background()
	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("Could not read message:", err)
			continue
		}
		fmt.Printf("Received message: partition=%d, offset=%d, key%s, value=%s\n", message.Partition, message.Offset, message.Key, string(message.Value))
		time.Sleep(500 * time.Millisecond)
	}
}
