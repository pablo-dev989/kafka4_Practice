package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"group.id":          "user-profiles-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

	defer func() {
		err := c.Close()
		if err != nil {
			log.Fatal("Failed to close consumer:", err)
		}
	}()

	topic := "user-profiles"
	err = c.Subscribe(topic, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer started. Waiting for messages...")

	for {
		msg, err := c.ReadMessage(55 * time.Second)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}
			fmt.Println("Consume error:", err)
			continue
		}
		fmt.Printf("Received message from %s: %s\n", msg.TopicPartition, string(msg.Value))
	}
}
