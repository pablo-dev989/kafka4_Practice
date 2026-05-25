package main

import "github.com/confluentinc/confluent-kafka-go/kafka"

func main() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"group.id":          "user-profiles-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}

}
