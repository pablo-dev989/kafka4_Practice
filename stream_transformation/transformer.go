package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Input Event represents the structure of the raw incomming messages
type InputEvent struct {
	Type    string `json:"type"`
	UserId  string `json:"user_id"`
	Payload string `json:"payload"`
}

// Output Event represents the structure of the clean transformed outgoing messages
type OutputEvent struct {
	UserId       string `json:"user_id"`
	Action       string `json:"action"`
	Timestamp    int64  `json:"timestamp"`
	OriginalData string `json:"original_data"`
}

func main() {

	rawTopic := "raw-user-events"
	processedTopic := "processed-user-events"

	//  Consumer setup
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   rawTopic,
		GroupID: "user-event-transformer-grp",
	})

	defer consumer.Close()

	//  Producer setup (TRansformer)
	producer := kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
	}

	defer producer.Close()

	log.Println("Starting stream transfomer...")

	ctx := context.Background()

	for {
		// read the raw messages
		inMsg, err := consumer.ReadMessage(ctx)
		if err != nil {
			log.Println("could not read message")
			break
		}

		// Transformation 1: Peek/ForEach (The Inspector)
		// we log every message that we receive for auditing/logginf/debugging
		log.Printf("PEEK: Received raw message: key=%s, value=%s", string(inMsg.Key), string(inMsg.Value))

		// Transformation 2: Filter (The Bouncer)
		var inputEvent InputEvent
		err = json.Unmarshal(inMsg.Value, &inputEvent)
		if err != nil {
			log.Printf("FILTER: Discarding malformed JSON message: %s. Error: %v\n", string(inMsg.Value), err)
			continue
		}

		if inputEvent.Type != "login" {
			log.Println("FILTER: Discarding message of type:", inputEvent.Type)
			continue
		}

		// TRansformation 3: Map (The translator)
		outputEvent := OutputEvent{
			UserId:       inputEvent.UserId,
			Action:       strings.ToUpper(inputEvent.Type),
			Timestamp:    time.Now().Unix(),
			OriginalData: inputEvent.Payload,
		}

		outValue, err := json.Marshal(outputEvent)
		if err != nil {
			log.Println("Error marshalling output event:", err)
			continue
		}

		outMsg := kafka.Message{
			Topic: processedTopic,
			Key:   []byte(outputEvent.UserId),
			Value: outValue,
		}

		// Produce the transformed message
		err = producer.WriteMessages(ctx, outMsg)
		if err != nil {
			log.Println("Failed to write transformed message:", err)
		} else {
			log.Println("MAP: Successfully produced transformed message for key:", string(outMsg.Key))
		}

	}

}
