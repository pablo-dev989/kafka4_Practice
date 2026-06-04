package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// The state of our application
// We use a simple in-memory map
// Key: user_id (string), Value:  click_count(int)
var clickCounts = make(map[string]int)

func main() {

	topic := "user-clicks"
	topic2 := "clicks-per-window"

	//  Consumer setup
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   topic,
		GroupID: "click-counter-grp",
	})

	defer consumer.Close()

	//  Producer setup (Transformer)
	producer := kafka.Writer{
		Addr: kafka.TCP("localhost:9092"),
	}

	defer producer.Close()

	log.Println("Starting stateful click counter...")

	ctx := context.Background()

	// -- Windowing Logic
	// CReate a Ticker that fires every 10 seconds. This define out tumbling window
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// The 10 second window has tumbled. It's now time to peocess the result
			log.Println("Window closed. Processing and emitting result...")
			for userId, count := range clickCounts {
				// Create the result payload
				result := map[string]interface{}{
					"user_id":     userId,
					"click_count": count,
					"window_end":  time.Now().UTC().Format(time.RFC3339),
				}
				resultBytes, err := json.Marshal(result)
				if err != nil {
					log.Println("failed to marshal result:", err)
					return
				}

				msg := kafka.Message{
					Topic: topic2,
					Key:   []byte(userId),
					Value: resultBytes,
				}

				err = producer.WriteMessages(ctx, msg)
				if err != nil {
					log.Println("failed to write result:", err)
				} else {
					log.Printf("Emitted the result for user: %s, count: %d", userId, count)
				}
			}

			//  IMPORTANT: Reset the state for the next window
			clickCounts = make(map[string]int)
			log.Println("******** STATE RESET FOR NEW WINDOW ********")

		default:
			//  In between clicks, we continuously read messages.
			msg, err := consumer.ReadMessage(ctx)
			if err != nil {
				log.Println("could not read the message:", err)
				return
			}

			// Aggregation Logic
			userId := string(msg.Key)
			clickCounts[userId]++ // Increment the count for this user
			log.Printf("increment count for user %s to %d", userId, clickCounts[userId])
		}

	}
}
