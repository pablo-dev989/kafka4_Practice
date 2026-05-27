package main

import (
	"context"
	"encoding/json"
	"fmt"
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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerUrl},
		Topic:   requestTopic,
		GroupID: groupId,
	})
	defer reader.Close()

	// DLQ Producer setup (with the consumer app)

	dlqWriter := kafka.Writer{
		Addr:  kafka.TCP(brokerUrl),
		Topic: dlqTopic,
	}
	defer dlqWriter.Close()

	log.Println("Starting consumer for topic:", requestTopic)

	ctx := context.Background()

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Context canceled", err)
				break
			}
			log.Println("Error fetching messages", err, "Continuing...")
			continue
		}

		log.Printf("Received message at offset %d: %s", m.Offset, string(m.Value))
		var order struct {
			OrderID string
			Items   int
		}

		var processingErr error

		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			processingErr = fmt.Errorf("malformed message:%v", err)
		} else {
			log.Printf("Processing order %s with %d items", order.OrderID, order.Items)

			if order.OrderID == "FAIL-ME-ORDER" {
				processingErr = fmt.Errorf("Simulated processing error for order ID: %v", order.OrderID)
			} else {
				// Simulate some task done
				time.Sleep(500 * time.Millisecond)
				processingErr = nil
			}
		}

		//  DLQ handling
		if processingErr != nil {
			log.Printf("ERROR: Failed to process order %s: %v. Sending to DLQ", string(m.Key), processingErr)

			// Construct a detailed payload for the DLQ message
			dlqPayload := map[string]interface{}{
				"original_message":   string(m.Value),
				"error":              processingErr.Error(),
				"timestamp":          time.Now().Format(time.RFC3339),
				"original_topic":     m.Topic,
				"original_partition": m.Partition,
				"original_offset":    m.Offset,
			}
			dlqValue, err := json.Marshal(dlqPayload)
			if err != nil {
				panic(err)
			}

			err = dlqWriter.WriteMessages(ctx, kafka.Message{
				Key:   m.Key,
				Value: dlqValue,
			})
			if err != nil {
				panic(err)
			}

			log.Println("Successfully sent message to DLQ!")
		} else {
			log.Println("Successfully processed order:", string(m.Key))
		}
		// Offset committing
		// this is crucial. Whether the message succeeded or was sent to the DLQ,
		// we commit its offset in the mian tipoc so we don't process it again.
		err = reader.CommitMessages(ctx, m)
		if err != nil {
			panic(err)
		}
		log.Println("Committed the offset:", m.Offset)
	}
}
