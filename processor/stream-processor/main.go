package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

const (
	broker           = "localhost:9092"
	schemaRegistry   = "http://localhost:8081"
	groupID          = "stream-processor-v1"
	ssePort          = ":8088"
	dlqTopic         = "dlq"
)

var (
	// Channel for broadcasting to all web clients
	broadcast = make(chan string)
	clients   = make(map[chan string]bool)
	clientsMu sync.Mutex
)

func main() {
	fmt.Println("🌊 StreamSense | Initializing Stream Processor...")

	// 1. Consumer for raw events
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})

	if err != nil {
		log.Fatalf("Consumer failure: %s", err)
	}

	// 2. Producer for enriched/filtered data
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		log.Fatalf("Producer failure: %s", err)
	}

	// 3. Schema Registry + Avro Deserializer
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistry))
	if err != nil {
		log.Fatalf("SR failure: %s", err)
	}

	deser, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Deserializer failure: %s", err)
	}

	topics := []string{"user-events", "order-events", "inventory-events"}
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatalf("Subscription failed: %s", err)
	}

	// 4. Setup SSE Server (HTTP streaming)
	r := gin.Default()
	r.Use(cors.Default())
	r.GET("/events", func(ctx *gin.Context) {
		ch := make(chan string)
		clientsMu.Lock()
		clients[ch] = true
		clientsMu.Unlock()

		// Stream blocks until the client disconnects; clean up afterward.
		ctx.Stream(func(w io.Writer) bool {
			if msg, ok := <-ch; ok {
				ctx.SSEvent("message", msg)
				return true
			}
			return false
		})

		clientsMu.Lock()
		delete(clients, ch)
		clientsMu.Unlock()
	})
	go r.Run(ssePort)

	// Signal handling for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("✅ StreamSense | Listening for events on: %v\n", topics)

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Received termination signal: %v\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// 1. Log incoming message
				topic := *e.TopicPartition.Topic
				fmt.Printf("📥 Received from %s: partition %d offset %d\n", 
					topic, e.TopicPartition.Partition, e.TopicPartition.Offset)

				// 2. Deserialize (Avro)
				obj, err := deser.Deserialize(topic, e.Value)
				if err != nil {
					log.Printf("❌ Deserialization error: %s (Topic: %s)", err, topic)
					// Route malformed messages to the Dead Letter Queue so the
					// main stream is never blocked by a single bad record.
					routeToDLQ(p, topic, e.Value, "deserialization_error", err.Error())
					continue
				}
				payload, ok := obj.(map[string]interface{})
				if !ok {
					log.Printf("❌ Unexpected payload type %T (Topic: %s)", obj, topic)
					routeToDLQ(p, topic, e.Value, "type_error", fmt.Sprintf("expected map, got %T", obj))
					continue
				}

				// 3. Validation / Enrichment. Records that fail validation are
				// sent to the DLQ rather than propagated downstream.
				if topic == "order-events" {
					if rev, ok := payload["revenue"].(float64); ok && rev < 0 {
						log.Printf("⚠️ Validation failed: negative revenue in order %v", payload["order_id"])
						routeToDLQ(p, topic, e.Value, "validation_error", "negative revenue")
						continue
					}
				}

				// 4. Broadcast to frontend!
				jsonData, _ := json.Marshal(map[string]interface{}{
					"topic":   topic,
					"payload": payload,
				})
				
				clientsMu.Lock()
				for client := range clients {
					client <- string(jsonData)
				}
				clientsMu.Unlock()

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.IsFatal() {
					run = false
				}
			}
		}
	}

	fmt.Println("Closing consumer and producer...")
	c.Close()
	p.Close()
}

// routeToDLQ publishes a failed message to the Dead Letter Queue topic,
// preserving the original payload and attaching error-context headers so the
// failure can be triaged later. DLQ production failures are logged but never
// block the main consume loop.
func routeToDLQ(p *kafka.Producer, sourceTopic string, value []byte, reason, detail string) {
	dlq := dlqTopic
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &dlq, Partition: kafka.PartitionAny},
		Value:          value,
		Headers: []kafka.Header{
			{Key: "dlq_reason", Value: []byte(reason)},
			{Key: "dlq_detail", Value: []byte(detail)},
			{Key: "source_topic", Value: []byte(sourceTopic)},
			{Key: "failed_at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}
	if err := p.Produce(msg, nil); err != nil {
		log.Printf("⚠️ Failed to route message to DLQ: %s", err)
		return
	}
	log.Printf("📨 Routed message from %s to DLQ (reason=%s)", sourceTopic, reason)
}
