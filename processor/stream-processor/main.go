package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

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

		ctx.Stream(func(w io.Writer) bool {
			if msg, ok := <-ch; ok {
				ctx.SSEvent("message", msg)
				return true
			}
			return false
		})

		ctx.OnDone(func() {
			clientsMu.Lock()
			delete(clients, ch)
			clientsMu.Unlock()
			close(ch)
		})
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
				var payload map[string]interface{}
				err := deser.DeserializeIntoRecord(topic, e.Value, &payload)
				if err != nil {
					log.Printf("❌ Deserialization error: %s (Topic: %s)", err, topic)
					// Route to DLQ here in future version
					continue
				}

				// 3. Simple Validation / Enrichment Simulation
				// Example: Ensure revenue is positive on order events
				if topic == "order-events" {
					rev, ok := payload["revenue"].(float64)
					if ok && rev < 0 {
						fmt.Printf("⚠️ Anomaly Detected: Negative revenue in order %v\n", payload["order_id"])
						// Could produce to 'anomalies' topic here
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
