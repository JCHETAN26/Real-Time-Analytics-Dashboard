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

// ─── Event types (must match Avro schema field names exactly) ─────────────────
// The Schema Registry has these schemas registered with capitalized field names
// because Go exported struct fields are used directly by the Avro serializer.

type UserEvent struct {
	EventID    string `avro:"EventID"`
	UserID     string `avro:"UserID"`
	EventType  string `avro:"EventType"`
	Page       string `avro:"Page"`
	SessionID  string `avro:"SessionID"`
	DeviceType string `avro:"DeviceType"`
	EventTime  int64  `avro:"EventTime"`
}

type OrderEvent struct {
	OrderID     string  `avro:"OrderID"`
	UserID      string  `avro:"UserID"`
	ProductID   string  `avro:"ProductID"`
	ProductName string  `avro:"ProductName"`
	Category    string  `avro:"Category"`
	OrderStatus string  `avro:"OrderStatus"`
	Revenue     float64 `avro:"Revenue"`
	Quantity    int32   `avro:"Quantity"`
	Region      string  `avro:"Region"`
	OrderTime   int64   `avro:"OrderTime"`
}

type InventoryEvent struct {
	ProductID       string `avro:"ProductID"`
	ProductName     string `avro:"ProductName"`
	Category        string `avro:"Category"`
	StockAdjustment int32  `avro:"StockAdjustment"`
	StockOnHand     int32  `avro:"StockOnHand"`
	Reason          string `avro:"Reason"`
	EventTime       int64  `avro:"EventTime"`
}

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	broker         = "localhost:9092"
	schemaRegistry = "http://localhost:8081"
	groupID        = "stream-processor-v1"
	ssePort        = ":8088"
	dlqTopic       = "dlq"
)

// ─── State ────────────────────────────────────────────────────────────────────

var (
	clients   = make(map[chan string]bool)
	clientsMu sync.Mutex
)

// ─── Deserialize helper ───────────────────────────────────────────────────────
// deserializeEvent deserializes the Avro payload into the appropriate typed
// struct and returns it as a map[string]interface{} for JSON broadcasting.
// Panics are caught and returned as errors.
func deserializeEvent(deser *avro.GenericDeserializer, topic string, value []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	var deserErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				deserErr = fmt.Errorf("panic in deserializer: %v", r)
			}
		}()
		switch topic {
		case "user-events":
			var ev UserEvent
			if err := deser.DeserializeInto(topic, value, &ev); err != nil {
				deserErr = err
				return
			}
			result = map[string]interface{}{
				"event_id":    ev.EventID,
				"user_id":     ev.UserID,
				"event_type":  ev.EventType,
				"page":        ev.Page,
				"session_id":  ev.SessionID,
				"device_type": ev.DeviceType,
				"event_time":  ev.EventTime,
			}
		case "order-events":
			var ev OrderEvent
			if err := deser.DeserializeInto(topic, value, &ev); err != nil {
				deserErr = err
				return
			}
			result = map[string]interface{}{
				"order_id":     ev.OrderID,
				"user_id":      ev.UserID,
				"product_id":   ev.ProductID,
				"product_name": ev.ProductName,
				"category":     ev.Category,
				"order_status": ev.OrderStatus,
				"revenue":      ev.Revenue,
				"quantity":     ev.Quantity,
				"region":       ev.Region,
				"order_time":   ev.OrderTime,
			}
		case "inventory-events":
			var ev InventoryEvent
			if err := deser.DeserializeInto(topic, value, &ev); err != nil {
				deserErr = err
				return
			}
			result = map[string]interface{}{
				"product_id":       ev.ProductID,
				"product_name":     ev.ProductName,
				"category":         ev.Category,
				"stock_adjustment": ev.StockAdjustment,
				"stock_on_hand":    ev.StockOnHand,
				"reason":           ev.Reason,
				"event_time":       ev.EventTime,
			}
		default:
			deserErr = fmt.Errorf("unknown topic: %s", topic)
		}
	}()

	return result, deserErr
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	fmt.Println("🌊 StreamSense | Initializing Stream Processor...")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           groupID,
		"auto.offset.reset":  "latest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		log.Fatalf("Consumer failure: %s", err)
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		log.Fatalf("Producer failure: %s", err)
	}

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistry))
	if err != nil {
		log.Fatalf("SR failure: %s", err)
	}

	deser, err := avro.NewGenericDeserializer(srClient, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Deserializer failure: %s", err)
	}

	topics := []string{"user-events", "order-events", "inventory-events"}
	if err := c.SubscribeTopics(topics, nil); err != nil {
		log.Fatalf("Subscription failed: %s", err)
	}

	// SSE server — one endpoint, all topics fan out here
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

		clientsMu.Lock()
		delete(clients, ch)
		clientsMu.Unlock()
	})
	go r.Run(ssePort)

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
				topic := *e.TopicPartition.Topic
				fmt.Printf("📥 Received from %s: partition %d offset %d\n",
					topic, e.TopicPartition.Partition, e.TopicPartition.Offset)

				payload, deserErr := deserializeEvent(deser, topic, e.Value)
				if deserErr != nil {
					log.Printf("❌ Deserialization error: %s (Topic: %s)", deserErr, topic)
					routeToDLQ(p, topic, e.Value, "deserialization_error", deserErr.Error())
					continue
				}

				// Validate: route negative-revenue orders to DLQ
				if topic == "order-events" {
					if rev, ok := payload["revenue"].(float64); ok && rev < 0 {
						log.Printf("⚠️ Validation failed: negative revenue in order %v", payload["order_id"])
						routeToDLQ(p, topic, e.Value, "validation_error", "negative revenue")
						continue
					}
				}

				// Broadcast to all SSE clients
				jsonData, _ := json.Marshal(map[string]interface{}{
					"topic":   topic,
					"payload": payload,
				})
				clientsMu.Lock()
				for client := range clients {
					client <- string(jsonData)
				}
				clientsMu.Unlock()

				log.Printf("📤 Broadcast %s event to %d client(s)", topic, len(clients))

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

// routeToDLQ routes a failed message to the dead letter queue with
// error-context headers so failures can be triaged without blocking the stream.
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
