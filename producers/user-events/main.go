package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/google/uuid"
)

type UserEvent struct {
	EventID    string `avro:"event_id"`
	UserID     string `avro:"user_id"`
	EventType  string `avro:"event_type"`
	Page       string `avro:"page"`
	SessionID  string `avro:"session_id"`
	DeviceType string `avro:"device_type"`
	EventTime  int64  `avro:"event_time"`
}

const (
	broker         = "localhost:9092"
	schemaRegistry = "http://localhost:8081"
	topic          = "user-events"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"linger.ms":          5,    // batch within 5ms window for throughput
		"batch.size":         65536,
		"compression.type":   "snappy",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistry))
	if err != nil {
		log.Fatalf("Failed to create SR client: %s", err)
	}

	ser, err := avro.NewGenericSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatalf("Failed to create serializer: %s", err)
	}

	// BULK_MODE=1 fires as fast as possible for a fixed duration (BULK_SECONDS, default 600).
	// Normal mode retains the original 1/sec rate for dashboard realism.
	bulkMode := os.Getenv("BULK_MODE") == "1"
	bulkSecs, _ := strconv.Atoi(os.Getenv("BULK_SECONDS"))
	if bulkSecs <= 0 {
		bulkSecs = 600 // 10 minutes default
	}

	if bulkMode {
		runBulk(p, ser, bulkSecs)
		return
	}

	fmt.Printf("🚀 StreamSense | UserEvent Producer → %s (normal mode)\n", topic)
	for {
		produceOne(p, ser)
		time.Sleep(1 * time.Second)
	}
}

// runBulk fires events as fast as Kafka can accept them for duration seconds,
// then prints a summary count you can use as a real metric.
func runBulk(p *kafka.Producer, ser *avro.GenericSerializer, durationSecs int) {
	deadline := time.Now().Add(time.Duration(durationSecs) * time.Second)
	var sent, failed int64

	// Drain delivery reports in background
	go func() {
		for e := range p.Events() {
			m, ok := e.(*kafka.Message)
			if !ok {
				continue
			}
			if m.TopicPartition.Error != nil {
				atomic.AddInt64(&failed, 1)
			} else {
				atomic.AddInt64(&sent, 1)
			}
		}
	}()

	fmt.Printf("🚀 BULK MODE | user-events | running for %ds\n", durationSecs)
	batchSize := 500
	topicName := topic

	for time.Now().Before(deadline) {
		for i := 0; i < batchSize; i++ {
			event := generateUserEvent()
			payload, err := ser.Serialize(topic, &event)
			if err != nil {
				continue
			}
			p.Produce(&kafka.Message{ //nolint:errcheck
				TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
				Value:          payload,
			}, nil) // nil = use internal events channel
		}
		p.Flush(1000) // flush every batch
		elapsed := time.Since(deadline.Add(-time.Duration(durationSecs) * time.Second))
		if int(elapsed.Seconds())%30 == 0 {
			fmt.Printf("  ⚡ user-events: %s elapsed | ~%d delivered so far\n",
				elapsed.Round(time.Second), atomic.LoadInt64(&sent))
		}
	}

	p.Flush(10000)
	total := atomic.LoadInt64(&sent)
	fmt.Printf("✅ BULK DONE | user-events | %d events delivered | %d failed\n", total, atomic.LoadInt64(&failed))
}

func produceOne(p *kafka.Producer, ser *avro.GenericSerializer) {
	event := generateUserEvent()
	payload, err := ser.Serialize(topic, &event)
	if err != nil {
		log.Printf("Serialization error: %s", err)
		return
	}
	deliveryChan := make(chan kafka.Event, 1)
	topicName := topic
	if err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
		Value:          payload,
	}, deliveryChan); err != nil {
		log.Printf("Produce error: %s", err)
		return
	}
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v", m.TopicPartition.Error)
	} else {
		log.Printf("✅ user-event: %s | %s | %s", event.EventID[:8], event.UserID, event.EventType)
	}
}

func generateUserEvent() UserEvent {
	eventTypes := []string{"page_view", "search", "click", "add_to_cart", "checkout"}
	devices := []string{"mobile", "desktop", "tablet"}
	pages := []string{"home", "product_listing", "product_detail", "cart", "checkout_success"}

	return UserEvent{
		EventID:    uuid.New().String(),
		UserID:     fmt.Sprintf("user_%d", rand.Intn(1000)),
		EventType:  eventTypes[rand.Intn(len(eventTypes))],
		Page:       pages[rand.Intn(len(pages))],
		SessionID:  uuid.New().String(),
		DeviceType: devices[rand.Intn(len(devices))],
		EventTime:  time.Now().UnixMilli(),
	}
}

func generateUserEvent() UserEvent {
	eventTypes := []string{"page_view", "search", "click", "add_to_cart", "checkout"}
	devices := []string{"mobile", "desktop", "tablet"}
	pages := []string{"home", "product_listing", "product_detail", "cart", "checkout_success"}

	return UserEvent{
		EventID:    uuid.New().String(),
		UserID:     fmt.Sprintf("user_%d", rand.Intn(1000)),
		EventType:  eventTypes[rand.Intn(len(eventTypes))],
		Page:       pages[rand.Intn(len(pages))],
		SessionID:  uuid.New().String(),
		DeviceType: devices[rand.Intn(len(devices))],
		EventTime:  time.Now().UnixMilli(),
	}
}
