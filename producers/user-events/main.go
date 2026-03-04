package main

import (
	"fmt"
	"log"
	"math/rand"
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
	broker           = "localhost:9092"
	schemaRegistry   = "http://localhost:8081"
	topic            = "user-events"
	schemaFile       = "../../infra/schemas/user_events.avsc"
)

func main() {
	// 1. Create Producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	// 2. Schema Registry Client
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistry))
	if err != nil {
		log.Fatalf("Failed to create SR client: %s", err)
	}

	// 3. Avro Serializer (Generic for agility)
	ser, err := avro.NewGenericSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatalf("Failed to create serializer: %s", err)
	}

	fmt.Printf("🚀 StreamSense | Starting UserEvent Producer for topic: %s\n", topic)

	// Simulation loop
	for {
		event := generateUserEvent()
		
		payload, err := ser.Serialize(topic, &event)
		if err != nil {
			log.Printf("Serialization error: %s (Topic: %s)", err, topic)
			time.Sleep(5 * time.Second)
			continue
		}

		deliveryChan := make(chan kafka.Event)
		topicName := topic
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
			Value:          payload,
		}, deliveryChan)

		if err != nil {
			log.Printf("Produce error: %s", err)
		} else {
			e := <-deliveryChan
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				log.Printf("✅ Produced event: %s | User: %s | Type: %s\n", event.EventID[:8], event.UserID, event.EventType)
			}
		}

		close(deliveryChan)
		time.Sleep(1 * time.Second)
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
