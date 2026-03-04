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
)

type InventoryEvent struct {
	ProductID       string `avro:"product_id"`
	ProductName     string `avro:"product_name"`
	Category        string `avro:"category"`
	StockAdjustment int32  `avro:"stock_adjustment"`
	StockOnHand     int32  `avro:"stock_on_hand"`
	Reason          string `avro:"reason"`
	EventTime       int64  `avro:"event_time"`
}

const (
	broker           = "localhost:9092"
	schemaRegistry   = "http://localhost:8081"
	topic            = "inventory-events"
)

var products = []Product{
	{"E001", "ProPhone 15", "Electronics"},
	{"E002", "AirBuds Pro", "Electronics"},
	{"E003", "SmartWatch V3", "Electronics"},
	{"C001", "Tech Hoodie", "Clothing"},
	{"C002", "UltraBoost 24", "Clothing"},
	{"J001", "Gold Necklace", "Jewelry"},
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
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

	fmt.Printf("🚀 StreamSense | Starting InventoryEvent Producer for topic: %s\n", topic)

	// Keep track of stock for simulation
	stock := make(map[string]int)
	for _, pr := range products {
		stock[pr.ID] = rand.Intn(500) + 100
	}

	for {
		pr := products[rand.Intn(len(products))]
		adjustment := rand.Intn(20) - 10 // Simulating stock in/out
		if adjustment == 0 {
			adjustment = -1
		}

		stock[pr.ID] += adjustment
		if stock[pr.ID] < 0 {
			stock[pr.ID] = rand.Intn(50) + 10 // Auto-restock simulation
			adjustment = stock[pr.ID]
		}
		
		reason := "restock"
		if adjustment < 0 {
			reason = "sale"
		}

		event := InventoryEvent{
			ProductID:       pr.ID,
			ProductName:     pr.Name,
			Category:        pr.Category,
			StockAdjustment: int32(adjustment),
			StockOnHand:     int32(stock[pr.ID]),
			Reason:          reason,
			EventTime:       time.Now().UnixMilli(),
		}

		payload, err := ser.Serialize(topic, &event)
		if err != nil {
			log.Printf("SR ERROR: %s (Topic: %s)", err, topic)
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
				log.Printf("📦 INVENTORY: %s (%s) | Adjustment: %d | On-Hand: %d\n", 
					pr.Name, pr.ID, adjustment, stock[pr.ID])
			}
		}
		close(deliveryChan)
		time.Sleep(time.Duration(rand.Intn(5000)+2000) * time.Millisecond)
	}
}
