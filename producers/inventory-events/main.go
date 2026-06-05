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
	broker         = "localhost:9092"
	schemaRegistry = "http://localhost:8081"
	topic          = "inventory-events"
)

type Product struct {
	ID       string
	Name     string
	Category string
}

var products = []Product{
	{"E001", "ProPhone 15", "Electronics"},
	{"E002", "AirBuds Pro", "Electronics"},
	{"E003", "SmartWatch V3", "Electronics"},
	{"C001", "Tech Hoodie", "Clothing"},
	{"C002", "UltraBoost 24", "Clothing"},
	{"J001", "Gold Necklace", "Jewelry"},
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"linger.ms":         5,
		"batch.size":        65536,
		"compression.type":  "snappy",
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

	bulkMode := os.Getenv("BULK_MODE") == "1"
	bulkSecs, _ := strconv.Atoi(os.Getenv("BULK_SECONDS"))
	if bulkSecs <= 0 {
		bulkSecs = 600
	}

	if bulkMode {
		runBulk(p, ser, bulkSecs)
		return
	}

	fmt.Printf("🚀 StreamSense | InventoryEvent Producer → %s (normal mode)\n", topic)
	stock := make(map[string]int)
	for _, pr := range products {
		stock[pr.ID] = rand.Intn(500) + 100
	}
	for {
		produceOne(p, ser, stock)
		time.Sleep(time.Duration(rand.Intn(5000)+2000) * time.Millisecond)
	}
}

func runBulk(p *kafka.Producer, ser *avro.GenericSerializer, durationSecs int) {
	deadline := time.Now().Add(time.Duration(durationSecs) * time.Second)
	var sent, failed int64

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

	fmt.Printf("🚀 BULK MODE | inventory-events | running for %ds\n", durationSecs)
	batchSize := 500
	topicName := topic
	stock := make(map[string]int)
	for _, pr := range products {
		stock[pr.ID] = rand.Intn(500) + 100
	}
	start := time.Now()

	for time.Now().Before(deadline) {
		for i := 0; i < batchSize; i++ {
			event := generateInventoryEvent(stock)
			payload, err := ser.Serialize(topic, &event)
			if err != nil {
				continue
			}
			p.Produce(&kafka.Message{ //nolint:errcheck
				TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
				Value:          payload,
			}, nil)
		}
		p.Flush(1000)
		elapsed := time.Since(start)
		if int(elapsed.Seconds())%30 == 0 && int(elapsed.Seconds()) > 0 {
			fmt.Printf("  ⚡ inventory-events: %s elapsed | ~%d delivered so far\n",
				elapsed.Round(time.Second), atomic.LoadInt64(&sent))
		}
	}

	p.Flush(10000)
	total := atomic.LoadInt64(&sent)
	fmt.Printf("✅ BULK DONE | inventory-events | %d events delivered | %d failed\n", total, atomic.LoadInt64(&failed))
}

func produceOne(p *kafka.Producer, ser *avro.GenericSerializer, stock map[string]int) {
	event := generateInventoryEvent(stock)
	payload, err := ser.Serialize(topic, &event)
	if err != nil {
		log.Printf("SR ERROR: %s", err)
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
		log.Printf("📦 INVENTORY: %s | adj: %d | on-hand: %d", event.ProductName, event.StockAdjustment, event.StockOnHand)
	}
}

func generateInventoryEvent(stock map[string]int) InventoryEvent {
	pr := products[rand.Intn(len(products))]
	adjustment := rand.Intn(20) - 10
	if adjustment == 0 {
		adjustment = -1
	}
	stock[pr.ID] += adjustment
	if stock[pr.ID] < 0 {
		stock[pr.ID] = rand.Intn(50) + 10
		adjustment = stock[pr.ID]
	}
	reason := "restock"
	if adjustment < 0 {
		reason = "sale"
	}
	return InventoryEvent{
		ProductID:       pr.ID,
		ProductName:     pr.Name,
		Category:        pr.Category,
		StockAdjustment: int32(adjustment),
		StockOnHand:     int32(stock[pr.ID]),
		Reason:          reason,
		EventTime:       time.Now().UnixMilli(),
	}
}

const (
	broker           = "localhost:9092"
	schemaRegistry   = "http://localhost:8081"
	topic            = "inventory-events"
)

// Product is a catalog item used to generate inventory events.
type Product struct {
	ID       string
	Name     string
	Category string
}

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
