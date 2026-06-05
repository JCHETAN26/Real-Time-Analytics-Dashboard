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

type OrderEvent struct {
	OrderID     string  `avro:"order_id"`
	UserID      string  `avro:"user_id"`
	ProductID   string  `avro:"product_id"`
	ProductName string  `avro:"product_name"`
	Category    string  `avro:"category"`
	OrderStatus string  `avro:"order_status"`
	Revenue     float64 `avro:"revenue"`
	Quantity    int32   `avro:"quantity"`
	Region      string  `avro:"region"`
	OrderTime   int64   `avro:"order_time"`
}

const (
	broker         = "localhost:9092"
	schemaRegistry = "http://localhost:8081"
	topic          = "order-events"
)

type Product struct {
	ID       string
	Name     string
	Category string
	Price    float64
}

var products = []Product{
	{"E001", "ProPhone 15", "Electronics", 999.99},
	{"E002", "AirBuds Pro", "Electronics", 249.00},
	{"E003", "SmartWatch V3", "Electronics", 349.00},
	{"C001", "Tech Hoodie", "Clothing", 75.00},
	{"C002", "UltraBoost 24", "Clothing", 190.00},
	{"J001", "Gold Necklace", "Jewelry", 1200.00},
}

var regions = []string{"North America", "Europe", "Asia", "South America", "Oceania"}

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

	fmt.Printf("🚀 StreamSense | OrderEvent Producer → %s (normal mode)\n", topic)
	for {
		produceOne(p, ser)
		time.Sleep(time.Duration(rand.Intn(3000)+1000) * time.Millisecond)
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

	fmt.Printf("🚀 BULK MODE | order-events | running for %ds\n", durationSecs)
	batchSize := 500
	topicName := topic
	start := time.Now()

	for time.Now().Before(deadline) {
		for i := 0; i < batchSize; i++ {
			order := generateOrder()
			payload, err := ser.Serialize(topic, &order)
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
			fmt.Printf("  ⚡ order-events: %s elapsed | ~%d delivered so far\n",
				elapsed.Round(time.Second), atomic.LoadInt64(&sent))
		}
	}

	p.Flush(10000)
	total := atomic.LoadInt64(&sent)
	fmt.Printf("✅ BULK DONE | order-events | %d events delivered | %d failed\n", total, atomic.LoadInt64(&failed))
}

func produceOne(p *kafka.Producer, ser *avro.GenericSerializer) {
	order := generateOrder()
	payload, err := ser.Serialize(topic, &order)
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
		log.Printf("💰 ORDER: %s | %s | $%.2f | %s", order.OrderID[:8], order.ProductName, order.Revenue, order.Region)
	}
}

func generateOrder() OrderEvent {
	pr := products[rand.Intn(len(products))]
	qty := int32(rand.Intn(3) + 1)
	return OrderEvent{
		OrderID:     uuid.New().String(),
		UserID:      fmt.Sprintf("user_%d", rand.Intn(1000)),
		ProductID:   pr.ID,
		ProductName: pr.Name,
		Category:    pr.Category,
		OrderStatus: "paid",
		Revenue:     pr.Price * float64(qty),
		Quantity:    qty,
		Region:      regions[rand.Intn(len(regions))],
		OrderTime:   time.Now().UnixMilli(),
	}
}
