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
	broker           = "localhost:9092"
	schemaRegistry   = "http://localhost:8081"
	topic            = "order-events"
)

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

	fmt.Printf("🚀 StreamSense | Starting OrderEvent Producer for topic: %s\n", topic)

	for {
		order := generateOrder()
		
		payload, err := ser.Serialize(topic, &order)
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
				log.Printf("💰 ORDER PRODUCED: %s | Product: %s | Revenue: $%.2f | Region: %s\n", 
					order.OrderID[:8], order.ProductName, order.Revenue, order.Region)
			}
		}
		close(deliveryChan)

		// Random interval for variety
		time.Sleep(time.Duration(rand.Intn(3000)+1000) * time.Millisecond)
	}
}

func generateOrder() OrderEvent {
	products := []struct {
		ID       string
		Name     string
		Category string
		Price    float64
	}{
		{"E001", "ProPhone 15", "Electronics", 999.99},
		{"E002", "AirBuds Pro", "Electronics", 249.00},
		{"E003", "SmartWatch V3", "Electronics", 349.00},
		{"C001", "Tech Hoodie", "Clothing", 75.00},
		{"C002", "UltraBoost 24", "Clothing", 190.00},
		{"J001", "Gold Necklace", "Jewelry", 1200.00},
	}
	p := products[rand.Intn(len(products))]
	qty := int32(rand.Intn(3) + 1)
	return OrderEvent{
		OrderID:     uuid.New().String(),
		UserID:      fmt.Sprintf("user_%d", rand.Intn(1000)),
		ProductID:   p.ID,
		ProductName: p.Name,
		Category:    p.Category,
		OrderStatus: "paid",
		Revenue:     p.Price * float64(qty),
		Quantity:    qty,
		Region:      regions[rand.Intn(len(regions))],
		OrderTime:   time.Now().UnixMilli(),
	}
}
