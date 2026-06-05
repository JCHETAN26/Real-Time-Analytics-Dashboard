// olist-replayer: replays the Brazilian E-Commerce (Olist) dataset through
// the StreamSense Kafka topics at a configurable speed multiplier.
//
// Required CSVs (place in ./data/ or set DATA_DIR env var):
//   olist_orders_dataset.csv
//   olist_order_items_dataset.csv
//   olist_products_dataset.csv
//   olist_customers_dataset.csv
//
// Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
//
// Usage:
//   SPEED_MULTIPLIER=100 go run main.go      # replay at 100x real time
//   SPEED_MULTIPLIER=0   go run main.go      # fire all events instantly (benchmark mode)

package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ─── Kafka event shapes (plain JSON — no Avro dependency needed for replayer) ─

type OrderEvent struct {
	OrderID     string  `json:"order_id"`
	UserID      string  `json:"user_id"`
	ProductID   string  `json:"product_id"`
	ProductName string  `json:"product_name"`
	Category    string  `json:"category"`
	OrderStatus string  `json:"order_status"`
	Revenue     float64 `json:"revenue"`
	Quantity    int32   `json:"quantity"`
	Region      string  `json:"region"`
	OrderTime   int64   `json:"order_time"` // Unix ms
}

type UserEvent struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	EventType  string `json:"event_type"`
	Page       string `json:"page"`
	SessionID  string `json:"session_id"`
	DeviceType string `json:"device_type"`
	EventTime  int64  `json:"event_time"` // Unix ms
}

// ─── Replay event (internal) ──────────────────────────────────────────────────

type replayEvent struct {
	topic     string
	payload   []byte
	realTime  time.Time // original timestamp from dataset
}

// ─── CSV row types ─────────────────────────────────────────────────────────────

type olistOrder struct {
	orderID        string
	customerID     string
	status         string
	purchasedAt    time.Time
}

type olistOrderItem struct {
	orderID   string
	productID string
	price     float64
	quantity  int32
}

type olistProduct struct {
	productID   string
	category    string
}

type olistCustomer struct {
	customerID string
	state      string
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func stateToRegion(state string) string {
	// Map Brazilian states to the regions used by the existing dashboard
	southAmerica := map[string]bool{
		"SP": true, "RJ": true, "MG": true, "RS": true, "PR": true,
		"SC": true, "BA": true, "GO": true, "DF": true, "PE": true,
		"CE": true, "MT": true, "MS": true, "PB": true, "ES": true,
		"RN": true, "AL": true, "PI": true, "MA": true, "AM": true,
		"PA": true, "RO": true, "TO": true, "AC": true, "AP": true,
		"RR": true, "SE": true,
	}
	if southAmerica[state] {
		return "South America"
	}
	return "South America"
}

func categoryToDisplayName(raw string) string {
	mapping := map[string]string{
		"cama_mesa_banho":           "Home & Bath",
		"esporte_lazer":             "Sports & Leisure",
		"moveis_decoracao":          "Furniture & Decor",
		"informatica_acessorios":    "Electronics",
		"utilidades_domesticas":     "Home Appliances",
		"relogios_presentes":        "Watches & Gifts",
		"beleza_saude":              "Beauty & Health",
		"ferramentas_jardim":        "Tools & Garden",
		"automotivo":                "Automotive",
		"brinquedos":                "Toys",
		"cool_stuff":                "Cool Stuff",
		"telefonia":                 "Mobile Phones",
		"eletrodomesticos":          "Appliances",
		"bebes":                     "Baby Products",
		"fashion_bolsas_e_acessorios": "Fashion",
		"livros_tecnicos":           "Books",
		"papelaria":                 "Stationery",
	}
	if name, ok := mapping[raw]; ok {
		return name
	}
	if raw == "" {
		return "General"
	}
	return raw
}

func parseOlistTime(s string) (time.Time, error) {
	// Olist timestamps: "2017-10-02 10:56:33"
	return time.Parse("2006-01-02 15:04:05", s)
}

func mustOpen(path string) *os.File {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("Cannot open %s: %v\n\nDownload the Olist dataset from:\nhttps://www.kaggle.com/datasets/olistbr/brazilian-ecommerce\nand place the CSV files in %s", path, err, filepath.Dir(path))
	}
	return f
}

// csvIndex returns a map of column name → index for the header row.
func csvIndex(headers []string) map[string]int {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return idx
}

// ─── CSV loaders ──────────────────────────────────────────────────────────────

func loadOrders(path string) map[string]olistOrder {
	f := mustOpen(path)
	defer f.Close()
	r := csv.NewReader(f)
	headers, _ := r.Read()
	idx := csvIndex(headers)
	orders := make(map[string]olistOrder)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(row) <= idx["order_purchase_timestamp"] {
			continue
		}
		t, err := parseOlistTime(row[idx["order_purchase_timestamp"]])
		if err != nil {
			continue
		}
		orders[row[idx["order_id"]]] = olistOrder{
			orderID:     row[idx["order_id"]],
			customerID:  row[idx["customer_id"]],
			status:      row[idx["order_status"]],
			purchasedAt: t,
		}
	}
	log.Printf("📦 Loaded %d orders", len(orders))
	return orders
}

func loadOrderItems(path string) map[string][]olistOrderItem {
	f := mustOpen(path)
	defer f.Close()
	r := csv.NewReader(f)
	headers, _ := r.Read()
	idx := csvIndex(headers)
	items := make(map[string][]olistOrderItem)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(row) <= idx["price"] {
			continue
		}
		price, _ := strconv.ParseFloat(row[idx["price"]], 64)
		items[row[idx["order_id"]]] = append(items[row[idx["order_id"]]], olistOrderItem{
			orderID:   row[idx["order_id"]],
			productID: row[idx["product_id"]],
			price:     price,
			quantity:  1, // Olist items are always qty=1 per row
		})
	}
	log.Printf("🛒 Loaded items for %d orders", len(items))
	return items
}

func loadProducts(path string) map[string]olistProduct {
	f := mustOpen(path)
	defer f.Close()
	r := csv.NewReader(f)
	headers, _ := r.Read()
	idx := csvIndex(headers)
	products := make(map[string]olistProduct)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(row) <= idx["product_category_name"] {
			continue
		}
		products[row[idx["product_id"]]] = olistProduct{
			productID: row[idx["product_id"]],
			category:  categoryToDisplayName(row[idx["product_category_name"]]),
		}
	}
	log.Printf("🏷️  Loaded %d products", len(products))
	return products
}

func loadCustomers(path string) map[string]olistCustomer {
	f := mustOpen(path)
	defer f.Close()
	r := csv.NewReader(f)
	headers, _ := r.Read()
	idx := csvIndex(headers)
	customers := make(map[string]olistCustomer)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(row) <= idx["customer_state"] {
			continue
		}
		customers[row[idx["customer_id"]]] = olistCustomer{
			customerID: row[idx["customer_id"]],
			state:      row[idx["customer_state"]],
		}
	}
	log.Printf("👤 Loaded %d customers", len(customers))
	return customers
}

// ─── Build replay queue ───────────────────────────────────────────────────────

func buildQueue(
	orders map[string]olistOrder,
	items map[string][]olistOrderItem,
	products map[string]olistProduct,
	customers map[string]olistCustomer,
) []replayEvent {

	var events []replayEvent

	devices := []string{"mobile", "desktop", "tablet"}
	eventTypes := []string{"page_view", "search", "click", "add_to_cart", "checkout"}

	for orderID, order := range orders {
		cust := customers[order.customerID]
		region := stateToRegion(cust.state)
		orderItems := items[orderID]

		if len(orderItems) == 0 {
			continue
		}

		// One order event per item (matches existing pipeline shape)
		for _, item := range orderItems {
			prod := products[item.productID]
			category := prod.category
			if category == "" {
				category = "General"
			}

			oe := OrderEvent{
				OrderID:     orderID + "-" + item.productID,
				UserID:      order.customerID[:16], // truncate to 16 chars
				ProductID:   item.productID[:8],
				ProductName: category + " Item",
				Category:    category,
				OrderStatus: order.status,
				Revenue:     math.Round(item.price*100) / 100,
				Quantity:    item.quantity,
				Region:      region,
				OrderTime:   order.purchasedAt.UnixMilli(),
			}

			b, _ := json.Marshal(oe)
			events = append(events, replayEvent{
				topic:    "order-events",
				payload:  b,
				realTime: order.purchasedAt,
			})
		}

		// Synthesize a user event (checkout) tied to the same timestamp
		ue := UserEvent{
			EventID:    orderID[:16],
			UserID:     order.customerID[:16],
			EventType:  eventTypes[len(order.customerID)%len(eventTypes)],
			Page:       "checkout_success",
			SessionID:  orderID[:16],
			DeviceType: devices[len(order.customerID)%len(devices)],
			EventTime:  order.purchasedAt.UnixMilli(),
		}
		b, _ := json.Marshal(ue)
		events = append(events, replayEvent{
			topic:    "user-events",
			payload:  b,
			realTime: order.purchasedAt,
		})
	}

	// Sort by original timestamp so replay is chronological
	sort.Slice(events, func(i, j int) bool {
		return events[i].realTime.Before(events[j].realTime)
	})

	log.Printf("📋 Built replay queue: %d events spanning %s → %s",
		len(events),
		events[0].realTime.Format("2006-01-02"),
		events[len(events)-1].realTime.Format("2006-01-02"),
	)

	return events
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	dataDir := getEnv("DATA_DIR", "./data")
	speedStr := getEnv("SPEED_MULTIPLIER", "500") // 500x = ~2 years of data in ~2 hours

	speedMultiplier, err := strconv.ParseFloat(speedStr, 64)
	if err != nil || speedMultiplier < 0 {
		log.Fatalf("Invalid SPEED_MULTIPLIER: %s", speedStr)
	}

	instantMode := speedMultiplier == 0

	// ── Load CSVs ──
	orders := loadOrders(filepath.Join(dataDir, "olist_orders_dataset.csv"))
	items := loadOrderItems(filepath.Join(dataDir, "olist_order_items_dataset.csv"))
	prods := loadProducts(filepath.Join(dataDir, "olist_products_dataset.csv"))
	customers := loadCustomers(filepath.Join(dataDir, "olist_customers_dataset.csv"))

	// ── Build sorted replay queue ──
	queue := buildQueue(orders, items, prods, customers)
	if len(queue) == 0 {
		log.Fatal("No events to replay — check your CSV files")
	}

	// ── Create Kafka producer ──
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"linger.ms":         10,
		"batch.size":        131072,
		"compression.type":  "snappy",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer p.Close()

	// Drain delivery reports
	var delivered, failed int64
	go func() {
		for e := range p.Events() {
			m, ok := e.(*kafka.Message)
			if !ok {
				continue
			}
			if m.TopicPartition.Error != nil {
				atomic.AddInt64(&failed, 1)
			} else {
				atomic.AddInt64(&delivered, 1)
			}
		}
	}()

	// ── Replay ──
	replayStart := time.Now()
	dataStart := queue[0].realTime

	if instantMode {
		log.Printf("⚡ INSTANT MODE — firing all %d events as fast as possible", len(queue))
	} else {
		estimatedDuration := time.Duration(
			float64(queue[len(queue)-1].realTime.Sub(queue[0].realTime)) / speedMultiplier,
		)
		log.Printf("🚀 REPLAY START | %d events | %.0fx speed | estimated duration: %s",
			len(queue), speedMultiplier, estimatedDuration.Round(time.Second))
	}

	for i, ev := range queue {
		if !instantMode {
			// How far into the dataset are we?
			dataElapsed := ev.realTime.Sub(dataStart)
			// How far into real time should we be?
			targetRealElapsed := time.Duration(float64(dataElapsed) / speedMultiplier)
			targetRealTime := replayStart.Add(targetRealElapsed)

			// Sleep until the right wall-clock moment
			sleepDur := time.Until(targetRealTime)
			if sleepDur > 0 {
				time.Sleep(sleepDur)
			}
		}

		topic := ev.topic
		p.Produce(&kafka.Message{ //nolint:errcheck
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          ev.payload,
		}, nil)

		// Flush and log progress every 5000 events
		if (i+1)%5000 == 0 {
			p.Flush(2000)
			d := atomic.LoadInt64(&delivered)
			log.Printf("  ↻ progress: %d/%d events | %d delivered | dataset time: %s",
				i+1, len(queue), d, ev.realTime.Format("2006-01-02 15:04"))
		}
	}

	p.Flush(15000)
	log.Printf("✅ REPLAY COMPLETE | %d delivered | %d failed | wall time: %s",
		atomic.LoadInt64(&delivered),
		atomic.LoadInt64(&failed),
		time.Since(replayStart).Round(time.Second),
	)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
