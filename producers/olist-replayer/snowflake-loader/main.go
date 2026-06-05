// snowflake-loader: loads the Olist dataset directly into Snowflake RAW tables.
// Uses RSA key-pair authentication to bypass MFA enforcement.
//
// Usage:
//   export SNOWFLAKE_ACCOUNT=rbmwyop-oi92503
//   export SNOWFLAKE_USER=streamsense_svc
//   export SNOWFLAKE_PRIVATE_KEY_PATH=/Users/chetan/StreamSense/infra/snowflake/keys/rsa_key_pkcs8.pem
//   export DATA_DIR=../data
//   go run main.go

package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/snowflakedb/gosnowflake"
)

// ─── Row types ────────────────────────────────────────────────────────────────

type orderRow struct {
	OrderID     string  `json:"order_id"`
	UserID      string  `json:"user_id"`
	ProductID   string  `json:"product_id"`
	ProductName string  `json:"product_name"`
	Category    string  `json:"category"`
	OrderStatus string  `json:"order_status"`
	Revenue     float64 `json:"revenue"`
	Quantity    int     `json:"quantity"`
	Region      string  `json:"region"`
	OrderTime   int64   `json:"order_time"`
}

type userEventRow struct {
	EventID    string `json:"event_id"`
	UserID     string `json:"user_id"`
	EventType  string `json:"event_type"`
	Page       string `json:"page"`
	SessionID  string `json:"session_id"`
	DeviceType string `json:"device_type"`
	EventTime  int64  `json:"event_time"`
}

// ─── CSV helpers ──────────────────────────────────────────────────────────────

func csvIndex(headers []string) map[string]int {
	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[h] = i
	}
	return idx
}

func mustOpen(path string) *os.File {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("Cannot open %s: %v", path, err)
	}
	return f
}

func parseOlistTime(s string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", s)
}

func categoryName(raw string) string {
	mapping := map[string]string{
		"cama_mesa_banho":             "Home & Bath",
		"esporte_lazer":               "Sports & Leisure",
		"moveis_decoracao":            "Furniture & Decor",
		"informatica_acessorios":      "Electronics",
		"utilidades_domesticas":       "Home Appliances",
		"relogios_presentes":          "Watches & Gifts",
		"beleza_saude":                "Beauty & Health",
		"ferramentas_jardim":          "Tools & Garden",
		"automotivo":                  "Automotive",
		"brinquedos":                  "Toys",
		"cool_stuff":                  "Cool Stuff",
		"telefonia":                   "Mobile Phones",
		"eletrodomesticos":            "Appliances",
		"bebes":                       "Baby Products",
		"fashion_bolsas_e_acessorios": "Fashion",
		"livros_tecnicos":             "Books",
		"papelaria":                   "Stationery",
	}
	if name, ok := mapping[raw]; ok {
		return name
	}
	if raw == "" {
		return "General"
	}
	return raw
}

// ─── Build rows ───────────────────────────────────────────────────────────────

func buildRows(dataDir string) ([]orderRow, []userEventRow) {
	type olistOrder struct {
		orderID     string
		customerID  string
		status      string
		purchasedAt time.Time
	}
	type olistItem struct {
		orderID   string
		productID string
		price     float64
	}
	type olistProduct struct {
		productID string
		category  string
	}
	type olistCustomer struct {
		customerID string
		state      string
	}

	orders := map[string]olistOrder{}
	{
		f := mustOpen(filepath.Join(dataDir, "olist_orders_dataset.csv"))
		defer f.Close()
		r := csv.NewReader(f)
		h, _ := r.Read()
		idx := csvIndex(h)
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
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
	}

	items := map[string][]olistItem{}
	{
		f := mustOpen(filepath.Join(dataDir, "olist_order_items_dataset.csv"))
		defer f.Close()
		r := csv.NewReader(f)
		h, _ := r.Read()
		idx := csvIndex(h)
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			price, _ := strconv.ParseFloat(row[idx["price"]], 64)
			items[row[idx["order_id"]]] = append(items[row[idx["order_id"]]], olistItem{
				orderID:   row[idx["order_id"]],
				productID: row[idx["product_id"]],
				price:     price,
			})
		}
	}

	products := map[string]olistProduct{}
	{
		f := mustOpen(filepath.Join(dataDir, "olist_products_dataset.csv"))
		defer f.Close()
		r := csv.NewReader(f)
		h, _ := r.Read()
		idx := csvIndex(h)
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			products[row[idx["product_id"]]] = olistProduct{
				productID: row[idx["product_id"]],
				category:  categoryName(row[idx["product_category_name"]]),
			}
		}
	}

	customers := map[string]olistCustomer{}
	{
		f := mustOpen(filepath.Join(dataDir, "olist_customers_dataset.csv"))
		defer f.Close()
		r := csv.NewReader(f)
		h, _ := r.Read()
		idx := csvIndex(h)
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			customers[row[idx["customer_id"]]] = olistCustomer{
				customerID: row[idx["customer_id"]],
				state:      row[idx["customer_state"]],
			}
		}
	}

	log.Printf("📦 orders=%d  items_keys=%d  products=%d  customers=%d",
		len(orders), len(items), len(products), len(customers))

	devices := []string{"mobile", "desktop", "tablet"}
	var orderRows []orderRow
	var userRows []userEventRow

	for orderID, o := range orders {
		_ = customers[o.customerID] // all Olist data is Brazil → South America

		for _, item := range items[orderID] {
			prod := products[item.productID]
			cat := prod.category
			if cat == "" {
				cat = "General"
			}
			pid := item.productID
			if len(pid) > 8 {
				pid = pid[:8]
			}
			uid := o.customerID
			if len(uid) > 16 {
				uid = uid[:16]
			}
			orderRows = append(orderRows, orderRow{
				OrderID:     orderID + "-" + item.productID,
				UserID:      uid,
				ProductID:   pid,
				ProductName: cat + " Item",
				Category:    cat,
				OrderStatus: o.status,
				Revenue:     math.Round(item.price*100) / 100,
				Quantity:    1,
				Region:      "South America",
				OrderTime:   o.purchasedAt.UnixMilli(),
			})
		}

		uid := o.customerID
		if len(uid) > 16 {
			uid = uid[:16]
		}
		eid := orderID
		if len(eid) > 16 {
			eid = eid[:16]
		}
		userRows = append(userRows, userEventRow{
			EventID:    eid,
			UserID:     uid,
			EventType:  "checkout",
			Page:       "checkout_success",
			SessionID:  eid,
			DeviceType: devices[len(o.customerID)%len(devices)],
			EventTime:  o.purchasedAt.UnixMilli(),
		})
	}

	log.Printf("🏗️  Built %d order rows and %d user event rows", len(orderRows), len(userRows))
	return orderRows, userRows
}

// ─── Snowflake helpers ────────────────────────────────────────────────────────

func ensureTable(db *sql.DB, table string) error {
	_, err := db.ExecContext(context.Background(), fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS RAW.%s (
			RECORD_CONTENT VARIANT,
			LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
		)`, table))
	return err
}

func bulkInsert(db *sql.DB, table string, jsonRows []string) error {
	const batchSize = 50
	total := 0
	for i := 0; i < len(jsonRows); i += batchSize {
		end := i + batchSize
		if end > len(jsonRows) {
			end = len(jsonRows)
		}
		batch := jsonRows[i:end]

		// Build INSERT ... SELECT using UNION ALL
		// INSERT INTO t (RECORD_CONTENT) SELECT PARSE_JSON('...') UNION ALL SELECT PARSE_JSON('...') ...
		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("INSERT INTO RAW.%s (RECORD_CONTENT) ", table))
		for j, row := range batch {
			if j > 0 {
				sb.WriteString(" UNION ALL ")
			}
			escaped := strings.ReplaceAll(row, "'", "''")
			sb.WriteString(fmt.Sprintf("SELECT PARSE_JSON('%s')", escaped))
		}

		if _, err := db.ExecContext(context.Background(), sb.String()); err != nil {
			return fmt.Errorf("batch insert failed at offset %d: %w", i, err)
		}
		total += len(batch)
		if total%5000 == 0 || total == len(jsonRows) {
			log.Printf("  ↻ %s: %d / %d rows inserted", table, total, len(jsonRows))
		}
	}
	return nil
}

// ─── Key-pair connection ──────────────────────────────────────────────────────

func openWithKeyPair(keyPath string) (*sql.DB, error) {
	account := os.Getenv("SNOWFLAKE_ACCOUNT")
	user := os.Getenv("SNOWFLAKE_USER")
	warehouse := os.Getenv("SNOWFLAKE_WAREHOUSE")
	if warehouse == "" {
		warehouse = "STREAMSENSE_WH"
	}
	if account == "" || user == "" {
		return nil, fmt.Errorf("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER must be set")
	}

	keyBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read private key %s: %w", keyPath, err)
	}
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM from %s", keyPath)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PKCS8 key: %w", err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is not RSA")
	}

	cfg := &gosnowflake.Config{
		Account:       account,
		User:          user,
		Database:      "STREAMSENSE",
		Schema:        "RAW",
		Warehouse:     warehouse,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    rsaKey,
	}
	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build DSN: %w", err)
	}
	return sql.Open("snowflake", dsn)
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	keyPath := os.Getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
	if keyPath == "" {
		log.Fatal(`SNOWFLAKE_PRIVATE_KEY_PATH is not set.

Export these vars then re-run:
  export SNOWFLAKE_ACCOUNT=rbmwyop-oi92503
  export SNOWFLAKE_USER=streamsense_svc
  export SNOWFLAKE_PRIVATE_KEY_PATH=/Users/chetan/StreamSense/infra/snowflake/keys/rsa_key_pkcs8.pem
  export DATA_DIR=/Users/chetan/StreamSense/producers/olist-replayer/data`)
	}

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "../data"
	}

	orderRows, userRows := buildRows(dataDir)

	log.Printf("🔌 Connecting to Snowflake via key-pair auth...")
	db, err := openWithKeyPair(keyPath)
	if err != nil {
		log.Fatalf("Connection setup failed: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatalf("Snowflake ping failed: %v", err)
	}
	log.Printf("✅ Snowflake connected")

	for _, t := range []string{"ORDER_EVENTS", "USER_EVENTS"} {
		if err := ensureTable(db, t); err != nil {
			log.Fatalf("Failed to ensure table %s: %v", t, err)
		}
	}
	log.Printf("✅ Tables ready")

	log.Printf("⬆️  Loading %d rows into RAW.ORDER_EVENTS...", len(orderRows))
	start := time.Now()
	orderJSON := make([]string, 0, len(orderRows))
	for _, row := range orderRows {
		b, _ := json.Marshal(row)
		orderJSON = append(orderJSON, string(b))
	}
	if err := bulkInsert(db, "ORDER_EVENTS", orderJSON); err != nil {
		log.Fatalf("order_events insert failed: %v", err)
	}
	log.Printf("✅ ORDER_EVENTS loaded in %s", time.Since(start).Round(time.Second))

	log.Printf("⬆️  Loading %d rows into RAW.USER_EVENTS...", len(userRows))
	start = time.Now()
	userJSON := make([]string, 0, len(userRows))
	for _, row := range userRows {
		b, _ := json.Marshal(row)
		userJSON = append(userJSON, string(b))
	}
	if err := bulkInsert(db, "USER_EVENTS", userJSON); err != nil {
		log.Fatalf("user_events insert failed: %v", err)
	}
	log.Printf("✅ USER_EVENTS loaded in %s", time.Since(start).Round(time.Second))

	log.Printf("\n🎉 LOAD COMPLETE")
	log.Printf("   order_events : %d rows", len(orderRows))
	log.Printf("   user_events  : %d rows", len(userRows))
	log.Printf("\nNext: cd warehouse/dbt && dbt run")
}
